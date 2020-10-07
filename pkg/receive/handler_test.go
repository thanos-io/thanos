// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestCountCause(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		f    func(error) bool
		out  int
	}{
		{
			name: "nil",
			f:    isConflict,
			out:  0,
		},
		{
			name: "nil multierror",
			err:  terrors.MultiError([]error{}),
			f:    isConflict,
			out:  0,
		},
		{
			name: "matching nil",
			f:    func(err error) bool { return err == nil },
			out:  1,
		},
		{
			name: "matching simple",
			err:  conflictErr,
			f:    isConflict,
			out:  1,
		},
		{
			name: "non-matching multierror",
			err: terrors.MultiError([]error{
				errors.New("foo"),
				errors.New("bar"),
			}),
			f:   isConflict,
			out: 0,
		},
		{
			name: "nested non-matching multierror",
			err: errors.Wrap(terrors.MultiError([]error{
				errors.New("foo"),
				errors.New("bar"),
			}), "baz"),
			f:   isConflict,
			out: 0,
		},
		{
			name: "deep nested non-matching multierror",
			err: errors.Wrap(terrors.MultiError([]error{
				errors.New("foo"),
				terrors.MultiError([]error{
					errors.New("bar"),
					errors.New("qux"),
				}),
			}), "baz"),
			f:   isConflict,
			out: 0,
		},
		{
			name: "matching multierror",
			err: terrors.MultiError([]error{
				storage.ErrOutOfOrderSample,
				errors.New("foo"),
				errors.New("bar"),
			}),
			f:   isConflict,
			out: 1,
		},
		{
			name: "matching multierror many",
			err: terrors.MultiError([]error{
				storage.ErrOutOfOrderSample,
				conflictErr,
				errors.New(strconv.Itoa(http.StatusConflict)),
				errors.New("foo"),
				errors.New("bar"),
			}),
			f:   isConflict,
			out: 3,
		},
		{
			name: "nested matching multierror",
			err: errors.Wrap(terrors.MultiError([]error{
				storage.ErrOutOfOrderSample,
				errors.New("foo"),
				errors.New("bar"),
			}), "baz"),
			f:   isConflict,
			out: 0,
		},
		{
			name: "deep nested matching multierror",
			err: errors.Wrap(terrors.MultiError([]error{
				terrors.MultiError([]error{
					errors.New("qux"),
					errors.New(strconv.Itoa(http.StatusConflict)),
				}),
				errors.New("foo"),
				errors.New("bar"),
			}), "baz"),
			f:   isConflict,
			out: 0,
		},
	} {
		if n := countCause(tc.err, tc.f); n != tc.out {
			t.Errorf("test case %s: expected %d, got %d", tc.name, tc.out, n)
		}
	}
}

func newHandlerHashring(appendables []*fakeAppendable, replicationFactor uint64) ([]*Handler, Hashring) {
	cfg := []HashringConfig{
		{
			Hashring: "test",
		},
	}
	var handlers []*Handler
	// create a fake peer group where we manually fill the cache with fake addresses pointed to our handlers
	// This removes the network from the tests and creates a more consistent testing harness.
	peers := &peerGroup{
		dialOpts: nil,
		m:        sync.RWMutex{},
		cache:    map[string]storepb.WriteableStoreClient{},
		dialer: func(context.Context, string, ...grpc.DialOption) (*grpc.ClientConn, error) {
			// dialer should never be called since we are creating fake clients with fake addresses
			// this protects against some leaking test that may attempt to dial random IP addresses
			// which may pose a security risk.
			return nil, errors.New("unexpected dial called in testing")
		},
	}

	for i := range appendables {
		h := NewHandler(nil, &Options{
			TenantHeader:      DefaultTenantHeader,
			ReplicaHeader:     DefaultReplicaHeader,
			ReplicationFactor: replicationFactor,
			ForwardTimeout:    5 * time.Second,
			Writer:            NewWriter(log.NewNopLogger(), newFakeTenantAppendable(appendables[i])),
		})
		handlers = append(handlers, h)
		h.peers = peers
		addr := randomAddr()
		h.options.Endpoint = addr
		cfg[0].Endpoints = append(cfg[0].Endpoints, h.options.Endpoint)
		peers.cache[addr] = &fakeRemoteWriteGRPCServer{h: h}
	}
	hashring := newMultiHashring(cfg)
	for _, h := range handlers {
		h.Hashring(hashring)
	}
	return handlers, hashring
}

func TestReceiveQuorum(t *testing.T) {
	appenderErrFn := func() error { return errors.New("failed to get appender") }
	conflictErrFn := func() error { return storage.ErrOutOfBounds }
	commitErrFn := func() error { return errors.New("failed to commit") }
	wreq1 := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []storepb.Label{
					{
						Name:  "foo",
						Value: "bar",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     1,
						Timestamp: 1,
					},
					{
						Value:     2,
						Timestamp: 2,
					},
					{
						Value:     3,
						Timestamp: 3,
					},
				},
			},
		},
	}
	for _, tc := range []struct {
		name              string
		status            int
		replicationFactor uint64
		wreq              *prompb.WriteRequest
		appendables       []*fakeAppendable
	}{
		{
			name:              "size 1 success",
			status:            http.StatusOK,
			replicationFactor: 1,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 1 commit error",
			status:            http.StatusInternalServerError,
			replicationFactor: 1,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
			},
		},
		{
			name:              "size 1 conflict",
			status:            http.StatusConflict,
			replicationFactor: 1,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 2 success",
			status:            http.StatusOK,
			replicationFactor: 1,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 success",
			status:            http.StatusOK,
			replicationFactor: 1,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 success with replication",
			status:            http.StatusOK,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 commit error",
			status:            http.StatusInternalServerError,
			replicationFactor: 1,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
			},
		},
		{
			name:              "size 3 commit error with replication",
			status:            http.StatusInternalServerError,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
			},
		},
		{
			name:              "size 3 appender error with replication",
			status:            http.StatusInternalServerError,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender:    newFakeAppender(nil, nil, nil, nil),
					appenderErr: appenderErrFn,
				},
				{
					appender:    newFakeAppender(nil, nil, nil, nil),
					appenderErr: appenderErrFn,
				},
				{
					appender:    newFakeAppender(nil, nil, nil, nil),
					appenderErr: appenderErrFn,
				},
			},
		},
		{
			name:              "size 3 conflict with replication",
			status:            http.StatusConflict,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, nil, nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 conflict and commit error with replication",
			status:            http.StatusConflict,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, commitErrFn, nil),
				},
			},
		},
		{
			name:              "size 3 with replication and one faulty",
			status:            http.StatusOK,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 with replication and one commit error",
			status:            http.StatusOK,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 with replication and two conflicts",
			status:            http.StatusConflict,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 with replication one conflict and one commit error",
			status:            http.StatusInternalServerError,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 with replication two commit errors",
			status:            http.StatusInternalServerError,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handlers, hashring := newHandlerHashring(tc.appendables, tc.replicationFactor)
			tenant := "test"
			// Test from the point of view of every node
			// so that we know status code does not depend
			// on which node is erroring and which node is receiving.
			for i, handler := range handlers {
				// Test that the correct status is returned.
				rec, err := makeRequest(handler, tenant, tc.wreq)
				if err != nil {
					t.Fatalf("handler %d: unexpectedly failed making HTTP request: %v", tc.status, err)
				}
				if rec.Code != tc.status {
					t.Errorf("handler %d: got unexpected HTTP status code: expected %d, got %d; body: %s", i, tc.status, rec.Code, rec.Body.String())
				}
			}
			// Test that each time series is stored
			// the correct amount of times in each fake DB.
			for _, ts := range tc.wreq.Timeseries {
				lset := make(labels.Labels, len(ts.Labels))
				for j := range ts.Labels {
					lset[j] = labels.Label{
						Name:  ts.Labels[j].Name,
						Value: ts.Labels[j].Value,
					}
				}
				for j, a := range tc.appendables {
					var expectedMin int
					n := a.appender.(*fakeAppender).Get(lset)
					got := uint64(len(n))
					if a.appenderErr == nil && endpointHit(t, hashring, tc.replicationFactor, handlers[j].options.Endpoint, tenant, &ts) {
						// We have len(handlers) copies of each sample because the test case
						// is run once for each handler and they all use the same appender.
						expectedMin = int((tc.replicationFactor/2)+1) * len(ts.Samples)
					}
					if uint64(expectedMin) > got {
						t.Errorf("handler: %d, labels %q: expected minimum of %d samples, got %d", j, lset.String(), expectedMin, got)
					}
				}
			}
		})
	}
}

func TestReceiveWithConsistencyDelay(t *testing.T) {
	appenderErrFn := func() error { return errors.New("failed to get appender") }
	conflictErrFn := func() error { return storage.ErrOutOfBounds }
	commitErrFn := func() error { return errors.New("failed to commit") }
	wreq1 := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []storepb.Label{
					{
						Name:  "foo",
						Value: "bar",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     1,
						Timestamp: 1,
					},
					{
						Value:     2,
						Timestamp: 2,
					},
					{
						Value:     3,
						Timestamp: 3,
					},
				},
			},
		},
	}
	for _, tc := range []struct {
		name              string
		status            int
		replicationFactor uint64
		wreq              *prompb.WriteRequest
		appendables       []*fakeAppendable
	}{
		{
			name:              "size 1 success",
			status:            http.StatusOK,
			replicationFactor: 1,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 1 commit error",
			status:            http.StatusInternalServerError,
			replicationFactor: 1,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
			},
		},
		{
			name:              "size 1 conflict",
			status:            http.StatusConflict,
			replicationFactor: 1,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 2 success",
			status:            http.StatusOK,
			replicationFactor: 1,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 success",
			status:            http.StatusOK,
			replicationFactor: 1,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 success with replication",
			status:            http.StatusOK,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 commit error",
			status:            http.StatusInternalServerError,
			replicationFactor: 1,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
			},
		},
		{
			name:              "size 3 commit error with replication",
			status:            http.StatusInternalServerError,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
			},
		},
		{
			name:              "size 3 appender error with replication",
			status:            http.StatusInternalServerError,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender:    newFakeAppender(nil, nil, nil, nil),
					appenderErr: appenderErrFn,
				},
				{
					appender:    newFakeAppender(nil, nil, nil, nil),
					appenderErr: appenderErrFn,
				},
				{
					appender:    newFakeAppender(nil, nil, nil, nil),
					appenderErr: appenderErrFn,
				},
			},
		},
		{
			name:              "size 3 conflict with replication",
			status:            http.StatusConflict,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, nil, nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 conflict and commit error with replication",
			status:            http.StatusConflict,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, commitErrFn, nil),
				},
			},
		},
		{
			name:              "size 3 with replication and one faulty",
			status:            http.StatusOK,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 with replication and one commit error",
			status:            http.StatusOK,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 with replication and two conflicts",
			status:            http.StatusConflict,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 with replication one conflict and one commit error",
			status:            http.StatusInternalServerError,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 with replication two commit errors",
			status:            http.StatusInternalServerError,
			replicationFactor: 3,
			wreq:              wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
			},
		},
	} {
		// Run the quorum tests with consistency delay, which should allow us
		// to see all requests completing all the time, since we're using local
		// network we are not expecting anything to go wrong with these.
		t.Run(tc.name, func(t *testing.T) {
			handlers, hashring := newHandlerHashring(tc.appendables, tc.replicationFactor)
			tenant := "test"
			// Test from the point of view of every node
			// so that we know status code does not depend
			// on which node is erroring and which node is receiving.
			for i, handler := range handlers {
				// Test that the correct status is returned.
				rec, err := makeRequest(handler, tenant, tc.wreq)
				if err != nil {
					t.Fatalf("handler %d: unexpectedly failed making HTTP request: %v", tc.status, err)
				}
				if rec.Code != tc.status {
					t.Errorf("handler %d: got unexpected HTTP status code: expected %d, got %d; body: %s", i, tc.status, rec.Code, rec.Body.String())
				}
			}

			time.Sleep(50 * time.Millisecond)

			// Test that each time series is stored
			// the correct amount of times in each fake DB.
			for _, ts := range tc.wreq.Timeseries {
				lset := make(labels.Labels, len(ts.Labels))
				for j := range ts.Labels {
					lset[j] = labels.Label{
						Name:  ts.Labels[j].Name,
						Value: ts.Labels[j].Value,
					}
				}
				for j, a := range tc.appendables {
					var expected int
					n := a.appender.(*fakeAppender).Get(lset)
					got := uint64(len(n))
					if a.appenderErr == nil && endpointHit(t, hashring, tc.replicationFactor, handlers[j].options.Endpoint, tenant, &ts) {
						// We have len(handlers) copies of each sample because the test case
						// is run once for each handler and they all use the same appender.
						expected = len(handlers) * len(ts.Samples)
					}
					if uint64(expected) != got {
						t.Errorf("handler: %d, labels %q: expected %d samples, got %d", j, lset.String(), expected, got)
					}
				}
			}
		})
	}
}

// endpointHit is a helper to determine if a given endpoint in a hashring would be selected
// for a given time series, tenant, and replication factor.
func endpointHit(t *testing.T, h Hashring, rf uint64, endpoint, tenant string, timeSeries *prompb.TimeSeries) bool {
	for i := uint64(0); i < rf; i++ {
		e, err := h.GetN(tenant, timeSeries, i)
		if err != nil {
			t.Fatalf("got unexpected error querying hashring: %v", err)
		}
		if e == endpoint {
			return true
		}
	}
	return false
}

// cycleErrors returns an error generator that cycles through every given error.
func cycleErrors(errs []error) func() error {
	var mu sync.Mutex
	var i int
	return func() error {
		mu.Lock()
		defer mu.Unlock()
		err := errs[i]
		i++
		if i >= len(errs) {
			i = 0
		}
		return err
	}
}

// makeRequest is a helper to make a correct request against a remote write endpoint given a request.
func makeRequest(h *Handler, tenant string, wreq *prompb.WriteRequest) (*httptest.ResponseRecorder, error) {
	buf, err := proto.Marshal(wreq)
	if err != nil {
		return nil, errors.Wrap(err, "marshal request")
	}
	req, err := http.NewRequest("POST", h.options.Endpoint, bytes.NewBuffer(snappy.Encode(nil, buf)))
	if err != nil {
		return nil, errors.Wrap(err, "create request")
	}
	req.Header.Add(h.options.TenantHeader, tenant)

	rec := httptest.NewRecorder()
	h.receiveHTTP(rec, req)
	rec.Flush()

	return rec, nil
}

func randomAddr() string {
	return fmt.Sprintf("http://%d.%d.%d.%d:%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(35000)+30000)
}

type fakeRemoteWriteGRPCServer struct {
	h storepb.WriteableStoreServer
}

func (f *fakeRemoteWriteGRPCServer) RemoteWrite(ctx context.Context, in *storepb.WriteRequest, opts ...grpc.CallOption) (*storepb.WriteResponse, error) {
	return f.h.RemoteWrite(ctx, in)
}

func BenchmarkHandlerReceiveHTTP(b *testing.B) {
	b.ReportAllocs()

	benchmarkHandlerMultiTSDBReceiveRemoteWrite(testutil.NewTB(b))
}

func TestHandlerReceiveHTTP(t *testing.T) {
	benchmarkHandlerMultiTSDBReceiveRemoteWrite(testutil.NewTB(t))
}

func benchmarkHandlerMultiTSDBReceiveRemoteWrite(b testutil.TB) {
	dir, err := ioutil.TempDir("", "test_receive")
	testutil.Ok(b, err)
	defer func() { testutil.Ok(b, os.RemoveAll(dir)) }()

	biggy := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Samples: []prompb.Sample{
					{
						Value:     1,
						Timestamp: 1,
					},
				},
			},
		},
	}

	lbl := &strings.Builder{}
	lbl.Grow(1024 * 1024 * 10) // 10 MB, 0.5MB compressed.
	for i := 0; i < lbl.Cap()/10; i++ {
		_, _ = lbl.WriteString("abcdefghij")
	}
	biggy.Timeseries[0].Labels = []labelpb.Label{
		{Name: "__name__", Value: lbl.String()},
	}
	body, err := proto.Marshal(biggy)
	testutil.Ok(b, err)

	compressed := snappy.Encode(nil, body)

	handlers, _ := newHandlerHashring([]*fakeAppendable{nil}, 1)
	handler := handlers[0]

	logger := log.NewNopLogger()
	m := NewMultiTSDB(
		dir, logger, prometheus.NewRegistry(), &tsdb.Options{
			MinBlockDuration:  int64(2 * time.Hour / time.Millisecond),
			MaxBlockDuration:  int64(2 * time.Hour / time.Millisecond),
			RetentionDuration: int64(6 * time.Hour / time.Millisecond),
			NoLockfile:        true,
		},
		labels.FromStrings("replica", "01"),
		"tenant_id",
		nil,
		false,
	)
	defer func() { testutil.Ok(b, m.Close()) }()
	handler.writer = NewWriter(logger, m)
	handler.options.DefaultTenantID = "foo"

	testutil.Ok(b, m.Flush())
	testutil.Ok(b, m.Open())

	// It takes time to create new tenant, wait for it.
	app, err := m.TenantAppendable("foo")
	testutil.Ok(b, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	testutil.Ok(b, runutil.Retry(1*time.Second, ctx.Done(), func() error {
		_, err = app.Appender(ctx)
		return err
	}))

	// 15 GB per 500 ops, (30MB 3x blowout) 20GB per 500 ops for non zero copy code, so 40 MB (4x blowout)
	b.ResetTimer()
	// 500 requests per N.
	for i := 0; i < b.N()*500; i++ {
		if i == 1 {
			runtime.GC()
			dumpMemProfile(b, "single_req2.out")
		}
		r := httptest.NewRecorder()
		handler.receiveHTTP(r, &http.Request{Body: ioutil.NopCloser(bytes.NewReader(compressed))})
		testutil.Equals(b, http.StatusOK, r.Code, "got non 200 error: %v", r.Body.String())

		time.Sleep(1 * time.Millisecond)
		if i == 499 {
			runtime.GC()
			dumpMemProfile(b, "multi2.out")
		}
	}
}

func dumpMemProfile(t testing.TB, n string) {
	f, err := os.OpenFile(filepath.Join("../../_dev/dumps", n), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, f.Sync())
		testutil.Ok(t, f.Close())
	}()

	testutil.Ok(t, pprof.WriteHeapProfile(f))
}
