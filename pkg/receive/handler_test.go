// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

func newTestHandlerHashring(appendables []*fakeAppendable, replicationFactor uint64) ([]*Handler, Hashring) {
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
			ForwardTimeout:    10 * time.Second,
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
			handlers, hashring := newTestHandlerHashring(tc.appendables, tc.replicationFactor)
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
			handlers, hashring := newTestHandlerHashring(tc.appendables, tc.replicationFactor)
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

func serialize(t testing.TB, lbls []labelpb.Label, samples []prompb.Sample) []byte {
	// Create significant number of samples to see the weight of it on profiles.
	r := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:  lbls,
				Samples: samples,
			},
		},
	}
	body, err := proto.Marshal(r)
	testutil.Ok(t, err)
	return snappy.Encode(nil, body)
}

func BenchmarkHandlerReceiveHTTP(b *testing.B) {
	benchmarkHandlerMultiTSDBReceiveRemoteWrite(testutil.NewTBWithAlloc(b))
}

func TestHandlerReceiveHTTP(t *testing.T) {
	benchmarkHandlerMultiTSDBReceiveRemoteWrite(testutil.NewTB(t))
}

type tsModifierTenantStorage struct {
	TenantStorage

	modifier int64
}

func (s *tsModifierTenantStorage) TenantAppendable(tenant string) (Appendable, error) {
	a, err := s.TenantStorage.TenantAppendable(tenant)
	return &tsModifierAppendable{Appendable: a, modifier: s.modifier}, err
}

type tsModifierAppendable struct {
	Appendable

	modifier int64
}

func (a *tsModifierAppendable) Appender(ctx context.Context) (storage.Appender, error) {
	ret, err := a.Appendable.Appender(ctx)
	return &tsModifierAppender{Appender: ret, modifier: a.modifier}, err
}

type tsModifierAppender struct {
	storage.Appender

	modifier int64
}

var cnt int64

func (a *tsModifierAppender) Add(l labels.Labels, _ int64, v float64) (uint64, error) {
	cnt += a.modifier
	return a.Appender.Add(l, cnt, v)
}

func (a *tsModifierAppender) AddFast(ref uint64, _ int64, v float64) error {
	cnt += a.modifier
	return a.Appender.AddFast(ref, cnt, v)
}

func benchmarkHandlerMultiTSDBReceiveRemoteWrite(b testutil.TB) {
	dir, err := ioutil.TempDir("", "test_receive")
	testutil.Ok(b, err)
	defer func() { testutil.Ok(b, os.RemoveAll(dir)) }()

	handlers, _ := newTestHandlerHashring([]*fakeAppendable{nil}, 1)
	handler := handlers[0]

	reg := prometheus.NewRegistry()

	logger := log.NewNopLogger()
	m := NewMultiTSDB(
		dir, logger, reg, &tsdb.Options{
			MinBlockDuration:  int64(2 * time.Hour / time.Millisecond),
			MaxBlockDuration:  int64(2 * time.Hour / time.Millisecond),
			RetentionDuration: int64(6 * time.Hour / time.Millisecond),
			NoLockfile:        true,
			StripeSize:        1, // Disable stripe pre allocation so we can clear profiles.
		},
		labels.FromStrings("replica", "01"),
		"tenant_id",
		nil,
		false,
	)
	defer func() { testutil.Ok(b, m.Close()) }()
	handler.writer = NewWriter(logger, m)

	testutil.Ok(b, m.Flush())
	testutil.Ok(b, m.Open())

	// Create 2MB of samples payload.
	manySamples := make([]prompb.Sample, 10e4)
	for i := range manySamples {
		manySamples[i] = prompb.Sample{
			Value:     math.MaxFloat64,
			Timestamp: math.MinInt64, // Timestamp does not matter, it will be overriden.
		}
	}

	for _, tcase := range []struct {
		name         string
		writeRequest []byte
	}{
		{
			name: "typical labels under 1KB, single sample",
			writeRequest: serialize(b, func() []labelpb.Label {
				lbls := make([]labelpb.Label, 10)
				for i := 0; i < len(lbls); i++ {
					// Label ~20B name, 50B value.
					lbls[i] = labelpb.Label{Name: fmt.Sprintf("abcdefghijabcdefghijabcdefghij%d", i), Value: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i)}
				}
				return lbls
			}(), []prompb.Sample{{Value: math.MaxFloat64, Timestamp: math.MinInt64}}), // Timestamp does not matter, it will be overriden.
		},
		{
			name: "typical labels under 1KB, 2MB of samples",
			writeRequest: serialize(b, func() []labelpb.Label {
				lbls := make([]labelpb.Label, 10)
				for i := 0; i < len(lbls); i++ {
					// Label ~20B name, 50B value.
					lbls[i] = labelpb.Label{Name: fmt.Sprintf("abcdefghijabcdefghijabcdefghij%d", i), Value: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i)}
				}
				return lbls
			}(), manySamples),
		},
		{
			name: "bigger labels over 1KB, single sample",
			writeRequest: serialize(b, func() []labelpb.Label {
				lbls := make([]labelpb.Label, 10)
				for i := 0; i < len(lbls); i++ {
					// Label ~50B name, 50B value.
					lbls[i] = labelpb.Label{Name: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i), Value: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i)}
				}
				return lbls
			}(), []prompb.Sample{{Value: math.MaxFloat64, Timestamp: math.MinInt64}}), // Timestamp does not matter, it will be overriden.
		},
		{
			name: "bigger labels over 1KB, 2MB of samples",
			writeRequest: serialize(b, func() []labelpb.Label {
				lbls := make([]labelpb.Label, 10)
				for i := 0; i < len(lbls); i++ {
					// Label ~50B name, 50B value.
					lbls[i] = labelpb.Label{Name: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i), Value: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i)}
				}
				return lbls
			}(), manySamples),
		},
		{
			name: "extremely large label value 10MB, single sample",
			writeRequest: serialize(b, func() []labelpb.Label {
				lbl := &strings.Builder{}
				lbl.Grow(1024 * 1024 * 10) // 10MB.
				word := "abcdefghij"
				for i := 0; i < lbl.Cap()/len(word); i++ {
					_, _ = lbl.WriteString(word)
				}
				return []labelpb.Label{{Name: "__name__", Value: lbl.String()}}
			}(), []prompb.Sample{{Value: math.MaxFloat64, Timestamp: math.MinInt64}}), // Timestamp does not matter, it will be overriden.
		},
		{
			name: "extremely large label value 10MB, 2MB samples",
			writeRequest: serialize(b, func() []labelpb.Label {
				lbl := &strings.Builder{}
				lbl.Grow(1024 * 1024 * 10) // 10MB.
				word := "abcdefghij"
				for i := 0; i < lbl.Cap()/len(word); i++ {
					_, _ = lbl.WriteString(word)
				}
				return []labelpb.Label{{Name: "__name__", Value: lbl.String()}}
			}(), manySamples),
		},
	} {
		b.Run(tcase.name, func(b testutil.TB) {
			handler.options.DefaultTenantID = fmt.Sprintf("%v-ok", tcase.name)
			handler.writer.multiTSDB = &tsModifierTenantStorage{TenantStorage: m, modifier: 1}

			// It takes time to create new tenant, wait for it.
			{
				app, err := m.TenantAppendable(handler.options.DefaultTenantID)
				testutil.Ok(b, err)

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				testutil.Ok(b, runutil.Retry(1*time.Second, ctx.Done(), func() error {
					_, err = app.Appender(ctx)
					return err
				}))
			}

			b.Run("OK", func(b testutil.TB) {
				b.ReportAllocs()

				n := b.N()
				b.ResetTimer()
				for i := 0; i < n; i++ {
					//if i == 1 {
					//	runtime.GC()
					//	dumpMemProfile(b, fmt.Sprintf("single_%v.out", tcase.name))
					//}

					r := httptest.NewRecorder()
					handler.receiveHTTP(r, &http.Request{ContentLength: int64(len(tcase.writeRequest)), Body: ioutil.NopCloser(bytes.NewReader(tcase.writeRequest))})
					testutil.Equals(b, http.StatusOK, r.Code, "got non 200 error: %v", r.Body.String())

					//if i == n-1 {
					//	runtime.GC()
					//	dumpMemProfile(b,fmt.Sprintf("multi_%v.out", tcase.name))
					//
					//	// Clear series, and see if resources are released.
					//  db := m.tenants[handler.options.DefaultTenantID].readyStorage().Get()
					//	fmt.Println(db.Head().NumSeries())
					//	testutil.Ok(b, db.Head().Truncate(timestamp.FromTime(time.Now())))
					//	fmt.Println(db.Head().NumSeries())
					//
					//	runtime.GC()
					//	dumpMemProfile(b, "multi_postdelete5.out")
					//}
				}
			})

			handler.options.DefaultTenantID = fmt.Sprintf("%v-conflicting", tcase.name)
			handler.writer.multiTSDB = &tsModifierTenantStorage{TenantStorage: m, modifier: -1} // Timestamp can't go down
			// which will cause conflict error.

			// It takes time to create new tenant, wait for it.
			{
				app, err := m.TenantAppendable(handler.options.DefaultTenantID)
				testutil.Ok(b, err)

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				testutil.Ok(b, runutil.Retry(1*time.Second, ctx.Done(), func() error {
					_, err = app.Appender(ctx)
					return err
				}))
			}
			// First request should be fine, since we don't change timestamp, rest is wrong.
			r := httptest.NewRecorder()
			handler.receiveHTTP(r, &http.Request{ContentLength: int64(len(tcase.writeRequest)), Body: ioutil.NopCloser(bytes.NewReader(tcase.writeRequest))})
			testutil.Equals(b, http.StatusOK, r.Code, "got non 200 error: %v", r.Body.String())

			b.Run("conflict errors", func(b testutil.TB) {
				b.ReportAllocs()

				n := b.N()
				b.ResetTimer()
				for i := 0; i < n; i++ {
					//if i == 1 {
					//	runtime.GC()
					//	dumpMemProfile(b, fmt.Sprintf("single_err_%v.out", tcase.name))
					//}
					r := httptest.NewRecorder()
					handler.receiveHTTP(r, &http.Request{ContentLength: int64(len(tcase.writeRequest)), Body: ioutil.NopCloser(bytes.NewReader(tcase.writeRequest))})
					testutil.Equals(b, http.StatusConflict, r.Code, "%v", i)

					//if i == n-1 {
					//	runtime.GC()
					//	dumpMemProfile(b, fmt.Sprintf("multi_err_%v.out", tcase.name))
					//}
				}
			})
		})
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
