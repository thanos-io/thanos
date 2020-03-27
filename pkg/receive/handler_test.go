// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"google.golang.org/grpc"
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
			Writer:            NewWriter(log.NewNopLogger(), appendables[i]),
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

func TestReceive(t *testing.T) {
	appenderErrFn := func() error { return errors.New("failed to get appender") }
	conflictErrFn := func() error { return storage.ErrOutOfBounds }
	commitErrFn := func() error { return errors.New("failed to commit") }
	wreq1 := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
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
				&fakeAppendable{
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
				&fakeAppendable{
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
				&fakeAppendable{
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
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				&fakeAppendable{
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
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				&fakeAppendable{
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
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				&fakeAppendable{
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
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				&fakeAppendable{
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
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				&fakeAppendable{
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
				&fakeAppendable{
					appender:    newFakeAppender(nil, nil, nil, nil),
					appenderErr: appenderErrFn,
				},
				&fakeAppendable{
					appender:    newFakeAppender(nil, nil, nil, nil),
					appenderErr: appenderErrFn,
				},
				&fakeAppendable{
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
				&fakeAppendable{
					appender: newFakeAppender(conflictErrFn, nil, nil, nil),
				},
				&fakeAppendable{
					appender: newFakeAppender(conflictErrFn, nil, nil, nil),
				},
				&fakeAppendable{
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
				&fakeAppendable{
					appender: newFakeAppender(conflictErrFn, nil, commitErrFn, nil),
				},
				&fakeAppendable{
					appender: newFakeAppender(conflictErrFn, nil, commitErrFn, nil),
				},
				&fakeAppendable{
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
				&fakeAppendable{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil, nil),
				},
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				&fakeAppendable{
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
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, nil, nil),
				},
				&fakeAppendable{
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
				&fakeAppendable{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil, nil),
				},
				&fakeAppendable{
					appender: newFakeAppender(conflictErrFn, nil, nil, nil),
				},
				&fakeAppendable{
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
				&fakeAppendable{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil, nil),
				},
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				&fakeAppendable{
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
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				&fakeAppendable{
					appender: newFakeAppender(nil, nil, commitErrFn, nil),
				},
				&fakeAppendable{
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
				status, err := makeRequest(handler, tenant, tc.wreq)
				if err != nil {
					t.Fatalf("handler %d: unexpectedly failed making HTTP request: %v", tc.status, err)
				}
				if status != tc.status {
					t.Errorf("handler %d: got unexpected HTTP status code: expected %d, got %d", i, tc.status, status)
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
					var expected int
					n := a.appender.(*fakeAppender).samples[lset.String()]
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
func makeRequest(h *Handler, tenant string, wreq *prompb.WriteRequest) (int, error) {
	buf, err := proto.Marshal(wreq)
	if err != nil {
		return 0, errors.Wrap(err, "marshal request")
	}
	req, err := http.NewRequest("POST", h.options.Endpoint, bytes.NewBuffer(snappy.Encode(nil, buf)))
	if err != nil {
		return 0, errors.Wrap(err, "create request")
	}
	req.Header.Add(h.options.TenantHeader, tenant)

	rec := httptest.NewRecorder()
	h.receiveHTTP(rec, req)
	rec.Flush()

	return rec.Code, nil
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
