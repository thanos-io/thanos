// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"bytes"
	"context"
	goerrors "errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/alecthomas/units"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	prometheusMetadata "github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tenancy"
)

type fakeTenantAppendable struct {
	f *fakeAppendable
}

func newFakeTenantAppendable(f *fakeAppendable) *fakeTenantAppendable {
	return &fakeTenantAppendable{f: f}
}

func (t *fakeTenantAppendable) TenantAppendable(_ string) (Appendable, error) {
	return t.f, nil
}

type fakeAppendable struct {
	appender    storage.Appender
	appenderErr func() error
}

var _ Appendable = &fakeAppendable{}

func nilErrFn() error {
	return nil
}

func (f *fakeAppendable) Appender(_ context.Context) (storage.Appender, error) {
	errf := f.appenderErr
	if errf == nil {
		errf = nilErrFn
	}
	return f.appender, errf()
}

type fakeAppender struct {
	sync.Mutex
	samples     map[storage.SeriesRef][]prompb.Sample
	exemplars   map[storage.SeriesRef][]exemplar.Exemplar
	appendErr   func() error
	commitErr   func() error
	rollbackErr func() error
}

var _ storage.Appender = &fakeAppender{}
var _ storage.GetRef = &fakeAppender{}

func newFakeAppender(appendErr, commitErr, rollbackErr func() error) *fakeAppender { //nolint:unparam
	if appendErr == nil {
		appendErr = nilErrFn
	}
	if commitErr == nil {
		commitErr = nilErrFn
	}
	if rollbackErr == nil {
		rollbackErr = nilErrFn
	}
	return &fakeAppender{
		samples:     make(map[storage.SeriesRef][]prompb.Sample),
		exemplars:   make(map[storage.SeriesRef][]exemplar.Exemplar),
		appendErr:   appendErr,
		commitErr:   commitErr,
		rollbackErr: rollbackErr,
	}
}

func (f *fakeAppender) SetOptions(opts *storage.AppendOptions) {}

func (f *fakeAppender) UpdateMetadata(storage.SeriesRef, labels.Labels, prometheusMetadata.Metadata) (storage.SeriesRef, error) {
	return 0, nil
}

func (f *fakeAppender) Get(l labels.Labels) []prompb.Sample {
	f.Lock()
	defer f.Unlock()
	s := f.samples[storage.SeriesRef(l.Hash())]
	res := make([]prompb.Sample, len(s))
	copy(res, s)
	return res
}

func (f *fakeAppender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	f.Lock()
	defer f.Unlock()
	if ref == 0 {
		ref = storage.SeriesRef(l.Hash())
	}
	f.samples[ref] = append(f.samples[ref], prompb.Sample{Timestamp: t, Value: v})
	return ref, f.appendErr()
}

func (f *fakeAppender) AppendExemplar(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	f.Lock()
	defer f.Unlock()
	if ref == 0 {
		ref = storage.SeriesRef(l.Hash())
	}
	f.exemplars[ref] = append(f.exemplars[ref], e)
	return ref, f.appendErr()
}

// TODO(rabenhorst): Needs to be implement for native histogram support.
func (f *fakeAppender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	panic("not implemented")
}

// TODO(sungjin1212): Needs to be implement for native histogram support.
func (f *fakeAppender) AppendHistogramCTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	panic("not implemented")
}

func (f *fakeAppender) AppendHistogramSTZeroSample(ref storage.SeriesRef, l labels.Labels, t, st int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	panic("not implemented")
}

func (f *fakeAppender) AppendSTZeroSample(ref storage.SeriesRef, l labels.Labels, t, st int64) (storage.SeriesRef, error) {
	panic("not implemented")
}

func (f *fakeAppender) GetRef(l labels.Labels, hash uint64) (storage.SeriesRef, labels.Labels) {
	return storage.SeriesRef(hash), l
}

func (f *fakeAppender) Commit() error {
	return f.commitErr()
}

func (f *fakeAppender) Rollback() error {
	return f.rollbackErr()
}

func (f *fakeAppender) AppendCTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64) (storage.SeriesRef, error) {
	panic("not implemented")
}

type fakePeersGroup struct {
	clients map[Endpoint]*peerWorker

	closeCalled map[Endpoint]bool
}

func (g *fakePeersGroup) Close() error {
	for _, c := range g.clients {
		c.wp.Close()
	}
	return nil
}

func (g *fakePeersGroup) markPeerUnavailable(s Endpoint) {
}

func (g *fakePeersGroup) markPeerAvailable(s Endpoint) {
}

func (g *fakePeersGroup) reset() {
}

func (g *fakePeersGroup) close(addr Endpoint) error {
	if g.closeCalled == nil {
		g.closeCalled = map[Endpoint]bool{}
	}
	g.closeCalled[addr] = true
	g.clients[addr].wp.Close()
	return nil
}

func (g *fakePeersGroup) getConnection(_ context.Context, endpoint Endpoint) (WriteableStoreAsyncClient, error) {
	c, ok := g.clients[endpoint]
	if !ok {
		return nil, fmt.Errorf("client %s not found", endpoint)
	}
	return c, nil
}

var _ = (peersContainer)(&fakePeersGroup{})

func newTestHandlerHashring(
	appendables []*fakeAppendable,
	replicationFactor uint64,
	hashringAlgo HashringAlgorithm,
	capnpReplication bool,
) ([]*Handler, Hashring, func() error, error) {
	var (
		cfg      = []HashringConfig{{Hashring: "test"}}
		handlers []*Handler
		wOpts    = &WriterOptions{}
	)
	fakePeers := &fakePeersGroup{
		clients: map[Endpoint]*peerWorker{},
	}

	var (
		closers = make([]func() error, 0)

		ag         = addrGen{}
		logger, _  = logging.NewLogger("debug", "logfmt", "receive_test")
		limiter, _ = NewLimiter(extkingpin.NewNopConfig(), nil, RouterIngestor, log.NewNopLogger(), 1*time.Second)
	)
	for i := range appendables {
		h := NewHandler(logger, &Options{
			TenantHeader:      tenancy.DefaultTenantHeader,
			ReplicaHeader:     DefaultReplicaHeader,
			ReplicationFactor: replicationFactor,
			ForwardTimeout:    5 * time.Minute,
			Writer:            NewWriter(log.NewNopLogger(), newFakeTenantAppendable(appendables[i]), wOpts),
			Limiter:           limiter,
		})
		handlers = append(handlers, h)
		h.peers = fakePeers
		endpoint := ag.newEndpoint()
		h.options.Endpoint = endpoint.Address
		cfg[0].Endpoints = append(cfg[0].Endpoints, endpoint)

		var peer *peerWorker
		if capnpReplication {
			writer := NewCapNProtoWriter(logger, newFakeTenantAppendable(appendables[i]), nil)
			var (
				listener = bufconn.Listen(1024)
				handler  = NewCapNProtoHandler(log.NewNopLogger(), writer)
			)
			srv := NewCapNProtoServer(listener, handler, log.NewNopLogger())
			client := writecapnp.NewRemoteWriteClient(listener, logger)
			peer = newPeerWorker(client, prometheus.NewHistogram(prometheus.HistogramOpts{}), 1, 0)
			closers = append(closers, func() error {
				srv.Shutdown()
				return goerrors.Join(listener.Close(), client.Close())
			})
			go func() { _ = srv.ListenAndServe() }()
		} else {
			peer = newPeerWorker(&fakeRemoteWriteGRPCServer{h: h}, prometheus.NewHistogram(prometheus.HistogramOpts{}), 1, 0)
		}
		fakePeers.clients[endpoint] = peer
	}
	// Use hashmod as default.
	if hashringAlgo == "" {
		hashringAlgo = AlgorithmHashmod
	}

	hashring, err := NewMultiHashring(hashringAlgo, replicationFactor, cfg, prometheus.NewRegistry())
	if err != nil {
		return nil, nil, nil, err
	}
	for _, h := range handlers {
		h.Hashring(hashring)
	}
	closeFunc := func() error {
		errs := make([]error, 0, len(closers))
		for _, closeFunc := range closers {
			errs = append(errs, closeFunc())
		}
		return goerrors.Join(errs...)
	}
	return handlers, hashring, closeFunc, nil
}

func testReceiveQuorum(t *testing.T, hashringAlgo HashringAlgorithm, withConsistencyDelay, capnpReplication bool) {
	appenderErrFn := func() error { return errors.New("failed to get appender") }
	conflictErrFn := func() error { return storage.ErrOutOfBounds }
	tooOldSampleErrFn := func() error { return storage.ErrTooOldSample }
	commitErrFn := func() error { return errors.New("failed to commit") }
	wreq := &prompb.WriteRequest{
		Timeseries: makeSeriesWithValues(50),
	}

	for _, tc := range []struct {
		name              string
		status            int
		replicationFactor uint64
		wreq              *prompb.WriteRequest
		appendables       []*fakeAppendable
		randomNode        bool
	}{
		{
			name:              "size 1 success",
			status:            http.StatusOK,
			replicationFactor: 1,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:              "size 1 commit error",
			status:            http.StatusInternalServerError,
			replicationFactor: 1,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
			},
		},
		{
			name:              "size 1 conflict",
			status:            http.StatusConflict,
			replicationFactor: 1,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
			},
		},
		{
			name:              "size 2 success",
			status:            http.StatusOK,
			replicationFactor: 1,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 success",
			status:            http.StatusOK,
			replicationFactor: 1,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 success with replication",
			status:            http.StatusOK,
			replicationFactor: 3,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 commit error",
			status:            http.StatusInternalServerError,
			replicationFactor: 1,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
			},
		},
		{
			name:              "size 3 commit error with replication",
			status:            http.StatusInternalServerError,
			replicationFactor: 3,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
			},
		},
		{
			name:              "size 3 appender error with replication",
			status:            http.StatusInternalServerError,
			replicationFactor: 3,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender:    newFakeAppender(nil, nil, nil),
					appenderErr: appenderErrFn,
				},
				{
					appender:    newFakeAppender(nil, nil, nil),
					appenderErr: appenderErrFn,
				},
				{
					appender:    newFakeAppender(nil, nil, nil),
					appenderErr: appenderErrFn,
				},
			},
		},
		{
			name:              "size 3 conflict with replication",
			status:            http.StatusConflict,
			replicationFactor: 3,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
			},
		},
		{
			name:              "size 3 conflict and commit error with replication",
			status:            http.StatusConflict,
			replicationFactor: 3,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, commitErrFn, nil),
				},
			},
		},
		{
			name:              "size 3 conflict with replication and error is ErrTooOldSample",
			status:            http.StatusConflict,
			replicationFactor: 3,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(tooOldSampleErrFn, nil, nil),
				},
				{
					appender: newFakeAppender(tooOldSampleErrFn, nil, nil),
				},
				{
					appender: newFakeAppender(tooOldSampleErrFn, nil, nil),
				},
			},
		},
		{
			name:              "size 3 with replication and one faulty",
			status:            http.StatusOK,
			replicationFactor: 3,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 with replication and one commit error",
			status:            http.StatusOK,
			replicationFactor: 3,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 with replication and two conflicts",
			status:            http.StatusConflict,
			replicationFactor: 3,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:              "size 3 with replication one conflict and one commit error",
			status:            http.StatusInternalServerError,
			replicationFactor: 3,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil),
				},
			},
		},
		{
			name:              "size 3 with replication two commit errors",
			status:            http.StatusInternalServerError,
			replicationFactor: 3,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:              "size 6 with replication 3",
			status:            http.StatusOK,
			replicationFactor: 3,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:              "size 6 with replication 3 one commit and two conflict error",
			status:            http.StatusConflict,
			replicationFactor: 3,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, conflictErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, conflictErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:              "size 6 with replication 5 two commit errors",
			status:            http.StatusOK,
			replicationFactor: 5,
			wreq:              wreq,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handlers, hashring, closeFunc, err := newTestHandlerHashring(tc.appendables, tc.replicationFactor, hashringAlgo, capnpReplication)
			if err != nil {
				t.Fatalf("unable to create test handler: %v", err)
			}
			defer func() {
				testutil.Ok(t, closeFunc())
				// Wait a few milliseconds for peer workers to process the queue.
				time.AfterFunc(50*time.Millisecond, func() {
					for _, h := range handlers {
						h.Close()
					}
				})
			}()
			tenant := "test"

			if tc.randomNode {
				handler := handlers[0]
				rec, err := makeRequest(handler, tenant, tc.wreq)
				if err != nil {
					t.Fatalf("handler: unexpectedly failed making HTTP request: %v", err)
				}
				if rec.Code != tc.status {
					t.Errorf("handler: got unexpected HTTP status code: expected %d, got %d; body: %s", tc.status, rec.Code, rec.Body.String())
				}
			} else {
				// Test from the point of view of every node
				// so that we know status code does not depend
				// on which node is erroring and which node is receiving.
				for i, handler := range handlers {
					// Test that the correct status is returned.
					rec, err := makeRequest(handler, tenant, tc.wreq)
					if err != nil {
						t.Fatalf("handler %d: unexpectedly failed making HTTP request: %v", i+1, err)
					}
					// TODO(GiedriusS): fix this for gRPC replication too.
					if capnpReplication {
						if rec.Code == 503 {
							rec.Code = 500
						}
					}
					if rec.Code != tc.status {
						t.Errorf("handler %d: got unexpected HTTP status code: expected %d, got %d; body: %s", i+1, tc.status, rec.Code, rec.Body.String())
					}
				}
			}

			if withConsistencyDelay {
				time.Sleep(50 * time.Millisecond)
			}

			// Test that each time series is stored
			// the correct amount of times in each fake DB.
			for _, ts := range tc.wreq.Timeseries {
				lset := labelpb.ZLabelsToPromLabels(ts.Labels)
				for j, a := range tc.appendables {
					if withConsistencyDelay && tc.status == http.StatusOK {
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
					} else {
						var expectedMin int
						n := a.appender.(*fakeAppender).Get(lset)
						got := uint64(len(n))
						if a.appenderErr == nil && endpointHit(t, hashring, tc.replicationFactor, handlers[j].options.Endpoint, tenant, &ts) {
							// We have len(handlers) copies of each sample because the test case
							// is run once for each handler and they all use the same appender.
							expectedMin = int((tc.replicationFactor/2)+1) * len(ts.Samples)
							if tc.randomNode {
								expectedMin = len(ts.Samples)
							}
							// When the write fails, early failure quorum return may cancel
							// in-flight remote writes before they reach the appender, so
							// we can only guarantee at least one write landed.
							if tc.status != http.StatusOK {
								expectedMin = len(ts.Samples)
							}
						}
						if uint64(expectedMin) > got {
							t.Errorf("handler: %d, labels %q: expected minimum of %d samples, got %d", j, lset.String(), expectedMin, got)
						}
					}

				}
			}
		})
	}
}

func TestReceiveQuorumHashmod(t *testing.T) {
	t.Parallel()

	for _, capnpReplication := range []bool{false, true} {
		t.Run(fmt.Sprintf("capnproto-replication=%t", capnpReplication), func(t *testing.T) {
			testReceiveQuorum(t, AlgorithmHashmod, false, capnpReplication)
		})
	}
}

func TestReceiveQuorumKetama(t *testing.T) {
	t.Parallel()

	for _, capnpReplication := range []bool{false, true} {
		t.Run(fmt.Sprintf("capnproto-replication=%t", capnpReplication), func(t *testing.T) {
			testReceiveQuorum(t, AlgorithmKetama, false, capnpReplication)
		})
	}
}

func TestReceiveWithConsistencyDelayHashmod(t *testing.T) {
	if testing.
		Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()

	for _, capnpReplication := range []bool{false, true} {
		t.Run(fmt.Sprintf("capnproto-replication=%t", capnpReplication), func(t *testing.T) {
			testReceiveQuorum(t, AlgorithmHashmod, true, capnpReplication)
		})
	}
}

func TestReceiveWithConsistencyDelayKetama(t *testing.T) {
	if testing.
		Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()

	for _, capnpReplication := range []bool{false, true} {
		t.Run(fmt.Sprintf("capnproto-replication=%t", capnpReplication), func(t *testing.T) {
			testReceiveQuorum(t, AlgorithmKetama, true, capnpReplication)
		})
	}
}

func TestReceiveWriteRequestLimits(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		status        int
		amountSeries  int
		amountSamples int
	}{
		{
			name:         "Request above limit of series",
			status:       http.StatusRequestEntityTooLarge,
			amountSeries: 21,
		},
		{
			name:         "Request under the limit of series",
			status:       http.StatusOK,
			amountSeries: 20,
		},
		{
			name:          "Request above limit of samples (series * samples)",
			status:        http.StatusRequestEntityTooLarge,
			amountSeries:  30,
			amountSamples: 15,
		},
		{
			name:          "Request under the limit of samples (series * samples)",
			status:        http.StatusOK,
			amountSeries:  10,
			amountSamples: 2,
		},
		{
			name:          "Request above body size limit",
			status:        http.StatusRequestEntityTooLarge,
			amountSeries:  300,
			amountSamples: 150,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.amountSamples == 0 {
				tc.amountSamples = 1
			}

			appendables := []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			}
			handlers, _, closeFunc, err := newTestHandlerHashring(appendables, 3, AlgorithmHashmod, false)
			if err != nil {
				t.Fatalf("unable to create test handler: %v", err)
			}
			defer func() {
				testutil.Ok(t, closeFunc())
				// Wait a few milliseconds for peer workers to process the queue.
				time.AfterFunc(50*time.Millisecond, func() {
					for _, h := range handlers {
						h.Close()
					}
				})
			}()

			handler := handlers[0]

			tenant := "test"
			tenantConfig, err := yaml.Marshal(&RootLimitsConfig{
				WriteLimits: WriteLimitsConfig{
					TenantsLimits: TenantsWriteLimitsConfig{
						tenant: &WriteLimitConfig{
							RequestLimits: NewEmptyRequestLimitsConfig().
								SetSizeBytesLimit(int64(1 * units.Megabyte)).
								SetSeriesLimit(20).
								SetSamplesLimit(200),
						},
					},
				},
			})
			if err != nil {
				t.Fatal("handler: failed to generate limit configuration")
			}
			tmpLimitsPath := path.Join(t.TempDir(), "limits.yaml")
			testutil.Ok(t, os.WriteFile(tmpLimitsPath, tenantConfig, 0666))
			limitConfig, _ := extkingpin.NewStaticPathContent(tmpLimitsPath)
			handler.Limiter, _ = NewLimiter(
				limitConfig, nil, RouterIngestor, log.NewNopLogger(), 1*time.Second,
			)

			wreq := &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{},
			}

			for i := 0; i < tc.amountSeries; i += 1 {
				label := labelpb.ZLabel{Name: "foo", Value: "bar"}
				series := prompb.TimeSeries{
					Labels: []labelpb.ZLabel{label},
				}
				for j := 0; j < tc.amountSamples; j += 1 {
					sample := prompb.Sample{Value: float64(j), Timestamp: int64(j)}
					series.Samples = append(series.Samples, sample)
				}
				wreq.Timeseries = append(wreq.Timeseries, series)
			}

			// Test that the correct status is returned.
			rec, err := makeRequest(handler, tenant, wreq)
			if err != nil {
				t.Fatalf("handler %d: unexpectedly failed making HTTP request: %v", tc.status, err)
			}
			if rec.Code != tc.status {
				t.Errorf("handler: got unexpected HTTP status code: expected %d, got %d; body: %s", tc.status, rec.Code, rec.Body.String())
			}
		})
	}
}

// endpointHit is a helper to determine if a given endpoint in a hashring would be selected
// for a given time series, tenant, and replication factor.
func endpointHit(t *testing.T, h Hashring, rf uint64, endpoint, tenant string, timeSeries *prompb.TimeSeries) bool {
	for i := range rf {
		e, err := h.GetN(tenant, timeSeries, i)
		if err != nil {
			t.Fatalf("got unexpected error querying hashring: %v", err)
		}
		if e.HasAddress(endpoint) {
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

type addrGen struct{ n int }

func (a *addrGen) newEndpoint() Endpoint {
	a.n++
	addr := fmt.Sprintf("http://node-%d:%d", a.n, 12345+a.n)
	return Endpoint{
		Address:          addr,
		CapNProtoAddress: addr,
	}
}

type fakeRemoteWriteGRPCServer struct {
	h storepb.WriteableStoreServer
}

func (f *fakeRemoteWriteGRPCServer) RemoteWrite(ctx context.Context, in *storepb.WriteRequest, opts ...grpc.CallOption) (*storepb.WriteResponse, error) {
	return f.h.RemoteWrite(ctx, in)
}

func (f *fakeRemoteWriteGRPCServer) RemoteWriteAsync(ctx context.Context, in *storepb.WriteRequest, er endpointReplica, seriesIDs []int, responses chan writeResponse, cb func(error)) {
	_, err := f.h.RemoteWrite(ctx, in)
	responses <- writeResponse{
		er:        er,
		err:       err,
		seriesIDs: seriesIDs,
	}
	cb(err)
}

func (f *fakeRemoteWriteGRPCServer) Close() error { return nil }

func BenchmarkHandlerReceiveHTTP(b *testing.B) {
	benchmarkHandlerMultiTSDBReceiveRemoteWrite(testutil.NewTB(b))
}

func TestHandlerReceiveHTTP(t *testing.T) {
	if testing.
		Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()

	benchmarkHandlerMultiTSDBReceiveRemoteWrite(testutil.NewTB(t))
}

// tsOverrideTenantStorage is storage that overrides timestamp to make it have consistent interval.
type tsOverrideTenantStorage struct {
	TenantStorage

	interval int64
}

func (s *tsOverrideTenantStorage) TenantAppendable(tenant string) (Appendable, error) {
	a, err := s.TenantStorage.TenantAppendable(tenant)
	return &tsOverrideAppendable{Appendable: a, interval: s.interval}, err
}

type tsOverrideAppendable struct {
	Appendable

	interval int64
}

func (a *tsOverrideAppendable) Appender(ctx context.Context) (storage.Appender, error) {
	ret, err := a.Appendable.Appender(ctx)
	return &tsOverrideAppender{Appender: ret, interval: a.interval}, err
}

type tsOverrideAppender struct {
	storage.Appender

	interval int64
}

var cnt int64

func (a *tsOverrideAppender) Append(ref storage.SeriesRef, l labels.Labels, _ int64, v float64) (storage.SeriesRef, error) {
	cnt += a.interval
	return a.Appender.Append(ref, l, cnt, v)
}

func (a *tsOverrideAppender) GetRef(lset labels.Labels, hash uint64) (storage.SeriesRef, labels.Labels) {
	return a.Appender.(storage.GetRef).GetRef(lset, hash)
}

// serializeSeriesWithOneSample returns marshaled and compressed remote write requests like it would
// be sent to Thanos receive.
// It has one sample and allow passing multiple series, in same manner as typical Prometheus would batch it.
func serializeSeriesWithOneSample(t testing.TB, series [][]labelpb.ZLabel) []byte {
	r := &prompb.WriteRequest{Timeseries: make([]prompb.TimeSeries, 0, len(series))}

	for _, s := range series {
		r.Timeseries = append(r.Timeseries, prompb.TimeSeries{
			Labels: s,
			// Timestamp does not matter, it will be overridden.
			Samples: []prompb.Sample{{Value: math.MaxFloat64, Timestamp: math.MinInt64}},
		})
	}
	body, err := proto.Marshal(r)
	testutil.Ok(t, err)
	return snappy.Encode(nil, body)
}

func makeSeriesWithValues(numSeries int) []prompb.TimeSeries {
	series := make([]prompb.TimeSeries, numSeries)
	for i := range numSeries {
		series[i] = prompb.TimeSeries{
			Labels: []labelpb.ZLabel{
				{
					Name:  fmt.Sprintf("pod-%d", i),
					Value: fmt.Sprintf("nginx-%d", i),
				},
			},
			Samples: []prompb.Sample{
				{
					Value:     float64(i),
					Timestamp: 10,
				},
			},
		}
	}
	return series
}

func benchmarkHandlerMultiTSDBReceiveRemoteWrite(b testutil.TB) {
	dir := b.TempDir()

	handlers, _, closeFunc, err := newTestHandlerHashring([]*fakeAppendable{nil}, 1, AlgorithmHashmod, false)
	if err != nil {
		b.Fatalf("unable to create test handler: %v", err)
	}
	defer func() {
		testutil.Ok(b, closeFunc())
	}()
	handler := handlers[0]

	reg := prometheus.NewRegistry()

	logger := log.NewNopLogger()
	m := NewMultiTSDB(
		dir, logger, reg, &tsdb.Options{
			MinBlockDuration:  int64(2 * time.Hour / time.Millisecond),
			MaxBlockDuration:  int64(2 * time.Hour / time.Millisecond),
			RetentionDuration: int64(6 * time.Hour / time.Millisecond),
			NoLockfile:        true,
			StripeSize:        1, // Disable stripe pre allocation so we can have clear profiles.
		},
		labels.FromStrings("replica", "01"),
		"tenant_id",
		nil,
		false,
		false,
		metadata.NoneFunc,
	)
	defer func() { testutil.Ok(b, m.Close()) }()
	handler.writer = NewWriter(logger, m, &WriterOptions{})

	testutil.Ok(b, m.Flush())
	testutil.Ok(b, m.Open())

	for _, tcase := range []struct {
		name         string
		writeRequest []byte
	}{
		{
			name: "typical labels under 1KB, 500 of them",
			writeRequest: serializeSeriesWithOneSample(b, func() [][]labelpb.ZLabel {
				series := make([][]labelpb.ZLabel, 500)
				for s := range series {
					lbls := make([]labelpb.ZLabel, 10)
					for i := range lbls {
						// Label ~20B name, 50B value.
						lbls[i] = labelpb.ZLabel{Name: fmt.Sprintf("abcdefghijabcdefghijabcdefghij%d", i), Value: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i)}
					}
					series[s] = lbls
				}
				return series
			}()),
		},
		{
			name: "typical labels under 1KB, 5000 of them",
			writeRequest: serializeSeriesWithOneSample(b, func() [][]labelpb.ZLabel {
				series := make([][]labelpb.ZLabel, 5000)
				for s := range series {
					lbls := make([]labelpb.ZLabel, 10)
					for i := range lbls {
						// Label ~20B name, 50B value.
						lbls[i] = labelpb.ZLabel{Name: fmt.Sprintf("abcdefghijabcdefghijabcdefghij%d", i), Value: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i)}
					}
					series[s] = lbls
				}
				return series
			}()),
		},
		{
			name: "typical labels under 1KB, 20000 of them",
			writeRequest: serializeSeriesWithOneSample(b, func() [][]labelpb.ZLabel {
				series := make([][]labelpb.ZLabel, 20000)
				for s := range series {
					lbls := make([]labelpb.ZLabel, 10)
					for i := range lbls {
						// Label ~20B name, 50B value.
						lbls[i] = labelpb.ZLabel{Name: fmt.Sprintf("abcdefghijabcdefghijabcdefghij%d", i), Value: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i)}
					}
					series[s] = lbls
				}
				return series
			}()),
		},
		{
			name: "extremely large label value 10MB, 10 of them",
			writeRequest: serializeSeriesWithOneSample(b, func() [][]labelpb.ZLabel {
				series := make([][]labelpb.ZLabel, 10)
				for s := range series {
					lbl := &strings.Builder{}
					lbl.Grow(1024 * 1024 * 10) // 10MB.
					word := "abcdefghij"
					for i := 0; i < lbl.Cap()/len(word); i++ {
						_, _ = lbl.WriteString(word)
					}
					series[s] = []labelpb.ZLabel{{Name: "__name__", Value: lbl.String()}}
				}
				return series
			}()),
		},
	} {
		b.Run(tcase.name, func(b testutil.TB) {
			handler.options.DefaultTenantID = fmt.Sprintf("%v-ok", tcase.name)
			handler.writer.multiTSDB = &tsOverrideTenantStorage{TenantStorage: m, interval: 1}

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
				n := b.N()
				b.ResetTimer()
				for range n {
					r := httptest.NewRecorder()
					handler.receiveHTTP(r, &http.Request{ContentLength: int64(len(tcase.writeRequest)), Body: io.NopCloser(bytes.NewReader(tcase.writeRequest))})
					testutil.Equals(b, http.StatusOK, r.Code, "got non 200 error: %v", r.Body.String())
				}
			})

			handler.options.DefaultTenantID = fmt.Sprintf("%v-conflicting", tcase.name)
			handler.writer.multiTSDB = &tsOverrideTenantStorage{TenantStorage: m, interval: -1} // Timestamp can't go down, which will cause conflict error.

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
			handler.receiveHTTP(r, &http.Request{ContentLength: int64(len(tcase.writeRequest)), Body: io.NopCloser(bytes.NewReader(tcase.writeRequest))})
			testutil.Equals(b, http.StatusOK, r.Code, "got non 200 error: %v", r.Body.String())

			b.Run("conflict errors", func(b testutil.TB) {
				n := b.N()
				b.ResetTimer()
				for i := range n {
					r := httptest.NewRecorder()
					handler.receiveHTTP(r, &http.Request{ContentLength: int64(len(tcase.writeRequest)), Body: io.NopCloser(bytes.NewReader(tcase.writeRequest))})
					testutil.Equals(b, http.StatusConflict, r.Code, "%v-%s", i, func() string {
						b, _ := io.ReadAll(r.Body)
						return string(b)
					}())
				}
			})
		})
	}

	runtime.GC()
	// Take snapshot at the end to reveal how much memory we keep in TSDB.
	testutil.Ok(b, Heap("../../../_dev/thanos/2021/receive2"))

}

func Heap(dir string) (err error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(dir, "errimpr1-go1.16.3.pprof"))
	if err != nil {
		return err
	}
	defer runutil.CloseWithErrCapture(&err, f, "close")
	return pprof.WriteHeapProfile(f)
}

func TestIsTenantValid(t *testing.T) {
	t.Parallel()

	for _, tcase := range []struct {
		name   string
		tenant string

		expectedErr error
	}{
		{
			name:        "test malicious tenant",
			tenant:      "/etc/foo",
			expectedErr: errors.New("Tenant name not valid"),
		},
		{
			name:        "test malicious tenant going out of receiver directory",
			tenant:      "./../../hacker_dir",
			expectedErr: errors.New("Tenant name not valid"),
		},
		{
			name:        "test slash-only tenant",
			tenant:      "///",
			expectedErr: errors.New("Tenant name not valid"),
		},
		{
			name:   "test default tenant",
			tenant: "default-tenant",
		},
		{
			name:   "test tenant with uuid",
			tenant: "528d0490-8720-4478-aa29-819d90fc9a9f",
		},
		{
			name:   "test valid tenant",
			tenant: "foo",
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			err := tenancy.IsTenantValid(tcase.tenant)
			if tcase.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tcase.expectedErr.Error(), err.Error())
				return
			}
			testutil.Ok(t, err)
		})
	}
}

func TestRelabel(t *testing.T) {
	t.Parallel()

	for _, tcase := range []struct {
		name                 string
		relabel              []*relabel.Config
		writeRequest         prompb.WriteRequest
		expectedWriteRequest prompb.WriteRequest
	}{
		{
			name: "empty relabel configs",
			writeRequest: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "__name__",
								Value: "test_metric",
							},
							{
								Name:  "foo",
								Value: "bar",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: 0,
								Value:     1,
							},
						},
					},
				},
			},
			expectedWriteRequest: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "__name__",
								Value: "test_metric",
							},
							{
								Name:  "foo",
								Value: "bar",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: 0,
								Value:     1,
							},
						},
					},
				},
			},
		},
		{
			name: "has relabel configs but no relabelling applied",
			relabel: []*relabel.Config{
				{
					SourceLabels:         model.LabelNames{"zoo"},
					TargetLabel:          "bar",
					Regex:                relabel.MustNewRegexp("bar"),
					Action:               relabel.Replace,
					Replacement:          "baz",
					NameValidationScheme: model.UTF8Validation,
				},
			},
			writeRequest: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "__name__",
								Value: "test_metric",
							},
							{
								Name:  "foo",
								Value: "bar",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: 0,
								Value:     1,
							},
						},
					},
				},
			},
			expectedWriteRequest: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "__name__",
								Value: "test_metric",
							},
							{
								Name:  "foo",
								Value: "bar",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: 0,
								Value:     1,
							},
						},
					},
				},
			},
		},
		{
			name: "relabel rewrite existing labels",
			relabel: []*relabel.Config{
				{
					TargetLabel:          "foo",
					Action:               relabel.Replace,
					Regex:                relabel.MustNewRegexp(""),
					Replacement:          "test",
					NameValidationScheme: model.UTF8Validation,
				},
				{
					TargetLabel:          "__name__",
					Action:               relabel.Replace,
					Regex:                relabel.MustNewRegexp(""),
					Replacement:          "foo",
					NameValidationScheme: model.UTF8Validation,
				},
			},
			writeRequest: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "__name__",
								Value: "test_metric",
							},
							{
								Name:  "foo",
								Value: "bar",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: 0,
								Value:     1,
							},
						},
					},
				},
			},
			expectedWriteRequest: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "__name__",
								Value: "foo",
							},
							{
								Name:  "foo",
								Value: "test",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: 0,
								Value:     1,
							},
						},
					},
				},
			},
		},
		{
			name: "relabel drops label",
			relabel: []*relabel.Config{
				{
					Action:               relabel.LabelDrop,
					Regex:                relabel.MustNewRegexp("foo"),
					NameValidationScheme: model.UTF8Validation,
				},
			},
			writeRequest: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "__name__",
								Value: "test_metric",
							},
							{
								Name:  "foo",
								Value: "bar",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: 0,
								Value:     1,
							},
						},
					},
				},
			},
			expectedWriteRequest: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "__name__",
								Value: "test_metric",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: 0,
								Value:     1,
							},
						},
					},
				},
			},
		},
		{
			name: "relabel drops time series",
			relabel: []*relabel.Config{
				{
					SourceLabels:         model.LabelNames{"foo"},
					Action:               relabel.Drop,
					Regex:                relabel.MustNewRegexp("bar"),
					NameValidationScheme: model.UTF8Validation,
				},
			},
			writeRequest: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "__name__",
								Value: "test_metric",
							},
							{
								Name:  "foo",
								Value: "bar",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: 0,
								Value:     1,
							},
						},
					},
				},
			},
			expectedWriteRequest: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{},
			},
		},
		{
			name: "relabel rewrite existing exemplar series labels",
			relabel: []*relabel.Config{
				{
					Action:               relabel.LabelDrop,
					Regex:                relabel.MustNewRegexp("foo"),
					NameValidationScheme: model.UTF8Validation,
				},
			},
			writeRequest: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "__name__",
								Value: "test_metric",
							},
							{
								Name:  "foo",
								Value: "bar",
							},
						},
						Exemplars: []prompb.Exemplar{
							{
								Labels: []labelpb.ZLabel{
									{
										Name:  "traceID",
										Value: "foo",
									},
								},
								Value:     1,
								Timestamp: 1,
							},
						},
					},
				},
			},
			expectedWriteRequest: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "__name__",
								Value: "test_metric",
							},
						},
						Exemplars: []prompb.Exemplar{
							{
								Labels: []labelpb.ZLabel{
									{
										Name:  "traceID",
										Value: "foo",
									},
								},
								Value:     1,
								Timestamp: 1,
							},
						},
					},
				},
			},
		},
		{
			name: "relabel drops exemplars",
			relabel: []*relabel.Config{
				{
					SourceLabels:         model.LabelNames{"foo"},
					Action:               relabel.Drop,
					Regex:                relabel.MustNewRegexp("bar"),
					NameValidationScheme: model.UTF8Validation,
				},
			},
			writeRequest: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "__name__",
								Value: "test_metric",
							},
							{
								Name:  "foo",
								Value: "bar",
							},
						},
						Exemplars: []prompb.Exemplar{
							{
								Labels: []labelpb.ZLabel{
									{
										Name:  "traceID",
										Value: "foo",
									},
								},
								Value:     1,
								Timestamp: 1,
							},
						},
					},
				},
			},
			expectedWriteRequest: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{},
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			h := NewHandler(nil, &Options{
				RelabelConfigs: tcase.relabel,
			})

			h.relabel(&tcase.writeRequest)
			testutil.Equals(t, tcase.expectedWriteRequest, tcase.writeRequest)
		})
	}
}

func TestGetStatsLimitParameter(t *testing.T) {
	t.Parallel()

	t.Run("invalid limit parameter, not integer", func(t *testing.T) {
		r, err := http.NewRequest(http.MethodGet, "http://0:0", nil)
		testutil.Ok(t, err)

		q := r.URL.Query()
		q.Add(LimitStatsQueryParam, "abc")
		r.URL.RawQuery = q.Encode()

		_, err = getStatsLimitParameter(r)
		testutil.NotOk(t, err)
	})
	t.Run("invalid limit parameter, too large", func(t *testing.T) {
		r, err := http.NewRequest(http.MethodGet, "http://0:0", nil)
		testutil.Ok(t, err)

		q := r.URL.Query()
		q.Add(LimitStatsQueryParam, strconv.FormatUint(math.MaxInt+1, 10))
		r.URL.RawQuery = q.Encode()

		_, err = getStatsLimitParameter(r)
		testutil.NotOk(t, err)
	})
	t.Run("not present returns default", func(t *testing.T) {
		r, err := http.NewRequest(http.MethodGet, "http://0:0", nil)
		testutil.Ok(t, err)

		limit, err := getStatsLimitParameter(r)
		testutil.Ok(t, err)
		testutil.Equals(t, limit, DefaultStatsLimit)
	})
	t.Run("if present and valid, the parameter is returned", func(t *testing.T) {
		r, err := http.NewRequest(http.MethodGet, "http://0:0", nil)
		testutil.Ok(t, err)

		const givenLimit = 20

		q := r.URL.Query()
		q.Add(LimitStatsQueryParam, strconv.FormatUint(givenLimit, 10))
		r.URL.RawQuery = q.Encode()

		limit, err := getStatsLimitParameter(r)
		testutil.Ok(t, err)
		testutil.Equals(t, limit, givenLimit)
	})
}

func TestHashringChangeCallsClose(t *testing.T) {
	t.Parallel()

	appendables := []*fakeAppendable{
		{
			appender: newFakeAppender(nil, nil, nil),
		},
		{
			appender: newFakeAppender(nil, nil, nil),
		},
		{
			appender: newFakeAppender(nil, nil, nil),
		},
	}
	allHandlers, _, closeFunc, err := newTestHandlerHashring(appendables, 3, AlgorithmHashmod, false)
	testutil.Ok(t, err)
	testutil.Ok(t, closeFunc())

	appendables = appendables[1:]

	_, smallHashring, closeFunc, err := newTestHandlerHashring(appendables, 2, AlgorithmHashmod, false)
	testutil.Ok(t, err)
	testutil.Ok(t, closeFunc())

	allHandlers[0].Hashring(smallHashring)

	pg := allHandlers[0].peers.(*fakePeersGroup)
	testutil.Assert(t, len(pg.closeCalled) > 0)
}

func TestHandlerEarlyStop(t *testing.T) {
	t.Parallel()

	h := NewHandler(nil, &Options{})
	h.Close()

	err := h.Run()
	testutil.NotOk(t, err)
	testutil.Equals(t, "http: Server closed", err.Error())
}

type hashringSeenTenants struct {
	Hashring

	seenTenants map[string]struct{}
}

func (h *hashringSeenTenants) GetN(tenant string, ts *prompb.TimeSeries, n uint64) (Endpoint, error) {
	if h.seenTenants == nil {
		h.seenTenants = map[string]struct{}{}
	}
	h.seenTenants[tenant] = struct{}{}
	return h.Hashring.GetN(tenant, ts, n)
}

func TestDistributeSeries(t *testing.T) {
	t.Parallel()

	const tenantIDLabelName = "thanos_tenant_id"
	h := NewHandler(nil, &Options{
		SplitTenantLabelName: tenantIDLabelName,
	})

	endpoint := Endpoint{Address: "http://localhost:9090", CapNProtoAddress: "http://localhost:19391"}
	hashring, err := newSimpleHashring([]Endpoint{endpoint})
	require.NoError(t, err)
	hr := &hashringSeenTenants{Hashring: hashring}
	h.Hashring(hr)

	_, remote, err := h.distributeTimeseriesToReplicas(
		"foo",
		[]uint64{0},
		[]prompb.TimeSeries{
			{
				Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("a", "b", tenantIDLabelName, "bar")),
			},
			{
				Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("b", "a", tenantIDLabelName, "boo")),
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, remote, 1)
	require.Len(t, remote[endpointReplica{endpoint: endpoint, replica: 0}]["bar"].timeSeries, 1)
	require.Len(t, remote[endpointReplica{endpoint: endpoint, replica: 0}]["boo"].timeSeries, 1)

	require.Equal(t, 1, labelpb.ZLabelsToPromLabels(remote[endpointReplica{endpoint: endpoint, replica: 0}]["bar"].timeSeries[0].Labels).Len())
	require.Equal(t, 1, labelpb.ZLabelsToPromLabels(remote[endpointReplica{endpoint: endpoint, replica: 0}]["boo"].timeSeries[0].Labels).Len())

	require.Equal(t, map[string]struct{}{"bar": {}, "boo": {}}, hr.seenTenants)
}

func TestHandlerSplitTenantLabelLocalWrite(t *testing.T) {
	const tenantIDLabelName = "thanos_tenant_id"

	appendable := &fakeAppendable{
		appender: newFakeAppender(nil, nil, nil),
	}

	h := NewHandler(nil, &Options{
		Endpoint:             "localhost",
		SplitTenantLabelName: tenantIDLabelName,
		ReceiverMode:         RouterIngestor,
		ReplicationFactor:    1,
		ForwardTimeout:       1 * time.Second,
		Writer: NewWriter(
			log.NewNopLogger(),
			newFakeTenantAppendable(appendable),
			&WriterOptions{},
		),
	})

	// initialize hashring with a single local endpoint matching the handler endpoint to force
	// using local write
	hashring, err := newSimpleHashring([]Endpoint{
		{
			Address: h.options.Endpoint,
		},
	})
	require.NoError(t, err)
	hr := &hashringSeenTenants{Hashring: hashring}
	h.Hashring(hr)

	response, err := h.RemoteWrite(context.Background(), &storepb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: labelpb.ZLabelsFromPromLabels(
					labels.FromStrings("a", "b", tenantIDLabelName, "bar"),
				),
			},
			{
				Labels: labelpb.ZLabelsFromPromLabels(
					labels.FromStrings("b", "a", tenantIDLabelName, "foo"),
				),
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, map[string]struct{}{"bar": {}, "foo": {}}, hr.seenTenants)
}

func TestHandlerFlippingHashrings(t *testing.T) {
	t.Parallel()

	h := NewHandler(log.NewLogfmtLogger(os.Stderr), &Options{})
	t.Cleanup(h.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	h1, err := newSimpleHashring([]Endpoint{
		{
			Address: "http://localhost:9090",
		},
	})
	require.NoError(t, err)
	h2, err := newSimpleHashring([]Endpoint{
		{
			Address: "http://localhost:9091",
		},
	})
	require.NoError(t, err)

	h.Hashring(h1)

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-time.After(50 * time.Millisecond):
			case <-ctx.Done():
				return
			}

			_, err := h.handleRequest(ctx, 0, "test", &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("foo", "bar")),
						Samples: []prompb.Sample{
							{
								Timestamp: time.Now().Unix(),
								Value:     123,
							},
						},
					},
				},
			})
			require.Error(t, err)
		}
	}()
	go func() {
		defer wg.Done()
		var flipper bool

		for {
			select {
			case <-time.After(200 * time.Millisecond):
			case <-ctx.Done():
				return
			}

			if flipper {
				h.Hashring(h2)
			} else {
				h.Hashring(h1)
			}
			flipper = !flipper
		}
	}()

	<-time.After(1 * time.Second)
	cancel()
	wg.Wait()
}
