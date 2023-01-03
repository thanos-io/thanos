// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"bytes"
	"context"
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
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	"github.com/alecthomas/units"
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

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
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
		appendErr:   appendErr,
		commitErr:   commitErr,
		rollbackErr: rollbackErr,
	}
}

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
func (f *fakeAppender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram) (storage.SeriesRef, error) {
	panic("not implemented")
}

func (f *fakeAppender) GetRef(l labels.Labels) (storage.SeriesRef, labels.Labels) {
	return storage.SeriesRef(l.Hash()), l
}

func (f *fakeAppender) Commit() error {
	return f.commitErr()
}

func (f *fakeAppender) Rollback() error {
	return f.rollbackErr()
}

func newTestHandlerHashring(appendables []*fakeAppendable, replicationFactor uint64, hashringAlgo HashringAlgorithm) ([]*Handler, Hashring) {
	var (
		cfg      = []HashringConfig{{Hashring: "test"}}
		handlers []*Handler
	)
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

	ag := addrGen{}
	limiter, _ := NewLimiter(NewNopConfig(), nil, RouterIngestor, log.NewNopLogger())
	for i := range appendables {
		h := NewHandler(nil, &Options{
			TenantHeader:      DefaultTenantHeader,
			ReplicaHeader:     DefaultReplicaHeader,
			ReplicationFactor: replicationFactor,
			ForwardTimeout:    5 * time.Minute,
			Writer:            NewWriter(log.NewNopLogger(), newFakeTenantAppendable(appendables[i])),
			Limiter:           limiter,
		})
		handlers = append(handlers, h)
		h.peers = peers
		addr := ag.newAddr()
		h.options.Endpoint = addr
		cfg[0].Endpoints = append(cfg[0].Endpoints, h.options.Endpoint)
		peers.cache[addr] = &fakeRemoteWriteGRPCServer{h: h}
	}
	// Use hashmod as default.
	if hashringAlgo == "" {
		hashringAlgo = AlgorithmHashmod
	}

	hashring := newMultiHashring(hashringAlgo, replicationFactor, cfg)
	for _, h := range handlers {
		h.Hashring(hashring)
	}
	return handlers, hashring
}

func testReceiveQuorum(t *testing.T, hashringAlgo HashringAlgorithm, withConsistencyDelay bool) {
	appenderErrFn := func() error { return errors.New("failed to get appender") }
	conflictErrFn := func() error { return storage.ErrOutOfBounds }
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
			handlers, hashring := newTestHandlerHashring(tc.appendables, tc.replicationFactor, hashringAlgo)
			tenant := "test"
			// Test from the point of view of every node
			// so that we know status code does not depend
			// on which node is erroring and which node is receiving.
			for i, handler := range handlers {
				// Test that the correct status is returned.
				rec, err := makeRequest(handler, tenant, tc.wreq)
				if err != nil {
					t.Fatalf("handler %d: unexpectedly failed making HTTP request: %v", i+1, err)
				}
				if rec.Code != tc.status {
					t.Errorf("handler %d: got unexpected HTTP status code: expected %d, got %d; body: %s", i+1, tc.status, rec.Code, rec.Body.String())
				}
			}

			if withConsistencyDelay {
				time.Sleep(50 * time.Millisecond)
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
					if withConsistencyDelay {
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
	testReceiveQuorum(t, AlgorithmHashmod, false)
}

func TestReceiveQuorumKetama(t *testing.T) {
	testReceiveQuorum(t, AlgorithmKetama, false)
}

func TestReceiveWithConsistencyDelayHashmod(t *testing.T) {
	testReceiveQuorum(t, AlgorithmHashmod, true)
}

func TestReceiveWithConsistencyDelayKetama(t *testing.T) {
	testReceiveQuorum(t, AlgorithmKetama, true)
}

func TestReceiveWriteRequestLimits(t *testing.T) {
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
			handlers, _ := newTestHandlerHashring(appendables, 3, AlgorithmHashmod)
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
				limitConfig, nil, RouterIngestor, log.NewNopLogger(),
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

type addrGen struct{ n int }

func (a *addrGen) newAddr() string {
	a.n++
	return fmt.Sprintf("http://node-%d:%d", a.n, 12345+a.n)
}

type fakeRemoteWriteGRPCServer struct {
	h storepb.WriteableStoreServer
}

func (f *fakeRemoteWriteGRPCServer) RemoteWrite(ctx context.Context, in *storepb.WriteRequest, opts ...grpc.CallOption) (*storepb.WriteResponse, error) {
	return f.h.RemoteWrite(ctx, in)
}

func BenchmarkHandlerReceiveHTTP(b *testing.B) {
	benchmarkHandlerMultiTSDBReceiveRemoteWrite(testutil.NewTB(b))
}

func TestHandlerReceiveHTTP(t *testing.T) {
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

func (a *tsOverrideAppender) GetRef(lset labels.Labels) (storage.SeriesRef, labels.Labels) {
	return a.Appender.(storage.GetRef).GetRef(lset)
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
	for i := 0; i < numSeries; i++ {
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

	handlers, _ := newTestHandlerHashring([]*fakeAppendable{nil}, 1, AlgorithmHashmod)
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
		metadata.NoneFunc,
	)
	defer func() { testutil.Ok(b, m.Close()) }()
	handler.writer = NewWriter(logger, m)

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
				for s := 0; s < len(series); s++ {
					lbls := make([]labelpb.ZLabel, 10)
					for i := 0; i < len(lbls); i++ {
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
				for s := 0; s < len(series); s++ {
					lbls := make([]labelpb.ZLabel, 10)
					for i := 0; i < len(lbls); i++ {
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
				for s := 0; s < len(series); s++ {
					lbls := make([]labelpb.ZLabel, 10)
					for i := 0; i < len(lbls); i++ {
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
				for s := 0; s < len(series); s++ {
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
				for i := 0; i < n; i++ {
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
				for i := 0; i < n; i++ {
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

func TestRelabel(t *testing.T) {
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
					SourceLabels: model.LabelNames{"zoo"},
					TargetLabel:  "bar",
					Regex:        relabel.MustNewRegexp("bar"),
					Action:       relabel.Replace,
					Replacement:  "baz",
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
					TargetLabel: "foo",
					Action:      relabel.Replace,
					Regex:       relabel.MustNewRegexp(""),
					Replacement: "test",
				},
				{
					TargetLabel: "__name__",
					Action:      relabel.Replace,
					Regex:       relabel.MustNewRegexp(""),
					Replacement: "foo",
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
					Action: relabel.LabelDrop,
					Regex:  relabel.MustNewRegexp("foo"),
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
					SourceLabels: model.LabelNames{"foo"},
					Action:       relabel.Drop,
					Regex:        relabel.MustNewRegexp("bar"),
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
					Action: relabel.LabelDrop,
					Regex:  relabel.MustNewRegexp("foo"),
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
					SourceLabels: model.LabelNames{"foo"},
					Action:       relabel.Drop,
					Regex:        relabel.MustNewRegexp("bar"),
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
