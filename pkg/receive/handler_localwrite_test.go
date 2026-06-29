// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tenancy"
)

// newLocalWriteHandler builds a Handler backed by the *real* peerGroup (not the
// fakePeersGroup used elsewhere in the tests) so that the local write path goes
// through localAsyncWriter.
func newLocalWriteHandler(t *testing.T, endpoints []Endpoint, apps []*fakeAppendable) *Handler {
	t.Helper()
	require.Len(t, apps, 1, "only the local appendable is wired; remote nodes are not dialable in this test")

	logger := log.NewNopLogger()
	limiter, err := NewLimiter(extkingpin.NewNopConfig(), nil, RouterIngestor, log.NewNopLogger(), 1*time.Second)
	require.NoError(t, err)

	h := NewHandler(logger, &Options{
		TenantHeader:        tenancy.DefaultTenantHeader,
		ReplicaHeader:       DefaultReplicaHeader,
		ReplicationFactor:   1,
		ForwardTimeout:      5 * time.Minute,
		Endpoint:            endpoints[0].Address,
		Writer:              NewWriter(logger, newFakeTenantAppendable(apps[0]), &WriterOptions{}),
		Limiter:             limiter,
		ReplicationProtocol: ProtobufReplication,
	})

	cfg := []HashringConfig{{Hashring: "test", Endpoints: endpoints}}
	hashring, err := NewMultiHashring(AlgorithmHashmod, 1, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	h.Hashring(hashring)

	return h
}

// TestLocalAsyncWriterRemoteWritePersists ensures localAsyncWriter actually
// writes the request to the underlying TSDB.
func TestLocalAsyncWriterRemoteWritePersists(t *testing.T) {
	fa := newFakeAppender(nil, nil, nil)
	app := &fakeAppendable{appender: fa}
	w := NewWriter(log.NewNopLogger(), newFakeTenantAppendable(app), &WriterOptions{})
	lw := &localAsyncWriter{w: w}

	series := makeSeriesWithValues(5)
	resp, err := lw.RemoteWrite(context.Background(), &storepb.WriteRequest{
		TimeseriesTenantData: []storepb.TimeSeriesTenantTuple{
			{Tenant: "tenant-a", Timeseries: series},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp, "RemoteWrite must return a non-nil response on success")

	for _, ts := range series {
		lset := labelpb.ZLabelsToPromLabels(ts.Labels)
		require.Lenf(t, fa.Get(lset), len(ts.Samples), "series %s was not persisted locally", lset.String())
	}
}

// TestLocalAsyncWriterRemoteWritePropagatesError ensures a local write failure
// surfaces as an error to the caller. A no-op RemoteWrite would mask backend
// errors and make the handler return a success code while losing data.
func TestLocalAsyncWriterRemoteWritePropagatesError(t *testing.T) {
	app := &fakeAppendable{
		appender:    newFakeAppender(nil, nil, nil),
		appenderErr: func() error { return errors.New("appender unavailable") },
	}
	w := NewWriter(log.NewNopLogger(), newFakeTenantAppendable(app), &WriterOptions{})
	lw := &localAsyncWriter{w: w}

	_, err := lw.RemoteWrite(context.Background(), &storepb.WriteRequest{
		TimeseriesTenantData: []storepb.TimeSeriesTenantTuple{
			{Tenant: "tenant-a", Timeseries: makeSeriesWithValues(1)},
		},
	})
	require.Error(t, err)
}

// TestReceiveLocalWritePersistsEndToEnd drives a full HTTP remote-write request
// through a single-node receiver whose only hashring member is itself. This is
// the real local-write path (peerGroup.getConnection -> localAsyncWriter) that
// the existing test harness mocks away.
func TestReceiveLocalWritePersistsEndToEnd(t *testing.T) {
	fa := newFakeAppender(nil, nil, nil)
	app := &fakeAppendable{appender: fa}
	h := newLocalWriteHandler(t, []Endpoint{newUniqueEndpoint()}, []*fakeAppendable{app})
	defer h.Close()

	series := makeSeriesWithValues(10)
	rec, err := makeRequest(h, "tenant-a", &prompb.WriteRequest{Timeseries: series})
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, rec.Code, "unexpected status; body: %s", rec.Body.String())

	for _, ts := range series {
		lset := labelpb.ZLabelsToPromLabels(ts.Labels)
		require.Lenf(t, fa.Get(lset), len(ts.Samples), "series %s was not persisted locally", lset.String())
	}
}

// TestReceiveLocalWriteReportsFailureWhenLocalWriteFails verifies the headline
// promise of the commit: when the local write fails the handler must return a
// non-2xx status instead of pretending the write succeeded.
func TestReceiveLocalWriteReportsFailureWhenLocalWriteFails(t *testing.T) {
	app := &fakeAppendable{
		appender:    newFakeAppender(nil, nil, nil),
		appenderErr: func() error { return errors.New("appender unavailable") },
	}
	h := newLocalWriteHandler(t, []Endpoint{newUniqueEndpoint()}, []*fakeAppendable{app})
	defer h.Close()

	rec, err := makeRequest(h, "tenant-a", &prompb.WriteRequest{Timeseries: makeSeriesWithValues(3)})
	require.NoError(t, err)
	require.NotEqualf(t, http.StatusOK, rec.Code, "local write failed but handler returned OK; body: %s", rec.Body.String())
}

// TestReceiveLocalWriteConflictReturns409 verifies a conflict from the local TSDB
// (e.g. an out-of-bounds sample) is surfaced as HTTP 409, not 503. localAsyncWriter
// wraps the writer error with fmt.Errorf("writing locally: %w", err); the status
// classifier resolves the cause with errors.Cause (github.com/pkg/errors), which
// follows Cause() but NOT the %w chain of fmt.Errorf, so the conflict sentinel is
// lost and the handler degrades the response to 503. Wrapping with errors.Wrap
// instead preserves the Cause() chain that writeErrors/isConflict rely on.
func TestReceiveLocalWriteConflictReturns409(t *testing.T) {
	app := &fakeAppendable{
		appender: newFakeAppender(func() error { return storage.ErrOutOfBounds }, nil, nil),
	}
	h := newLocalWriteHandler(t, []Endpoint{newUniqueEndpoint()}, []*fakeAppendable{app})
	defer h.Close()

	rec, err := makeRequest(h, "tenant-a", &prompb.WriteRequest{Timeseries: makeSeriesWithValues(3)})
	require.NoError(t, err)
	require.Equalf(t, http.StatusConflict, rec.Code, "local conflict should map to 409; body: %s", rec.Body.String())
}

// recordingWriteClient wraps a localAsyncWriter and records every WriteRequest it
// receives, so a test can assert how many writes the fanout collapsed into and
// which tenant data each carried.
type recordingWriteClient struct {
	lw   *localAsyncWriter
	mu   sync.Mutex
	reqs []*storepb.WriteRequest
}

func (c *recordingWriteClient) RemoteWrite(ctx context.Context, in *storepb.WriteRequest, opts ...grpc.CallOption) (*storepb.WriteResponse, error) {
	c.mu.Lock()
	c.reqs = append(c.reqs, in)
	c.mu.Unlock()
	return c.lw.RemoteWrite(ctx, in, opts...)
}

func (c *recordingWriteClient) RemoteWriteAsync(ctx context.Context, in *storepb.WriteRequest, er endpointReplica, seriesIDs []int, responses chan writeResponse, cb func(error)) {
	_, err := c.RemoteWrite(ctx, in)
	responses <- writeResponse{er: er, err: err, seriesIDs: seriesIDs}
	cb(err)
}

func (c *recordingWriteClient) TryRemoteWriteAsync(ctx context.Context, in *storepb.WriteRequest, er endpointReplica, seriesIDs []int, responses chan writeResponse, cb func(error)) bool {
	c.RemoteWriteAsync(ctx, in, er, seriesIDs, responses, cb)
	return true
}

func (c *recordingWriteClient) Close() error { return nil }

// singleClientPeers is a peersContainer that hands the same client to every
// endpoint, letting a test observe the writes a handler emits.
type singleClientPeers struct{ c WriteableStoreAsyncClient }

func (p *singleClientPeers) getConnection(context.Context, Endpoint) (WriteableStoreAsyncClient, error) {
	return p.c, nil
}
func (p *singleClientPeers) close(Endpoint) error         { return nil }
func (p *singleClientPeers) markPeerUnavailable(Endpoint) {}
func (p *singleClientPeers) markPeerAvailable(Endpoint)   {}
func (p *singleClientPeers) reset()                       {}
func (p *singleClientPeers) Close() error                 { return nil }

// TestReceiveMultiTenantSplitWritesOnce drives a single remote-write request
// whose series belong to two tenants (resolved via the split-tenant label)
// through the handler and asserts the fanout splits the data per tenant but
// emits exactly one write to the destination carrying both tenants.
func TestReceiveMultiTenantSplitWritesOnce(t *testing.T) {
	const tenantLabel = "thanos_tenant_id"

	fa := newFakeAppender(nil, nil, nil)
	app := &fakeAppendable{appender: fa}
	h := newLocalWriteHandler(t, []Endpoint{newUniqueEndpoint()}, []*fakeAppendable{app})
	defer h.Close()

	h.splitTenantLabelName = tenantLabel
	rec := &recordingWriteClient{lw: &localAsyncWriter{w: h.writer}}
	h.peers = &singleClientPeers{c: rec}

	mkSeries := func(tenant, metric string, val float64) prompb.TimeSeries {
		return prompb.TimeSeries{
			Labels:  labelpb.ZLabelsFromPromLabels(labels.FromStrings("__name__", metric, tenantLabel, tenant)),
			Samples: []prompb.Sample{{Value: val, Timestamp: 10}},
		}
	}

	wreq := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		mkSeries("tenant-a", "metric_a", 1),
		mkSeries("tenant-b", "metric_b", 2),
		mkSeries("tenant-a", "metric_c", 3),
	}}

	httpRec, err := makeRequest(h, "default", wreq)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, httpRec.Code, "unexpected status; body: %s", httpRec.Body.String())

	require.Len(t, rec.reqs, 1)

	gotByTenant := map[string][]string{}
	for _, tup := range rec.reqs[0].TimeseriesTenantData {
		for _, ts := range tup.Timeseries {
			lset := labelpb.ZLabelsToPromLabels(ts.Labels)
			require.Empty(t, lset.Get(tenantLabel), "split tenant label must be stripped from forwarded series")
			gotByTenant[tup.Tenant] = append(gotByTenant[tup.Tenant], lset.Get("__name__"))
		}
	}
	require.Len(t, gotByTenant, 2)
	require.ElementsMatch(t, []string{"metric_a", "metric_c"}, gotByTenant["tenant-a"])
	require.ElementsMatch(t, []string{"metric_b"}, gotByTenant["tenant-b"])

	for _, metric := range []string{"metric_a", "metric_b", "metric_c"} {
		require.Lenf(t, fa.Get(labels.FromStrings("__name__", metric)), 1, "series %s was not persisted exactly once", metric)
	}
}
