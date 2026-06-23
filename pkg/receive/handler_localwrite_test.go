// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

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
func newLocalWriteHandler(t *testing.T, rf uint64, endpoints []Endpoint, apps []*fakeAppendable) *Handler {
	t.Helper()
	require.Len(t, apps, 1, "only the local appendable is wired; remote nodes are not dialable in this test")

	logger := log.NewNopLogger()
	limiter, err := NewLimiter(extkingpin.NewNopConfig(), nil, RouterIngestor, log.NewNopLogger(), 1*time.Second)
	require.NoError(t, err)

	h := NewHandler(logger, &Options{
		TenantHeader:        tenancy.DefaultTenantHeader,
		ReplicaHeader:       DefaultReplicaHeader,
		ReplicationFactor:   rf,
		ForwardTimeout:      5 * time.Minute,
		Endpoint:            endpoints[0].Address,
		Writer:              NewWriter(logger, newFakeTenantAppendable(apps[0]), &WriterOptions{}),
		Limiter:             limiter,
		ReplicationProtocol: ProtobufReplication,
	})

	cfg := []HashringConfig{{Hashring: "test", Endpoints: endpoints}}
	hashring, err := NewMultiHashring(AlgorithmHashmod, rf, cfg, prometheus.NewRegistry())
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
		Tenant:     "tenant-a",
		Timeseries: series,
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
		Tenant:     "tenant-a",
		Timeseries: makeSeriesWithValues(1),
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
	h := newLocalWriteHandler(t, 1, []Endpoint{newUniqueEndpoint()}, []*fakeAppendable{app})
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
	h := newLocalWriteHandler(t, 1, []Endpoint{newUniqueEndpoint()}, []*fakeAppendable{app})
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
	h := newLocalWriteHandler(t, 1, []Endpoint{newUniqueEndpoint()}, []*fakeAppendable{app})
	defer h.Close()

	rec, err := makeRequest(h, "tenant-a", &prompb.WriteRequest{Timeseries: makeSeriesWithValues(3)})
	require.NoError(t, err)
	require.Equalf(t, http.StatusConflict, rec.Code, "local conflict should map to 409; body: %s", rec.Body.String())
}
