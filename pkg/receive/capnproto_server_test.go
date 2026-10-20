// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestCapNProtoServer_SingleConcurrentClient(t *testing.T) {
	if testing.
		Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()

	var (
		writer = NewCapNProtoWriter(
			log.NewNopLogger(),
			newFakeTenantAppendable(
				&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
			&CapNProtoWriterOptions{},
		)
		listener = bufconn.Listen(1024)
		handler  = NewCapNProtoHandler(prometheus.NewRegistry(), log.NewNopLogger(), writer)
		srv      = NewCapNProtoServer(listener, handler, log.NewNopLogger())
	)
	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Shutdown()

	for range 200 {
		client := writecapnp.NewRemoteWriteClient(listener, log.NewLogfmtLogger(os.Stdout))
		_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{
			Tenant: "default",
		})
		require.NoError(t, err)
		require.NoError(t, client.Close())
	}
	require.NoError(t, listener.Close())
}

func TestCapNProtoServer_MultipleConcurrentClients(t *testing.T) {
	t.Parallel()

	var (
		writer = NewCapNProtoWriter(
			log.NewNopLogger(),
			newFakeTenantAppendable(
				&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
			&CapNProtoWriterOptions{},
		)
		listener = bufconn.Listen(1024)
		handler  = NewCapNProtoHandler(prometheus.NewRegistry(), log.NewNopLogger(), writer)
		srv      = NewCapNProtoServer(listener, handler, log.NewNopLogger())
	)
	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Shutdown()

	for range 200 {
		client := writecapnp.NewRemoteWriteClient(listener, log.NewLogfmtLogger(os.Stdout))
		_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{
			Tenant: "default",
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, client.Close())
		}()
	}

	require.NoError(t, listener.Close())
}

func TestCapNProtoServer_Compression(t *testing.T) {
	app := newFakeAppender(nil, nil, nil)
	var (
		writer   = NewCapNProtoWriter(log.NewNopLogger(), newFakeTenantAppendable(&fakeAppendable{appender: app}), &CapNProtoWriterOptions{})
		listener = bufconn.Listen(1 << 20)
		srv      = NewCapNProtoServer(listener, NewCapNProtoHandler(prometheus.NewRegistry(), log.NewNopLogger(), writer), log.NewNopLogger())
	)
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Shutdown()

	clients := map[writecapnp.Compression]*writecapnp.RemoteWriteClient{}
	for _, c := range []writecapnp.Compression{writecapnp.CompressionNone, writecapnp.CompressionZstd} {
		clients[c] = writecapnp.NewRemoteWriteClient(listener, log.NewNopLogger(), writecapnp.WithCompression(c))
		defer func() { require.NoError(t, clients[c].Close()) }()
	}

	for r := range 20 {
		for c, client := range clients {
			req := &storepb.WriteRequest{Tenant: "default"}
			for i := range 50 {
				req.Timeseries = append(req.Timeseries, prompb.TimeSeries{
					Labels:  []labelpb.ZLabel{{Name: "__name__", Value: "m"}, {Name: "client", Value: string(c)}, {Name: "series", Value: fmt.Sprintf("%d-%d", r, i)}},
					Samples: []prompb.Sample{{Timestamp: int64(r), Value: float64(i)}},
				})
			}
			_, err := client.RemoteWrite(context.Background(), req)
			require.NoError(t, err)
		}
	}

	for c := range clients {
		for r := range 20 {
			for i := range 50 {
				lset := labels.FromStrings("__name__", "m", "client", string(c), "series", fmt.Sprintf("%d-%d", r, i))
				require.Equal(t, []prompb.Sample{{Timestamp: int64(r), Value: float64(i)}}, app.Get(lset), lset.String())
			}
		}
	}
	require.NoError(t, listener.Close())
}
