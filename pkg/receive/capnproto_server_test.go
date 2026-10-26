// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func newNopLimiter(tb testing.TB) *Limiter {
	limiter, err := NewLimiter(extkingpin.NewNopConfig(), nil, RouterIngestor, log.NewNopLogger(), 1*time.Second)
	require.NoError(tb, err)
	return limiter
}

func TestCapNProtoServer_WriteGate(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "limits.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("write:\n  global:\n    max_concurrency: 1\n"), 0o600))
	config, err := extkingpin.NewStaticPathContent(configPath)
	require.NoError(t, err)

	synctest.Test(t, func(t *testing.T) {
		reg := prometheus.NewRegistry()
		limiter, err := NewLimiter(config, reg, RouterIngestor, log.NewNopLogger(), 1*time.Second)
		require.NoError(t, err)

		finishedGateWaits := func() uint64 {
			families, err := reg.Gather()
			require.NoError(t, err)
			for _, family := range families {
				if family.GetName() == "thanos_receive_write_request_concurrent_gate_write_requests_duration_seconds" {
					return family.GetMetric()[0].GetHistogram().GetSampleCount()
				}
			}
			return 0
		}

		var (
			writer = NewCapNProtoWriter(
				log.NewNopLogger(),
				newFakeTenantAppendable(
					&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
				&CapNProtoWriterOptions{},
			)
			listener = bufconn.Listen(1024)
			handler  = NewCapNProtoHandler(prometheus.NewRegistry(), log.NewNopLogger(), writer, limiter)
			srv      = NewCapNProtoServer(listener, handler, log.NewNopLogger())
		)
		go func() {
			_ = srv.ListenAndServe()
		}()
		defer srv.Shutdown()

		client := writecapnp.NewRemoteWriteClient(listener, log.NewNopLogger())

		_, err = client.RemoteWrite(context.Background(), &storepb.WriteRequest{Tenant: "default"})
		require.NoError(t, err)

		require.NoError(t, limiter.WriteGate().Start(context.Background()))
		require.Equal(t, uint64(2), finishedGateWaits())

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_, err = client.RemoteWrite(ctx, &storepb.WriteRequest{Tenant: "default"})
		require.Error(t, err)

		// The server-side wait must end on cancellation, while the gate is still held.
		synctest.Wait()
		require.Equal(t, uint64(3), finishedGateWaits())
		limiter.WriteGate().Done()

		_, err = client.RemoteWrite(context.Background(), &storepb.WriteRequest{Tenant: "default"})
		require.NoError(t, err)

		require.NoError(t, client.Close())
		require.NoError(t, listener.Close())
	})
}

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
		handler  = NewCapNProtoHandler(prometheus.NewRegistry(), log.NewNopLogger(), writer, newNopLimiter(t))
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
		handler  = NewCapNProtoHandler(prometheus.NewRegistry(), log.NewNopLogger(), writer, newNopLimiter(t))
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
		srv      = NewCapNProtoServer(listener, NewCapNProtoHandler(prometheus.NewRegistry(), log.NewNopLogger(), writer, newNopLimiter(t)), log.NewNopLogger())
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

// A zstd client against a server from before this change must fail fast, not hang.
func TestCapNProtoServer_CompressedClientAgainstLegacyServer(t *testing.T) {
	t.Parallel()

	var (
		app      = newFakeAppender(nil, nil, nil)
		writer   = NewCapNProtoWriter(log.NewNopLogger(), newFakeTenantAppendable(&fakeAppendable{appender: app}), &CapNProtoWriterOptions{})
		handler  = NewCapNProtoHandler(prometheus.NewRegistry(), log.NewNopLogger(), writer, newNopLimiter(t))
		listener = bufconn.Listen(1 << 20)
		server   = writecapnp.Writer_ServerToClient(handler)
	)
	defer server.Release()

	// A server from before this change: packed stream only, no preface sniffing.
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				rpcConn := rpc.NewConn(rpc.NewPackedStreamTransport(conn), &rpc.Options{BootstrapClient: capnp.Client(server).AddRef()})
				<-rpcConn.Done()
			}()
		}
	}()
	defer func() { require.NoError(t, listener.Close()) }()

	client := writecapnp.NewRemoteWriteClient(listener, log.NewNopLogger(), writecapnp.WithCompression(writecapnp.CompressionZstd))
	defer func() { require.NoError(t, client.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := client.RemoteWrite(ctx, &storepb.WriteRequest{Tenant: "default"})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code(), "a legacy peer must fail fast, not hang until the deadline: %v", err)
	require.Contains(t, err.Error(), "compression=zstd", "the error must say which compression was in use")
}

func TestCapNProtoServer_SurvivesConnectionWithoutPreface(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	var logsMu sync.Mutex
	logger := level.NewFilter(log.LoggerFunc(func(kv ...interface{}) error {
		logsMu.Lock()
		defer logsMu.Unlock()
		return log.NewLogfmtLogger(&logs).Log(kv...)
	}), level.AllowDebug())

	var (
		writer   = NewCapNProtoWriter(log.NewNopLogger(), newFakeTenantAppendable(&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}), &CapNProtoWriterOptions{})
		listener = bufconn.Listen(1 << 20)
		srv      = NewCapNProtoServer(listener, NewCapNProtoHandler(prometheus.NewRegistry(), log.NewNopLogger(), writer, newNopLimiter(t)), logger)
	)
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Shutdown()

	conn, err := listener.DialContext(context.Background())
	require.NoError(t, err)
	_, err = conn.Write([]byte("TZ"))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	require.Eventually(t, func() bool {
		logsMu.Lock()
		defer logsMu.Unlock()
		return strings.Contains(logs.String(), "failed to set up capnproto transport")
	}, 5*time.Second, 10*time.Millisecond)

	client := writecapnp.NewRemoteWriteClient(listener, log.NewNopLogger(), writecapnp.WithCompression(writecapnp.CompressionZstd))
	defer func() { require.NoError(t, client.Close()) }()
	_, err = client.RemoteWrite(context.Background(), &storepb.WriteRequest{Tenant: "default"})
	require.NoError(t, err)
	require.NoError(t, listener.Close())
}

func TestCapNProtoServer_CompressionConcurrencyOption(t *testing.T) {
	t.Parallel()

	app := newFakeAppender(nil, nil, nil)
	var (
		writer   = NewCapNProtoWriter(log.NewNopLogger(), newFakeTenantAppendable(&fakeAppendable{appender: app}), &CapNProtoWriterOptions{})
		listener = bufconn.Listen(1 << 20)
		srv      = NewCapNProtoServer(listener, NewCapNProtoHandler(prometheus.NewRegistry(), log.NewNopLogger(), writer, newNopLimiter(t)), log.NewNopLogger(), WithCapNProtoCompressionConcurrency(0))
	)
	require.Equal(t, 0, srv.compressionConcurrency)
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Shutdown()

	client := writecapnp.NewRemoteWriteClient(listener, log.NewNopLogger(), writecapnp.WithCompression(writecapnp.CompressionZstd), writecapnp.WithCompressionConcurrency(0))
	defer func() { require.NoError(t, client.Close()) }()

	lset := labels.FromStrings("__name__", "m", "series", "0")
	_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{
		Tenant: "default",
		Timeseries: []prompb.TimeSeries{{
			Labels:  labelpb.ZLabelsFromPromLabels(lset),
			Samples: []prompb.Sample{{Timestamp: 1, Value: 1}},
		}},
	})
	require.NoError(t, err)
	require.Equal(t, []prompb.Sample{{Timestamp: 1, Value: 1}}, app.Get(lset))
	require.NoError(t, listener.Close())
}
