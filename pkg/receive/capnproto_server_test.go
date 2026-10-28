// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"

	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/storepb"
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
