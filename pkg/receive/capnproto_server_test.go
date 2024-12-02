// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestCapNProtoServer_SingleConcurrentClient(t *testing.T) {
	t.Parallel()

	var (
		writer = NewCapNProtoWriter(
			log.NewNopLogger(),
			newFakeTenantAppendable(
				&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
			&CapNProtoWriterOptions{},
		)
		listener = bufconn.Listen(1024)
		handler  = NewCapNProtoHandler(log.NewNopLogger(), writer)
		srv      = NewCapNProtoServer(listener, handler, log.NewNopLogger())
	)
	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Shutdown()

	for i := 0; i < 1000; i++ {
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
		handler  = NewCapNProtoHandler(log.NewNopLogger(), writer)
		srv      = NewCapNProtoServer(listener, handler, log.NewNopLogger())
	)
	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Shutdown()

	for i := 0; i < 1000; i++ {
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
