// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type errorDialer struct{}

func (d *errorDialer) DialContext(ctx context.Context) (net.Conn, error) {
	return nil, fmt.Errorf("dial failed")
}

type closedPipeDialer struct{}

func (d *closedPipeDialer) DialContext(ctx context.Context) (net.Conn, error) {
	client, server := net.Pipe()
	_ = server.Close()
	return client, nil
}

func TestRemoteWriteTransportSetupError(t *testing.T) {
	t.Parallel()

	client := NewRemoteWriteClient(&closedPipeDialer{}, log.NewNopLogger(), WithCompression(CompressionZstd))
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{})
	require.ErrorContains(t, err, "set up capnp transport")
	require.ErrorContains(t, err, "write compression preface")

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code())
}

// pipeDialer serves every dialed connection with serve on the other end of a pipe.
type pipeDialer func(net.Conn)

func (d pipeDialer) DialContext(context.Context) (net.Conn, error) {
	client, server := net.Pipe()
	go d(server)
	return client, nil
}

// blockingWriter answers writes only once the call is canceled.
type blockingWriter struct{}

func (blockingWriter) Write(ctx context.Context, _ Writer_write) error {
	<-ctx.Done()
	return ctx.Err()
}

// legacyServer serves conn like a receiver from before compression: packed only.
func legacyServer(conn net.Conn) {
	c := rpc.NewConn(rpc.NewPackedStreamTransport(conn), &rpc.Options{BootstrapClient: capnp.Client(Writer_ServerToClient(blockingWriter{}))})
	<-c.Done()
}

// blockingServer accepts any compression and never finishes a write.
func blockingServer(conn net.Conn) {
	transport, err := NewServerTransport(conn)
	if err != nil {
		_ = conn.Close()
		return
	}
	c := rpc.NewConn(transport, &rpc.Options{BootstrapClient: capnp.Client(Writer_ServerToClient(blockingWriter{}))})
	<-c.Done()
}

func TestRemoteWriteCompressionHint(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		dialer      Dialer
		compression Compression
		timeout     time.Duration
		wantHint    bool
	}{
		{name: "zstd to legacy peer", dialer: pipeDialer(legacyServer), compression: CompressionZstd, timeout: 10 * time.Second, wantHint: true},
		{name: "dial failure", dialer: &errorDialer{}, compression: CompressionZstd, timeout: 10 * time.Second},
		{name: "zstd write timeout after bootstrap", dialer: pipeDialer(blockingServer), compression: CompressionZstd, timeout: 200 * time.Millisecond},
		{name: "none write timeout", dialer: pipeDialer(blockingServer), compression: CompressionNone, timeout: 200 * time.Millisecond},
		{name: "empty compression", dialer: &errorDialer{}, compression: "", timeout: 10 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := NewRemoteWriteClient(tc.dialer, log.NewNopLogger(), WithCompression(tc.compression))
			defer func() { require.NoError(t, client.Close()) }()

			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			_, err := client.RemoteWrite(ctx, &storepb.WriteRequest{Tenant: "default"})
			require.Error(t, err)
			if tc.wantHint {
				require.ErrorContains(t, err, "bootstrap capnp writer (compression=zstd")
				return
			}
			require.NotContains(t, err.Error(), "compression=")
		})
	}
}

func TestRemoteWriteErrorCodes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		ctx      func() (context.Context, context.CancelFunc)
		wantCode codes.Code
	}{
		{
			name: "deadline exceeded",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			wantCode: codes.DeadlineExceeded,
		},
		{
			name: "canceled",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			wantCode: codes.Canceled,
		},
		{
			name: "unavailable",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.Background(), func() {}
			},
			wantCode: codes.Unavailable,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := NewRemoteWriteClient(&errorDialer{}, log.NewNopLogger())
			ctx, cancel := tc.ctx()
			defer cancel()

			_, err := client.RemoteWrite(ctx, &storepb.WriteRequest{})
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, tc.wantCode, st.Code())
		})
	}
}
