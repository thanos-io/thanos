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

type nopWriter struct{}

func (nopWriter) Write(context.Context, Writer_write) error { return nil }

// RemoteWriteClient relies on separate references building params in parallel.
func TestWriterReferencesBuildInParallel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srvConn, cliConn := net.Pipe()
	srv := rpc.NewConn(rpc.NewStreamTransport(srvConn), &rpc.Options{
		BootstrapClient: capnp.Client(Writer_ServerToClient(nopWriter{})),
	})
	defer srv.Close()
	cli := rpc.NewConn(rpc.NewStreamTransport(cliConn), nil)
	defer cli.Close()

	w := Writer(cli.Bootstrap(ctx))
	defer w.Release()

	write := func(build func()) error {
		ref := w.AddRef()
		defer ref.Release()
		res, release := ref.Write(ctx, func(Writer_write_Params) error { build(); return nil })
		defer release()
		_, err := res.Struct()
		return err
	}

	firstBuilding, secondBuilt := make(chan struct{}), make(chan struct{})
	errs := make(chan error, 1)
	go func() {
		errs <- write(func() {
			close(firstBuilding)
			select {
			case <-secondBuilt:
			case <-ctx.Done():
			}
		})
	}()
	select {
	case <-firstBuilding:
	case err := <-errs:
		t.Fatal(err)
	}
	require.NoError(t, write(func() { close(secondBuilt) }))
	require.NoError(t, <-errs)
}
