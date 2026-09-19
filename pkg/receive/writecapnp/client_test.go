// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

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
