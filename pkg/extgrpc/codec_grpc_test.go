// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extgrpc

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extgrpc/snappy"
	"github.com/thanos-io/thanos/pkg/prober"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// checkingWriter fails a request that was not retained by codec or whose series carry
// labels of another request.
type checkingWriter struct {
	codec *retainingCodec
}

func (w checkingWriter) RemoteWrite(_ context.Context, r *storepb.WriteRequest) (*storepb.WriteResponse, error) {
	if _, ok := w.codec.buffers.Load(r); !ok {
		return nil, status.Error(codes.FailedPrecondition, "request was not retained")
	}
	// Let concurrent requests reuse the pool while this one is in use.
	time.Sleep(time.Millisecond)
	for i, ts := range r.Timeseries {
		if len(ts.Labels) != 3 || ts.Labels[1].Value != r.Tenant || ts.Labels[2].Value != strconv.Itoa(i) {
			return nil, status.Errorf(codes.DataLoss, "series %d of %s has labels %v", i, r.Tenant, ts.Labels)
		}
	}
	return &storepb.WriteResponse{}, nil
}

func writeRequestFor(tenant string, series int) *storepb.WriteRequest {
	pad := strings.Repeat("x", 64)
	req := &storepb.WriteRequest{Tenant: tenant}
	for i := range series {
		req.Timeseries = append(req.Timeseries, prompb.TimeSeries{
			Labels: []labelpb.ZLabel{
				{Name: "__name__", Value: "metric_" + pad},
				{Name: "request", Value: tenant},
				{Name: "series", Value: strconv.Itoa(i)},
			},
			Samples: []prompb.Sample{{Value: 1, Timestamp: int64(i)}},
		})
	}
	return req
}

// The server is built with grpcserver.New, as in receive, so all its interceptors run.
func TestRetainedBuffersOverGRPC(t *testing.T) {
	codec := newRetainingCodec(&storepb.WriteRequest{})
	srvAddr := freeAddr(t)
	opts := []grpcserver.Option{
		grpcserver.WithServer(func(s *grpc.Server) { storepb.RegisterWriteableStoreServer(s, checkingWriter{codec: codec}) }),
		grpcserver.WithListen(srvAddr),
	}
	for _, o := range codec.serverOptions() {
		opts = append(opts, grpcserver.WithGRPCServerOption(o))
	}
	srv := grpcserver.New(log.NewNopLogger(), prometheus.NewRegistry(), opentracing.NoopTracer{}, nil, nil, component.Receive, prober.NewGRPC(), opts...)
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() { srv.Shutdown(nil) })

	conn, err := grpc.NewClient(srvAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(snappy.Name), grpc.WaitForReady(true)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := storepb.NewWriteableStoreClient(conn)

	g, ctx := errgroup.WithContext(context.Background())
	for w := range 16 {
		g.Go(func() error {
			for r := range 30 {
				tenant := fmt.Sprintf("w%d-r%d", w, r)
				if _, err := client.RemoteWrite(ctx, writeRequestFor(tenant, 100+r)); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())
	require.Equal(t, 0, retainedBuffersCount(codec))
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().String()
}
