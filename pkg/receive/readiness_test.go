// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/prober"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type mockReadinessChecker struct {
	ready bool
}

func (m *mockReadinessChecker) IsReady() bool {
	return m.ready
}

func (m *mockReadinessChecker) SetReady(ready bool) {
	m.ready = ready
}

func TestNewReadinessGRPCOptions(t *testing.T) {
	readyChecker := &mockReadinessChecker{ready: true}
	options := NewReadinessGRPCOptions(readyChecker)
	testutil.Equals(t, 2, len(options))
}

func TestReadinessInterceptors(t *testing.T) {
	checker := &mockReadinessChecker{ready: false}

	// Test the actual NewReadinessGRPCOptions function
	readinessOptions := NewReadinessGRPCOptions(checker)
	testutil.Equals(t, 2, len(readinessOptions)) // Should have unary and stream interceptors

	// Create grpcserver with actual readiness options (tests both unary and stream interceptors)
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	comp := component.Receive
	grpcProbe := prober.NewGRPC()

	// Find a free port for testing
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	testutil.Ok(t, err)
	addr := listener.Addr().String()
	listener.Close()

	mockSrv := &mockWriteableStoreServer{}
	var grpcOptions []grpcserver.Option
	grpcOptions = append(grpcOptions, grpcserver.WithListen(addr))
	grpcOptions = append(grpcOptions, readinessOptions...) // Use actual readiness options (both unary and stream)
	grpcOptions = append(grpcOptions, grpcserver.WithServer(func(s *grpc.Server) {
		storepb.RegisterWriteableStoreServer(s, mockSrv)
	}))

	srv := grpcserver.New(logger, reg, opentracing.NoopTracer{}, nil, nil, comp, grpcProbe, grpcOptions...)

	// Start server
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			t.Errorf("Server failed: %v", err)
		}
	}()
	defer srv.Shutdown(nil)

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	testutil.Ok(t, err)
	defer conn.Close()

	client := storepb.NewWriteableStoreClient(conn)

	// Test when not ready - this tests the unary interceptor (RemoteWrite is unary)
	// Stream interceptors are also applied but RemoteWrite doesn't use streaming
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := client.RemoteWrite(ctx, &storepb.WriteRequest{})
	testutil.Assert(t, err != nil)
	testutil.Equals(t, codes.Unavailable, status.Code(err))
	testutil.Assert(t, resp == nil)
	testutil.Equals(t, 0, mockSrv.callCount)

	// Test when ready
	checker.SetReady(true)

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()

	resp2, err2 := client.RemoteWrite(ctx2, &storepb.WriteRequest{})
	testutil.Ok(t, err2)
	testutil.Assert(t, resp2 != nil)
	testutil.Equals(t, 1, mockSrv.callCount)

	// Test not ready again
	checker.SetReady(false)

	ctx3, cancel3 := context.WithTimeout(context.Background(), time.Second)
	defer cancel3()

	resp3, err3 := client.RemoteWrite(ctx3, &storepb.WriteRequest{})
	testutil.Assert(t, err3 != nil)
	testutil.Equals(t, codes.Unavailable, status.Code(err3))
	testutil.Assert(t, resp3 == nil)
	testutil.Equals(t, 1, mockSrv.callCount) // Should not increment
}
