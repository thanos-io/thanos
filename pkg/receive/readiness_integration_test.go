// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/prober"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// mockWriteableStoreServer implements a simple WriteableStore service for testing.
type mockWriteableStoreServer struct {
	storepb.UnimplementedWriteableStoreServer
	callCount int
}

func (m *mockWriteableStoreServer) RemoteWrite(_ context.Context, _ *storepb.WriteRequest) (*storepb.WriteResponse, error) {
	m.callCount++
	return &storepb.WriteResponse{}, nil
}

// TestReadinessFeatureIntegration tests the full integration of the readiness feature
// including feature flag parsing and gRPC server setup.
func TestReadinessFeatureIntegration(t *testing.T) {
	t.Run("NewReadinessGRPCOptions creates correct options", func(t *testing.T) {
		probe := prober.NewHTTP()
		options := NewReadinessGRPCOptions(probe)
		testutil.Equals(t, 2, len(options)) // Should have unary and stream interceptor options
	})

	t.Run("grpc server with readiness - full behavior test", func(t *testing.T) {
		testReadinessWithGRPCServer(t, true)
	})

	t.Run("grpc server without readiness", func(t *testing.T) {
		testReadinessWithGRPCServer(t, false)
	})
}

func testReadinessWithGRPCServer(t *testing.T, enableReadiness bool) {
	httpProbe := prober.NewHTTP()
	testutil.Equals(t, false, httpProbe.IsReady())

	mockSrv := &mockWriteableStoreServer{}

	// Test the actual NewReadinessGRPCOptions function
	if enableReadiness {
		readinessOptions := NewReadinessGRPCOptions(httpProbe)
		testutil.Equals(t, 2, len(readinessOptions)) // Should have unary and stream interceptors
	}

	// Create grpcserver with actual production setup
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	tracer := opentracing.NoopTracer{}
	comp := component.Receive
	grpcProbe := prober.NewGRPC()

	// Find a free port for testing
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	testutil.Ok(t, err)
	addr := listener.Addr().String()
	listener.Close()

	var grpcOptions []grpcserver.Option
	grpcOptions = append(grpcOptions, grpcserver.WithListen(addr))
	grpcOptions = append(grpcOptions, grpcserver.WithServer(func(s *grpc.Server) {
		storepb.RegisterWriteableStoreServer(s, mockSrv)
	}))

	if enableReadiness {
		grpcOptions = append(grpcOptions, NewReadinessGRPCOptions(httpProbe)...)
	}

	srv := grpcserver.New(logger, reg, tracer, nil, nil, comp, grpcProbe, grpcOptions...)

	// Start server in background
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			t.Errorf("Server failed: %v", err)
		}
	}()
	defer srv.Shutdown(nil)

	// Wait for server to start
	time.Sleep(200 * time.Millisecond)

	// Create client connection
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	testutil.Ok(t, err)
	defer conn.Close()

	client := storepb.NewWriteableStoreClient(conn)

	// Test 1: RemoteWrite when NOT ready
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
	defer cancel1()

	resp1, err1 := client.RemoteWrite(ctx1, &storepb.WriteRequest{})

	if enableReadiness {
		// When readiness is enabled and probe is not ready, interceptor returns Unavailable error
		testutil.Assert(t, err1 != nil)
		testutil.Equals(t, codes.Unavailable, status.Code(err1))
		testutil.Assert(t, resp1 == nil)
		testutil.Equals(t, 0, mockSrv.callCount) // Service not called due to readiness interceptor
	} else {
		// When readiness is disabled, service should be called normally
		testutil.Ok(t, err1)
		testutil.Assert(t, resp1 != nil)
		testutil.Equals(t, 1, mockSrv.callCount)
	}

	// Make httpProbe ready
	httpProbe.Ready()
	testutil.Equals(t, true, httpProbe.IsReady())

	// Test 2: RemoteWrite when ready
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()

	resp2, err2 := client.RemoteWrite(ctx2, &storepb.WriteRequest{})
	testutil.Ok(t, err2)
	testutil.Assert(t, resp2 != nil)

	if enableReadiness {
		// Now that probe is ready, service should be called
		testutil.Equals(t, 1, mockSrv.callCount)
	} else {
		// Service called again (second time)
		testutil.Equals(t, 2, mockSrv.callCount)
	}

	// Test 3: Make probe not ready again
	httpProbe.NotReady(fmt.Errorf("test error"))
	testutil.Equals(t, false, httpProbe.IsReady())

	ctx3, cancel3 := context.WithTimeout(context.Background(), time.Second)
	defer cancel3()

	resp3, err3 := client.RemoteWrite(ctx3, &storepb.WriteRequest{})

	if enableReadiness {
		// Back to not ready - should return Unavailable error and not call service
		testutil.Assert(t, err3 != nil)
		testutil.Equals(t, codes.Unavailable, status.Code(err3))
		testutil.Assert(t, resp3 == nil)
		testutil.Equals(t, 1, mockSrv.callCount) // Count should not increase
	} else {
		// Service called again (third time)
		testutil.Ok(t, err3)
		testutil.Assert(t, resp3 != nil)
		testutil.Equals(t, 3, mockSrv.callCount)
	}
}
