// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tracing

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/thanos-io/thanos/pkg/tracing/tracing_middleware/grpctesting"
	"github.com/thanos-io/thanos/pkg/tracing/tracing_middleware/grpctesting/testpb"
)

// TestUnaryServerWireBytesInterceptor tests that the wire bytes interceptor
// correctly records the size of unary responses in the trace span.
func TestUnaryServerWireBytesInterceptor(t *testing.T) {
	mockTracer := mocktracer.New()

	// Create a test server with both tracing and wire bytes interceptors.
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			UnaryServerInterceptor(mockTracer),
			UnaryServerWireBytesInterceptor(),
		),
	)

	testpb.RegisterTestServiceServer(server, &grpctesting.TestPingService{T: t})

	// Start server and client.
	lis, serverAddr := startServer(t, server)
	defer lis.Close()
	defer server.Stop()

	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(UnaryClientInterceptor(mockTracer)),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := testpb.NewTestServiceClient(conn)

	// Make a call with a tracer in the context.
	ctx := ContextWithTracer(context.Background(), mockTracer)
	req := &testpb.PingRequest{Value: "test"}
	resp, err := client.Ping(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify that the wire bytes were recorded in the span.
	spans := mockTracer.FinishedSpans()
	require.NotEmpty(t, spans, "should have recorded spans")

	// Find the server span.
	var serverSpan *mocktracer.MockSpan
	for _, span := range spans {
		if span.Tag("span.kind") == ext.SpanKindRPCServerEnum {
			serverSpan = span
			break
		}
	}
	require.NotNil(t, serverSpan, "should have a server span")

	// Verify the wire bytes tag exists and is non-zero.
	wireBytes := serverSpan.Tag(wireBytesTagKey)
	require.NotNil(t, wireBytes, "wire bytes tag should be present")

	wireBytesInt, ok := wireBytes.(int)
	require.True(t, ok, "wire bytes should be an int")

	// The expected size should be the proto size of the response.
	expectedSize := proto.Size(resp)
	assert.Equal(t, expectedSize, wireBytesInt, "wire bytes should match proto size")
	assert.Greater(t, wireBytesInt, 0, "wire bytes should be greater than 0")
}

// TestStreamServerWireBytesInterceptor tests that the wire bytes interceptor
// correctly records the cumulative size of streaming responses in the trace span.
func TestStreamServerWireBytesInterceptor(t *testing.T) {
	mockTracer := mocktracer.New()

	// Create a test server with both tracing and wire bytes interceptors.
	server := grpc.NewServer(
		grpc.ChainStreamInterceptor(
			StreamServerInterceptor(mockTracer),
			StreamServerWireBytesInterceptor(),
		),
	)

	testpb.RegisterTestServiceServer(server, &grpctesting.TestPingService{T: t})

	// Start server and client.
	lis, serverAddr := startServer(t, server)
	defer lis.Close()
	defer server.Stop()

	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(StreamClientInterceptor(mockTracer)),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := testpb.NewTestServiceClient(conn)

	// Make a streaming call with a tracer in the context.
	ctx := ContextWithTracer(context.Background(), mockTracer)
	req := &testpb.PingRequest{Value: "test"}
	stream, err := client.PingList(ctx, req)
	require.NoError(t, err)

	// Receive all messages and track expected size.
	expectedTotalBytes := 0
	messageCount := 0
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		expectedTotalBytes += proto.Size(resp)
		messageCount++
	}

	require.Greater(t, messageCount, 0, "should have received at least one message")

	// Verify that the wire bytes were recorded in the span.
	spans := mockTracer.FinishedSpans()
	require.NotEmpty(t, spans, "should have recorded spans")

	// Find the server span.
	var serverSpan *mocktracer.MockSpan
	for _, span := range spans {
		if span.Tag("span.kind") == ext.SpanKindRPCServerEnum {
			serverSpan = span
			break
		}
	}
	require.NotNil(t, serverSpan, "should have a server span")

	// Verify the wire bytes tag exists and matches the cumulative size.
	wireBytes := serverSpan.Tag(wireBytesTagKey)
	require.NotNil(t, wireBytes, "wire bytes tag should be present")

	wireBytesInt, ok := wireBytes.(int)
	require.True(t, ok, "wire bytes should be an int")

	assert.Equal(t, expectedTotalBytes, wireBytesInt, "wire bytes should match cumulative proto size")
	assert.Greater(t, wireBytesInt, 0, "wire bytes should be greater than 0")
}

// startServer is a helper function to start a gRPC server for testing.
func startServer(t *testing.T, server *grpc.Server) (io.Closer, string) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = server.Serve(lis)
	}()

	return lis, lis.Addr().String()
}
