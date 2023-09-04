// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

/*
This was copied over from https://github.com/grpc-ecosystem/go-grpc-middleware/tree/v2.0.0-rc.3
and modified to support tracing in Thanos till migration to Otel is supported.
*/

/*
Package `grpc_testing` provides helper functions for testing validators in this package.
*/

package grpctesting

import (
	"context"
	"io"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/tracing/tracing_middleware/grpctesting/testpb"
)

const (
	// DefaultPongValue is the default value used.
	DefaultResponseValue = "default_response_value"
	// ListResponseCount is the expected number of responses to PingList.
	ListResponseCount = 100
)

type TestPingService struct {
	T *testing.T
}

func (s *TestPingService) PingEmpty(_ context.Context, _ *testpb.Empty) (*testpb.PingResponse, error) {
	return &testpb.PingResponse{Value: DefaultResponseValue, Counter: 0}, nil
}

func (s *TestPingService) Ping(_ context.Context, ping *testpb.PingRequest) (*testpb.PingResponse, error) {
	// Send user trailers and headers.
	return &testpb.PingResponse{Value: ping.Value, Counter: 0}, nil
}

func (s *TestPingService) PingError(_ context.Context, ping *testpb.PingRequest) (*testpb.Empty, error) {
	code := codes.Code(ping.ErrorCodeReturned)
	return nil, status.Errorf(code, "Userspace error.")
}

func (s *TestPingService) PingList(ping *testpb.PingRequest, stream testpb.TestService_PingListServer) error {
	if ping.ErrorCodeReturned != 0 {
		return status.Errorf(codes.Code(ping.ErrorCodeReturned), "foobar")
	}
	// Send user trailers and headers.
	for i := 0; i < ListResponseCount; i++ {
		if err := stream.Send(&testpb.PingResponse{Value: ping.Value, Counter: int32(i)}); err != nil {
			return err
		}
	}
	return nil
}

func (s *TestPingService) PingStream(stream testpb.TestService_PingStreamServer) error {
	count := 0
	for {
		ping, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&testpb.PingResponse{Value: ping.Value, Counter: int32(count)}); err != nil {
			return err
		}

		count += 1
	}
	return nil
}
