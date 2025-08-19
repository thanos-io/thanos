// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/prober"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
)

// ReadinessChecker is an interface for checking if the service is ready.
type ReadinessChecker interface {
	IsReady() bool
}

// NewReadinessGRPCOptions creates gRPC server options that add readiness interceptors.
// When the service is not ready, interceptors return empty responses to avoid timeouts
// during pod startup when using publishNotReadyAddresses: true.
func NewReadinessGRPCOptions(probe ReadinessChecker) []grpcserver.Option {
	unaryInterceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		if !probe.IsReady() {
			// Return empty response instead of processing the request.
			// This prevents timeouts while pods are starting up when using publishNotReadyAddresses: true.
			return nil, status.Errorf(codes.Unavailable, "service is not ready yet")
		}
		return handler(ctx, req)
	}

	streamInterceptor := func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !probe.IsReady() {
			// Return immediately instead of processing the request.
			// This prevents timeouts while pods are starting up when using publishNotReadyAddresses: true.
			return nil
		}
		return handler(srv, ss)
	}

	return []grpcserver.Option{
		grpcserver.WithGRPCServerOption(grpc.UnaryInterceptor(unaryInterceptor)),
		grpcserver.WithGRPCServerOption(grpc.StreamInterceptor(streamInterceptor)),
	}
}

// Ensure that HTTPProbe implements ReadinessChecker.
var _ ReadinessChecker = (*prober.HTTPProbe)(nil)
