// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tracing

import (
	"context"

	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/providers/opentracing/v2"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_tracing "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tracing"

	// opentracing "github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

// UnaryClientInterceptor returns a new unary client interceptor for OpenTracing.
func UnaryClientInterceptor(opts ...grpc_opentracing.Option) grpc.UnaryClientInterceptor {
	return grpc_tracing.UnaryClientInterceptor(grpc_opentracing.InterceptorTracer(opts...))
}

// StreamClientInterceptor returns a new streaming client interceptor for OpenTracing.
func StreamClientInterceptor(opts ...grpc_opentracing.Option) grpc.StreamClientInterceptor {
	return grpc_tracing.StreamClientInterceptor(grpc_opentracing.InterceptorTracer(opts...))
}

// UnaryServerInterceptor returns a new unary server interceptor for OpenTracing and injects given tracer.
func UnaryServerInterceptor(opts ...grpc_opentracing.Option) grpc.UnaryServerInterceptor {
	interceptor := grpc_tracing.UnaryServerInterceptor(grpc_opentracing.InterceptorTracer(opts...))

	return func(parentCtx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Add our own tracer.
		return interceptor(NewContextWithTracer(parentCtx, opts), req, info, handler)
	}
}

// StreamServerInterceptor returns a new streaming server interceptor for OpenTracing and injects given tracer.
func StreamServerInterceptor(opts ...grpc_opentracing.Option) grpc.StreamServerInterceptor {
	interceptor := grpc_tracing.StreamServerInterceptor(grpc_opentracing.InterceptorTracer(opts...))
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Add our own tracer.
		wrappedStream := grpc_middleware.WrapServerStream(stream)
		wrappedStream.WrappedContext = NewContextWithTracer(stream.Context(), opts)

		return interceptor(srv, wrappedStream, info, handler)
	}
}
