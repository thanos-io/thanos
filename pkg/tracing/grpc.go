package tracing

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	opentracing "github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

// UnaryClientInterceptor returns a new unary client interceptor for OpenTracing.
func UnaryClientInterceptor(tracer opentracing.Tracer, filterFunc grpc_opentracing.FilterFunc) grpc.UnaryClientInterceptor {
	return grpc_opentracing.UnaryClientInterceptor(grpc_opentracing.WithTracer(tracer), grpc_opentracing.WithFilterFunc(filterFunc))
}

// StreamClientInterceptor returns a new streaming client interceptor for OpenTracing.
func StreamClientInterceptor(tracer opentracing.Tracer, filterFunc grpc_opentracing.FilterFunc) grpc.StreamClientInterceptor {
	return grpc_opentracing.StreamClientInterceptor(grpc_opentracing.WithTracer(tracer), grpc_opentracing.WithFilterFunc(filterFunc))
}

// UnaryServerInterceptor returns a new unary server interceptor for OpenTracing and injects given tracer.
func UnaryServerInterceptor(tracer opentracing.Tracer, filterFunc grpc_opentracing.FilterFunc) grpc.UnaryServerInterceptor {
	if filterFunc == nil {
		filterFunc = func(ctx context.Context, fullMethodName string) bool { return true }
	}
	interceptor := grpc_opentracing.UnaryServerInterceptor(grpc_opentracing.WithTracer(tracer), grpc_opentracing.WithFilterFunc(filterFunc))
	return func(parentCtx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Add out tracer to context.
		return interceptor(ContextWithTracer(parentCtx, tracer), req, info, handler)
	}
}

// StreamServerInterceptor returns a new streaming server interceptor for OpenTracing and injects given tracer.
func StreamServerInterceptor(tracer opentracing.Tracer, filterFunc grpc_opentracing.FilterFunc) grpc.StreamServerInterceptor {
	if filterFunc == nil {
		filterFunc = func(ctx context.Context, fullMethodName string) bool { return true }
	}
	interceptor := grpc_opentracing.StreamServerInterceptor(grpc_opentracing.WithTracer(tracer), grpc_opentracing.WithFilterFunc(filterFunc))
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrappedStream := grpc_middleware.WrapServerStream(stream)
		// Add out tracer to context.
		wrappedStream.WrappedContext = ContextWithTracer(stream.Context(), tracer)
		return interceptor(srv, wrappedStream, info, handler)
	}
}
