package tracing

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	opentracing "github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

// UnaryClientInterceptor returns a new unary client interceptor for OpenTracing.
func UnaryClientInterceptor(tracer opentracing.Tracer) grpc.UnaryClientInterceptor {
	return grpc_opentracing.UnaryClientInterceptor(grpc_opentracing.WithTracer(tracer))
}

// StreamClientInterceptor returns a new streaming client interceptor for OpenTracing.
func StreamClientInterceptor(tracer opentracing.Tracer) grpc.StreamClientInterceptor {
	return grpc_opentracing.StreamClientInterceptor(grpc_opentracing.WithTracer(tracer))
}

// UnaryServerInterceptor returns a new unary server interceptor for OpenTracing and injects given tracer.
func UnaryServerInterceptor(tracer opentracing.Tracer) grpc.UnaryServerInterceptor {
	return func(parentCtx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return grpc_opentracing.UnaryServerInterceptor(grpc_opentracing.WithTracer(tracer))(ContextWithTracer(parentCtx, tracer), req, info, handler)
	}
}

// StreamServerInterceptor returns a new streaming server interceptor for OpenTracing and injects given tracer.
func StreamServerInterceptor(tracer opentracing.Tracer) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrappedStream := grpc_middleware.WrapServerStream(stream)
		wrappedStream.WrappedContext = ContextWithTracer(stream.Context(), tracer)

		return grpc_opentracing.StreamServerInterceptor(grpc_opentracing.WithTracer(tracer))(srv, wrappedStream, info, handler)
	}
}
