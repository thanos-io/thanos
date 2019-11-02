package logging

import (
	"context"
	"github.com/go-kit/kit/log"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/kit"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/kit/ctxkit"
	"google.golang.org/grpc/metadata"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	opentracing "github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

// UnaryClientInterceptor returns a new unary client interceptor for injecting request ID and logging.
func UnaryClientInterceptor(logger log.Logger) grpc.UnaryClientInterceptor {
	interceptor := kit.UnaryClientInterceptor(logger)
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if reqID, ok := GetRequestID(ctx); ok  {
			metadata.AppendToOutgoingContext(ctx, header, reqID)
		}
		return interceptor(ctx, method, req, reply, cc, invoker, opts...)
	}
}

// StreamClientInterceptor returns a new streaming client interceptor for injecting request ID and logging.
func StreamClientInterceptor(logger log.Logger) grpc.StreamClientInterceptor {
	interceptor := kit.StreamClientInterceptor(logger)
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (stream grpc.ClientStream, e error) {
		if reqID, ok := GetRequestID(ctx); ok  {
			ctx = metadata.AppendToOutgoingContext(ctx, header, reqID)
		}
		return interceptor(ctx, desc, cc, method, streamer, opts...)
	}
}

// UnaryServerInterceptor returns a new unary server interceptor for extracting request ID and logging.
func UnaryServerInterceptor(logger log.Logger, getTraceIDFn func(context.Context) (string, bool)) grpc.UnaryServerInterceptor {
	interceptor := kit.UnaryServerInterceptor(logger)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if traceID, ok := getTraceIDFn(ctx); ok {
			ctxkit.AddFields(ctx, "traceID", traceID)
		}

		if md, ok := metadata.FromIncomingContext(ctx); ok {
			requestIDList := md.Get(header)
			if len(requestIDList) > 1 {
				ctxkit.AddFields(ctx, "requestID", requestIDList[0])
			}
		}
		// TODO(bwplotka): Make sure gRPC kit allows taking generic fields from meta (indirection!)
		return interceptor(ctx, req, info, handler)
	}
}

// StreamServerInterceptor returns a new streaming server interceptor for extracting request ID and logging.
func StreamServerInterceptor(getTraceIDFn func(context.Context) (string, bool)) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrappedStream := grpc_middleware.WrapServerStream(stream)
		wrappedStream.WrappedContext = ContextWithTracer(stream.Context(), tracer)

		return grpc_opentracing.StreamServerInterceptor(grpc_opentracing.WithTracer(tracer))(srv, wrappedStream, info, handler)
	}
}
