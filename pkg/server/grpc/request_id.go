// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package grpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/thanos-io/thanos/pkg/server/http/middleware"
)

const requestIDKey = "request-id"

func NewUnaryClientRequestIDInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		reqID, ok := middleware.RequestIDFromContext(ctx)
		if ok {
			ctx = metadata.AppendToOutgoingContext(ctx, requestIDKey, reqID)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func NewUnaryServerRequestIDInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		if vals := metadata.ValueFromIncomingContext(ctx, requestIDKey); len(vals) == 1 {
			ctx = middleware.NewContextWithRequestID(ctx, vals[0])
		}
		return handler(ctx, req)
	}
}

func NewStreamClientRequestIDInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		reqID, ok := middleware.RequestIDFromContext(ctx)
		if ok {
			ctx = metadata.AppendToOutgoingContext(ctx, requestIDKey, reqID)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func NewStreamServerRequestIDInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if vals := metadata.ValueFromIncomingContext(ss.Context(), requestIDKey); len(vals) == 1 {
			ctx := middleware.NewContextWithRequestID(ss.Context(), vals[0])
			return handler(srv, newStreamWithContext(ctx, ss))
		}
		return handler(srv, ss)
	}
}

type streamWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func newStreamWithContext(ctx context.Context, serverStream grpc.ServerStream) *streamWithContext {
	return &streamWithContext{ServerStream: serverStream, ctx: ctx}
}

func (s streamWithContext) Context() context.Context {
	return s.ctx
}
