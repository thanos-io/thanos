// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tracing

import (
	"context"

	"github.com/gogo/protobuf/proto"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

const (
	// wireBytesTagKey is the trace span tag key for wire bytes.
	wireBytesTagKey = "result.wire_bytes"
)

// UnaryServerWireBytesInterceptor returns a new unary server interceptor that records
// the wire bytes sent in the response as a trace span attribute.
func UnaryServerWireBytesInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)

		// Record wire bytes in the span if there's an active span.
		if span := opentracing.SpanFromContext(ctx); span != nil && resp != nil {
			if msg, ok := resp.(proto.Message); ok {
				wireBytes := proto.Size(msg)
				span.SetTag(wireBytesTagKey, wireBytes)
			}
		}

		return resp, err
	}
}

// wireBytesServerStream wraps grpc.ServerStream to track wire bytes sent.
type wireBytesServerStream struct {
	grpc.ServerStream
	wireBytes int
	span      opentracing.Span
}

// SendMsg wraps the ServerStream.SendMsg to track wire bytes.
func (w *wireBytesServerStream) SendMsg(m any) error {
	if msg, ok := m.(proto.Message); ok {
		w.wireBytes += proto.Size(msg)
	}
	return w.ServerStream.SendMsg(m)
}

// StreamServerWireBytesInterceptor returns a new streaming server interceptor that records
// the wire bytes sent in streaming responses as a trace span attribute.
func StreamServerWireBytesInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		span := opentracing.SpanFromContext(stream.Context())

		wrappedStream := &wireBytesServerStream{
			ServerStream: grpc_middleware.WrapServerStream(stream),
			wireBytes:    0,
			span:         span,
		}

		err := handler(srv, wrappedStream)

		// Record wire bytes in the span after the stream completes.
		if span != nil {
			span.SetTag(wireBytesTagKey, wrappedStream.wireBytes)
		}

		return err
	}
}
