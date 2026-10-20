// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extgrpc

import (
	"context"
	"reflect"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

// RetainingServerOptions returns options for a server that decodes requests of msgs'
// types from pooled gRPC buffers kept until the handler returns. Only use them when
// nothing references such a request after its handler returns.
func RetainingServerOptions(msgs ...any) []grpc.ServerOption {
	return newRetainingCodec(msgs...).serverOptions()
}

// pooledMessage is a retained message that also decodes into pooled slices.
type pooledMessage interface {
	UnmarshalPooled([]byte) error
	ReleasePooled()
}

// retainingCodec is the proto codec of a single server, releasing retained buffers
// through its own interceptor.
type retainingCodec struct {
	encoding.CodecV2

	types   map[reflect.Type]struct{}
	pool    mem.BufferPool
	buffers sync.Map // message pointer -> mem.Buffer
}

func newRetainingCodec(msgs ...any) *retainingCodec {
	c := &retainingCodec{
		CodecV2: encoding.GetCodecV2("proto"),
		types:   make(map[reflect.Type]struct{}, len(msgs)),
		pool:    mem.DefaultBufferPool(),
	}
	for _, m := range msgs {
		c.types[reflect.TypeOf(m)] = struct{}{}
	}
	return c
}

func (c *retainingCodec) serverOptions() []grpc.ServerOption {
	// grpcserver.New applies these before its own interceptors, so the release runs last.
	return []grpc.ServerOption{
		grpc.ForceServerCodecV2(c),
		grpc.ChainUnaryInterceptor(c.releaseInterceptor),
	}
}

func (c *retainingCodec) Unmarshal(data mem.BufferSlice, v any) error {
	gmsg, ok := v.(gogoMsg)
	if _, retained := c.types[reflect.TypeOf(v)]; !ok || !retained {
		return c.CodecV2.Unmarshal(data, v)
	}

	buf := data.MaterializeToBuffer(c.pool)
	if pm, ok := v.(pooledMessage); ok {
		if err := pm.UnmarshalPooled(buf.ReadOnlyData()); err != nil {
			pm.ReleasePooled()
			buf.Free()
			return err
		}
	} else if err := gmsg.Unmarshal(buf.ReadOnlyData()); err != nil {
		buf.Free()
		return err
	}
	c.buffers.Store(v, buf)
	return nil
}

// release returns msg's buffer to the pool. msg must not be used afterwards.
func (c *retainingCodec) release(msg any) {
	if b, ok := c.buffers.LoadAndDelete(msg); ok {
		if pm, ok := msg.(pooledMessage); ok {
			pm.ReleasePooled()
		}
		b.(mem.Buffer).Free()
	}
}

func (c *retainingCodec) releaseInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	defer c.release(req)
	return handler(ctx, req)
}
