// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PooledWriteRequest wraps a WriteRequest with pooled unmarshaling.
type PooledWriteRequest struct {
	*WriteRequest
	wru *WriteRequestUnmarshaler
}

func (p *PooledWriteRequest) release() {
	if p.wru != nil {
		PutWriteRequestUnmarshaler(p.wru)
		p.wru = nil
		p.WriteRequest = nil
	}
}

// WriteableStoreServerPooled extends WriteableStoreServer with zero-allocation unmarshaling.
// The WriteRequest is only valid for the duration of RemoteWritePooled; use
// labelpb.ReAllocZLabelsStrings to copy labels that need to persist.
type WriteableStoreServerPooled interface {
	WriteableStoreServer
	RemoteWritePooled(ctx context.Context, req *PooledWriteRequest) (*WriteResponse, error)
}

// RegisterWriteableStoreServerPooled registers a WriteableStoreServerPooled with pooled unmarshaling.
func RegisterWriteableStoreServerPooled(s *grpc.Server, srv WriteableStoreServerPooled) {
	s.RegisterService(&writeableStorePooledServiceDesc, srv)
}

type rawBytesReceiver struct {
	data []byte
}

func (r *rawBytesReceiver) Reset()                                   {}
func (r *rawBytesReceiver) ProtoMessage()                            {}
func (r *rawBytesReceiver) String() string                           { return "" }
func (r *rawBytesReceiver) Unmarshal(data []byte) error              { r.data = data; return nil }
func (r *rawBytesReceiver) Size() int                                { return 0 }
func (r *rawBytesReceiver) MarshalToSizedBuffer([]byte) (int, error) { return 0, nil }

func writeableStorePooledRemoteWriteHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	receiver := &rawBytesReceiver{}
	if err := dec(receiver); err != nil {
		return nil, err
	}

	wru := GetWriteRequestUnmarshaler()
	wr, err := wru.UnmarshalProtobuf(receiver.data)
	if err != nil {
		PutWriteRequestUnmarshaler(wru)
		return nil, status.Errorf(codes.InvalidArgument, "failed to unmarshal WriteRequest: %v", err)
	}

	pooledReq := &PooledWriteRequest{
		WriteRequest: wr,
		wru:          wru,
	}
	defer pooledReq.release()

	if interceptor == nil {
		return srv.(WriteableStoreServerPooled).RemoteWritePooled(ctx, pooledReq)
	}

	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/thanos.WriteableStore/RemoteWrite",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(WriteableStoreServerPooled).RemoteWritePooled(ctx, req.(*PooledWriteRequest))
	}
	return interceptor(ctx, pooledReq, info, handler)
}

var writeableStorePooledServiceDesc = grpc.ServiceDesc{
	ServiceName: "thanos.WriteableStore",
	HandlerType: (*WriteableStoreServerPooled)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RemoteWrite",
			Handler:    writeableStorePooledRemoteWriteHandler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "store/storepb/rpc.proto",
}
