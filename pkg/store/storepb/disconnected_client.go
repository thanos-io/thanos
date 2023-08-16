// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var ErrStoreNotConnected = errors.New("store is not connected")

type disconnectedClient struct{}

func NewDisconnectedClient() *disconnectedClient {
	return &disconnectedClient{}
}

func (d disconnectedClient) Info(ctx context.Context, in *InfoRequest, opts ...grpc.CallOption) (*InfoResponse, error) {
	return nil, ErrStoreNotConnected
}

func (d disconnectedClient) Series(ctx context.Context, in *SeriesRequest, opts ...grpc.CallOption) (Store_SeriesClient, error) {
	return nil, ErrStoreNotConnected
}

func (d disconnectedClient) LabelNames(ctx context.Context, in *LabelNamesRequest, opts ...grpc.CallOption) (*LabelNamesResponse, error) {
	return nil, ErrStoreNotConnected
}

func (d disconnectedClient) LabelValues(ctx context.Context, in *LabelValuesRequest, opts ...grpc.CallOption) (*LabelValuesResponse, error) {
	return nil, ErrStoreNotConnected
}
