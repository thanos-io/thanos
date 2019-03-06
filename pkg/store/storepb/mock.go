package storepb

import (
	"context"

	"google.golang.org/grpc"
)

// StoreClientMock is a structure for mocking StoreClient interface
type StoreClientMock struct {
	InfoCallback        func(ctx context.Context, in *InfoRequest, opts ...grpc.CallOption) (*InfoResponse, error)
	SeriesCallback      func(ctx context.Context, in *SeriesRequest, opts ...grpc.CallOption) (Store_SeriesClient, error)
	LabelNamesCallback  func(ctx context.Context, in *LabelNamesRequest, opts ...grpc.CallOption) (*LabelNamesResponse, error)
	LabelValuesCallback func(ctx context.Context, in *LabelValuesRequest, opts ...grpc.CallOption) (*LabelValuesResponse, error)
}

var _ StoreClient = &StoreClientMock{}

func (c *StoreClientMock) Info(ctx context.Context, in *InfoRequest, opts ...grpc.CallOption) (*InfoResponse, error) {
	return c.InfoCallback(ctx, in, opts...)
}

func (c *StoreClientMock) Series(ctx context.Context, in *SeriesRequest, opts ...grpc.CallOption) (Store_SeriesClient, error) {
	return c.SeriesCallback(ctx, in, opts...)
}

func (c *StoreClientMock) LabelNames(ctx context.Context, in *LabelNamesRequest, opts ...grpc.CallOption) (*LabelNamesResponse, error) {
	return c.LabelNames(ctx, in, opts...)
}

func (c *StoreClientMock) LabelValues(ctx context.Context, in *LabelValuesRequest, opts ...grpc.CallOption) (*LabelValuesResponse, error) {
	return c.LabelValues(ctx, in, opts...)
}
