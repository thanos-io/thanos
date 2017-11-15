package query

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"google.golang.org/grpc"
)

func TestQuerier_LabelValues(t *testing.T) {
	a := &testStoreClient{
		values: map[string][]string{
			"test": []string{"a", "b", "c", "d"},
		},
	}
	b := &testStoreClient{
		values: map[string][]string{
			// The contract is that label values are sorted but we should be resilient
			// to misbehaving clients.
			"test": []string{"a", "out-of-order", "d", "x", "y"},
		},
	}
	c := &testStoreClient{
		values: map[string][]string{
			"test": []string{"e"},
		},
	}
	expected := []string{"a", "b", "c", "d", "e", "out-of-order", "x", "y"}

	q := newQuerier(nil, context.Background(), []StoreInfo{
		testStoreInfo{client: a},
		testStoreInfo{client: b},
		testStoreInfo{client: c},
	}, 0, 10000)
	defer q.Close()

	vals, err := q.LabelValues("test")
	testutil.Ok(t, err)
	testutil.Equals(t, expected, vals)
}

type testStoreInfo struct {
	labels []storepb.Label
	client storepb.StoreClient
}

func (s testStoreInfo) Labels() []storepb.Label {
	return s.labels
}

func (s testStoreInfo) Client() storepb.StoreClient {
	return s.client
}

type testStoreClient struct {
	values map[string][]string
}

func (s *testStoreClient) Info(ctx context.Context, req *storepb.InfoRequest, _ ...grpc.CallOption) (*storepb.InfoResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *testStoreClient) Series(ctx context.Context, req *storepb.SeriesRequest, _ ...grpc.CallOption) (*storepb.SeriesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *testStoreClient) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest, _ ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *testStoreClient) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest, _ ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	return &storepb.LabelValuesResponse{Values: s.values[req.Label]}, nil
}
