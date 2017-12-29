package testutil

import (
	"context"
	"io"
	"testing"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/tsdb/chunkenc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StoreClient is test gRPC store API client.
type StoreClient struct {
	Values map[string][]string

	RespSet []*storepb.SeriesResponse
}

func (s *StoreClient) Info(ctx context.Context, req *storepb.InfoRequest, _ ...grpc.CallOption) (*storepb.InfoResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *StoreClient) Series(ctx context.Context, req *storepb.SeriesRequest, _ ...grpc.CallOption) (storepb.Store_SeriesClient, error) {
	return &StoreSeriesClient{ctx: ctx, respSet: s.RespSet}, nil
}

func (s *StoreClient) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest, _ ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *StoreClient) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest, _ ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	return &storepb.LabelValuesResponse{Values: s.Values[req.Label]}, nil
}

// StoreSeriesClient is test gRPC storeAPI series client.
type StoreSeriesClient struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesClient
	ctx     context.Context
	i       int
	respSet []*storepb.SeriesResponse
}

func (c *StoreSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	if c.i >= len(c.respSet) {
		return nil, io.EOF
	}
	s := c.respSet[c.i]
	c.i++

	return s, nil
}

func (c *StoreSeriesClient) Context() context.Context {
	return c.ctx
}

type Sample struct {
	T int64
	V float64
}

func storeSeries(t testing.TB, lset labels.Labels, smpls []Sample) storepb.Series {
	var s storepb.Series

	for _, l := range lset {
		s.Labels = append(s.Labels, storepb.Label{Name: l.Name, Value: l.Value})
	}
	c := chunkenc.NewXORChunk()
	a, err := c.Appender()
	Ok(t, err)

	for _, smpl := range smpls {
		a.Append(smpl.T, smpl.V)
	}
	s.Chunks = append(s.Chunks, storepb.Chunk{
		Type:    storepb.Chunk_XOR,
		MinTime: smpls[0].T,
		MaxTime: smpls[len(smpls)-1].T,
		Data:    c.Bytes(),
	})
	return s
}

// StoreSeriesResponse creates test storepb.SeriesResponse that includes series with single chunk that stores all the given samples.
func StoreSeriesResponse(t testing.TB, lset labels.Labels, smpls []Sample) *storepb.SeriesResponse {
	s := storeSeries(t, lset, smpls)
	return storepb.NewSeriesResponse(&s)
}
