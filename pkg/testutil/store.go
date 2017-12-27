package testutil

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/tsdb/chunkenc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WarningSeries is a series that triggers partial error warning.
var WarningSeries = storepb.Series{Labels: []storepb.Label{{Name: "WarningSeries"}}}

// StoreClient is test gRPC store API client.
type StoreClient struct {
	Values    map[string][]string
	SeriesSet []storepb.Series
}

func (s *StoreClient) Info(ctx context.Context, req *storepb.InfoRequest, _ ...grpc.CallOption) (*storepb.InfoResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *StoreClient) Series(ctx context.Context, req *storepb.SeriesRequest, _ ...grpc.CallOption) (storepb.Store_SeriesClient, error) {
	return &StoreSeriesClient{ctx: ctx, seriesSet: s.SeriesSet}, nil
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
	ctx       context.Context
	i         int
	seriesSet []storepb.Series
}

func (c *StoreSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	if c.i >= len(c.seriesSet) {
		return nil, io.EOF
	}
	s := c.seriesSet[c.i]
	c.i++

	if s.String() == WarningSeries.String() {
		// This indicates that we should send warning instead.
		return storepb.NewWarnSeriesResponse(errors.New("warn")), nil

	}
	return storepb.NewSeriesResponse(&s), nil
}

func (c *StoreSeriesClient) Context() context.Context {
	return c.ctx
}

// StoreSeriesServer is test gRPC storeAPI series server.
type StoreSeriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer
	ctx context.Context

	SeriesSet []storepb.Series
	Warnings  []string
}

func NewStoreSeriesServer(ctx context.Context) *StoreSeriesServer {
	return &StoreSeriesServer{ctx: ctx}
}

func (s *StoreSeriesServer) Send(r *storepb.SeriesResponse) error {
	if r.GetWarning() != "" {
		s.Warnings = append(s.Warnings, r.GetWarning())
		return nil
	}

	if r.GetSeries() == nil {
		return errors.New("no seriesSet")
	}
	s.SeriesSet = append(s.SeriesSet, *r.GetSeries())
	return nil
}

func (s *StoreSeriesServer) Context() context.Context {
	return s.ctx
}

type Sample struct {
	T int64
	V float64
}

// StoreSeries creates test series with single chunk that stores all the given samples.
func StoreSeries(t testing.TB, lset labels.Labels, smpls []Sample) (s storepb.Series) {
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
