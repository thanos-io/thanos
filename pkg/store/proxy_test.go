package store

import (
	"testing"

	"context"

	"io"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/tsdb/chunkenc"
	tlabels "github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestQueryStore_Series(t *testing.T) {
	cls := []*Info{
		{
			Client: &storeClient{
				RespSet: []*storepb.SeriesResponse{
					storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storepb.NewWarnSeriesResponse(errors.New("partial error")),
					storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{2, 2}, {3, 3}, {4, 4}}),
				},
			},
			MinTime: 1,
			MaxTime: 300,
		},
		{
			Client: &storeClient{
				RespSet: []*storepb.SeriesResponse{
					storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
				},
			},
			MinTime: 1,
			MaxTime: 300,
		},
		{
			Client: &storeClient{
				RespSet: []*storepb.SeriesResponse{
					storepb.NewWarnSeriesResponse(errors.New("partial error")),
				},
			},
			MinTime: 1,
			MaxTime: 300,
		},
		{
			Client: &storeClient{
				RespSet: []*storepb.SeriesResponse{
					storeSeriesResponse(t, labels.FromStrings("a", "c"), []sample{{100, 1}, {300, 3}, {400, 4}}),
				},
			},
			MinTime: 1,
			MaxTime: 300,
		},
		{
			Client: &storeClient{
				RespSet: []*storepb.SeriesResponse{
					storeSeriesResponse(t, labels.FromStrings("a", "outside"), []sample{{1, 1}}),
				},
			},
			// Outside range for store itself.
			MinTime: 301,
			MaxTime: 302,
		},
	}
	q := NewProxyStore(nil, func() []*Info { return cls }, tlabels.FromStrings("fed", "a"))

	ctx := context.Background()
	s1 := newStoreSeriesServer(ctx)

	// This should return empty response, since there is external label mismatch.
	err := q.Series(
		&storepb.SeriesRequest{
			MinTime:  1,
			MaxTime:  300,
			Matchers: []storepb.LabelMatcher{{Name: "fed", Value: "not-a", Type: storepb.LabelMatcher_EQ}},
		}, s1,
	)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(s1.SeriesSet))
	testutil.Equals(t, 0, len(s1.Warnings))

	s2 := newStoreSeriesServer(ctx)
	err = q.Series(
		&storepb.SeriesRequest{
			MinTime:  1,
			MaxTime:  300,
			Matchers: []storepb.LabelMatcher{{Name: "fed", Value: "a", Type: storepb.LabelMatcher_EQ}},
		}, s2,
	)
	testutil.Ok(t, err)

	expected := []struct {
		lset    []storepb.Label
		samples []sample
	}{
		{
			lset:    []storepb.Label{{Name: "a", Value: "a"}},
			samples: []sample{{0, 0}, {2, 1}, {3, 2}},
		},
		{
			lset:    []storepb.Label{{Name: "a", Value: "b"}},
			samples: []sample{{2, 2}, {3, 3}, {4, 4}, {1, 1}, {2, 2}, {3, 3}},
		},
		{
			lset:    []storepb.Label{{Name: "a", Value: "c"}},
			samples: []sample{{100, 1}, {300, 3}, {400, 4}},
		},
	}

	// We should have all series given by all our clients.
	testutil.Equals(t, len(expected), len(s2.SeriesSet))

	for i, series := range s2.SeriesSet {
		testutil.Equals(t, expected[i].lset, series.Labels)

		k := 0
		for _, chk := range series.Chunks {
			c, err := chunkenc.FromData(chunkenc.EncXOR, chk.Raw.Data)
			testutil.Ok(t, err)

			iter := c.Iterator()
			for iter.Next() {
				testutil.Assert(t, k < len(expected[i].samples), "more samples than expected")

				tv, v := iter.At()
				testutil.Equals(t, expected[i].samples[k], sample{tv, v})
				k++
			}
			testutil.Ok(t, iter.Err())
		}
		testutil.Equals(t, len(expected[i].samples), k)
	}

	// We should have all warnings given by all our clients too.
	testutil.Equals(t, 2, len(s2.Warnings))
}

func TestStoreMatches(t *testing.T) {
	cases := []struct {
		s          *Info
		mint, maxt int64
		ms         []storepb.LabelMatcher
		ok         bool
	}{
		{
			s: &Info{Labels: []storepb.Label{{"a", "b"}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "1"},
			},
			ok: true,
		},
		{
			s:    &Info{MinTime: 100, MaxTime: 200},
			mint: 201,
			maxt: 300,
			ok:   false,
		},
		{
			s:    &Info{MinTime: 100, MaxTime: 200},
			mint: 200,
			maxt: 300,
			ok:   true,
		},
		{
			s:    &Info{MinTime: 100, MaxTime: 200},
			mint: 50,
			maxt: 99,
			ok:   false,
		},
		{
			s:    &Info{MinTime: 100, MaxTime: 200},
			mint: 50,
			maxt: 100,
			ok:   true,
		},
		{
			s: &Info{Labels: []storepb.Label{{"a", "b"}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			},
			ok: true,
		},
		{
			s: &Info{Labels: []storepb.Label{{"a", "b"}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "c"},
			},
			ok: false,
		},
		{
			s: &Info{Labels: []storepb.Label{{"a", "b"}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_RE, Name: "a", Value: "b|c"},
			},
			ok: true,
		},
		{
			s: &Info{Labels: []storepb.Label{{"a", "b"}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_NEQ, Name: "a", Value: ""},
			},
			ok: true,
		},
	}

	for i, c := range cases {
		ok, err := storeMatches(c.s, c.mint, c.maxt, c.ms...)
		testutil.Ok(t, err)
		testutil.Assert(t, c.ok == ok, "test case %d failed", i)
	}
}

// storeSeriesServer is test gRPC storeAPI series server.
type storeSeriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer
	ctx context.Context

	SeriesSet []storepb.Series
	Warnings  []string
}

func newStoreSeriesServer(ctx context.Context) *storeSeriesServer {
	return &storeSeriesServer{ctx: ctx}
}

func (s *storeSeriesServer) Send(r *storepb.SeriesResponse) error {
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

func (s *storeSeriesServer) Context() context.Context {
	return s.ctx
}

// storeClient is test gRPC store API client.
type storeClient struct {
	Values map[string][]string

	RespSet []*storepb.SeriesResponse
}

func (s *storeClient) Info(ctx context.Context, req *storepb.InfoRequest, _ ...grpc.CallOption) (*storepb.InfoResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *storeClient) Series(ctx context.Context, req *storepb.SeriesRequest, _ ...grpc.CallOption) (storepb.Store_SeriesClient, error) {
	return &StoreSeriesClient{ctx: ctx, respSet: s.RespSet}, nil
}

func (s *storeClient) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest, _ ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *storeClient) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest, _ ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
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

// storeSeriesResponse creates test storepb.SeriesResponse that includes series with single chunk that stores all the given samples.
func storeSeriesResponse(t testing.TB, lset labels.Labels, smpls []sample) *storepb.SeriesResponse {
	var s storepb.Series

	for _, l := range lset {
		s.Labels = append(s.Labels, storepb.Label{Name: l.Name, Value: l.Value})
	}
	c := chunkenc.NewXORChunk()
	a, err := c.Appender()
	testutil.Ok(t, err)

	for _, smpl := range smpls {
		a.Append(smpl.t, smpl.v)
	}
	s.Chunks = append(s.Chunks, storepb.AggrChunk{
		MinTime: smpls[0].t,
		MaxTime: smpls[len(smpls)-1].t,
		Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: c.Bytes()},
	})
	return storepb.NewSeriesResponse(&s)
}
