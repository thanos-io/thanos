package store

import (
	"context"
	"io"
	"testing"

	"time"

	"github.com/fortytw2/leaktest"
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

type testClient struct {
	// Just to pass interface check.
	storepb.StoreClient

	labels  []storepb.Label
	minTime int64
	maxTime int64
}

func (c *testClient) Labels() []storepb.Label {
	return c.labels
}

func (c *testClient) TimeRange() (int64, int64) {
	return c.minTime, c.maxTime
}

func (c *testClient) String() string {
	return "test"
}

func TestQueryStore_Series(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	cls := []Client{
		&testClient{
			StoreClient: &storeClient{
				RespSet: []*storepb.SeriesResponse{
					storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storepb.NewWarnSeriesResponse(errors.New("partial error")),
					storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{2, 2}, {3, 3}, {4, 4}}),
				},
			},
			minTime: 1,
			maxTime: 300,
		},
		&testClient{
			StoreClient: &storeClient{
				RespSet: []*storepb.SeriesResponse{
					storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
				},
			},
			minTime: 1,
			maxTime: 300,
		},
		&testClient{
			StoreClient: &storeClient{
				RespSet: []*storepb.SeriesResponse{
					storepb.NewWarnSeriesResponse(errors.New("partial error")),
				},
			},
			minTime: 1,
			maxTime: 300,
		},
		&testClient{
			StoreClient: &storeClient{
				RespSet: []*storepb.SeriesResponse{
					storeSeriesResponse(t, labels.FromStrings("a", "c"), []sample{{100, 1}, {300, 3}, {400, 4}}),
				},
			},
			minTime: 1,
			maxTime: 300,
		},
		&testClient{
			StoreClient: &storeClient{
				RespSet: []*storepb.SeriesResponse{
					storeSeriesResponse(t, labels.FromStrings("a", "outside"), []sample{{1, 1}}),
				},
			},
			// Outside range for store itself.
			minTime: 301,
			maxTime: 302,
		},
	}
	q := NewProxyStore(nil,
		func(context.Context) ([]Client, error) { return cls, nil },
		tlabels.FromStrings("fed", "a"),
	)

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
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	cases := []struct {
		s          Client
		mint, maxt int64
		ms         []storepb.LabelMatcher
		ok         bool
	}{
		{
			s: &testClient{labels: []storepb.Label{{"a", "b"}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "1"},
			},
			ok: true,
		},
		{
			s:    &testClient{minTime: 100, maxTime: 200},
			mint: 201,
			maxt: 300,
			ok:   false,
		},
		{
			s:    &testClient{minTime: 100, maxTime: 200},
			mint: 200,
			maxt: 300,
			ok:   true,
		},
		{
			s:    &testClient{minTime: 100, maxTime: 200},
			mint: 50,
			maxt: 99,
			ok:   false,
		},
		{
			s:    &testClient{minTime: 100, maxTime: 200},
			mint: 50,
			maxt: 100,
			ok:   true,
		},
		{
			s: &testClient{labels: []storepb.Label{{"a", "b"}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			},
			ok: true,
		},
		{
			s: &testClient{labels: []storepb.Label{{"a", "b"}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "c"},
			},
			ok: false,
		},
		{
			s: &testClient{labels: []storepb.Label{{"a", "b"}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_RE, Name: "a", Value: "b|c"},
			},
			ok: true,
		},
		{
			s: &testClient{labels: []storepb.Label{{"a", "b"}}},
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
