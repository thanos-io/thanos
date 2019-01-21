package store

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/gogo/protobuf/proto"
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

func TestProxyStore_Series_StoresFetchFail(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	q := NewProxyStore(nil,
		func(_ context.Context) ([]Client, error) { return nil, errors.New("Fail") },
		nil,
	)

	s := newStoreSeriesServer(context.Background())
	testutil.NotOk(t, q.Series(&storepb.SeriesRequest{
		MinTime:  1,
		MaxTime:  300,
		Matchers: []storepb.LabelMatcher{{Name: "a", Value: "a", Type: storepb.LabelMatcher_EQ}},
	}, s))
}

func TestProxyStore_Series(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	for _, tc := range []struct {
		title          string
		storeAPIs      []Client
		selectorLabels tlabels.Labels

		req *storepb.SeriesRequest

		expectedSeries      []rawSeries
		expectedErr         error
		expectedWarningsLen int
	}{
		{
			title: "no storeAPI available",
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "a", Value: "a", Type: storepb.LabelMatcher_EQ}},
			},
			expectedWarningsLen: 1, // No store matched for this query.
		},
		{
			title: "no storeAPI available for 301-302 time range",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					minTime: 1,
					maxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  301,
				MaxTime:  400,
				Matchers: []storepb.LabelMatcher{{Name: "a", Value: "a", Type: storepb.LabelMatcher_EQ}},
			},
			expectedWarningsLen: 1, // No store matched for this query.
		},
		{
			title: "storeAPI available for time range; no series for ext=2 external label matcher",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					minTime: 1,
					maxTime: 300,
					labels:  []storepb.Label{{Name: "ext", Value: "1"}},
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "2", Type: storepb.LabelMatcher_EQ}},
			},
			expectedWarningsLen: 1, // No store matched for this query.
		},
		{
			title: "storeAPI available for time range; available series for ext=1 external label matcher",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					minTime: 1,
					maxTime: 300,
					labels:  []storepb.Label{{Name: "ext", Value: "1"}},
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:    []storepb.Label{{Name: "a", Value: "a"}},
					samples: []sample{{0, 0}, {2, 1}, {3, 2}},
				},
			},
		},
		{
			title: "storeAPI available for time range; available series for any external label matcher",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					minTime: 1,
					maxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:    []storepb.Label{{Name: "a", Value: "a"}},
					samples: []sample{{0, 0}, {2, 1}, {3, 2}},
				},
			},
		},
		{
			title: "storeAPI available for time range; available series for any external label matcher, but selector blocks",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					minTime: 1,
					maxTime: 300,
				},
			},
			selectorLabels: tlabels.FromStrings("ext", "2"),
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
		},
		{
			title: "no validation if storeAPI follow matching contract",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					minTime: 1,
					maxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "a", Value: "b", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					// We did not ask for a=a, but we trust StoreAPI will match correctly, so proxy does check any of this.
					lset:    []storepb.Label{{Name: "a", Value: "a"}},
					samples: []sample{{0, 0}, {2, 1}, {3, 2}},
				},
			},
		},
		{
			title: "complex scenario with storeAPIs warnings",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{2, 2}, {3, 3}, {4, 4}}),
						},
					},
					minTime: 1,
					maxTime: 300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					minTime: 1,
					maxTime: 300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
						},
					},
					minTime: 1,
					maxTime: 300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "c"), []sample{{100, 1}, {300, 3}, {400, 4}}),
						},
					},
					minTime: 1,
					maxTime: 300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "outside"), []sample{{1, 1}}),
						},
					},
					// Outside range for store itself.
					minTime: 301,
					maxTime: 302,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:    []storepb.Label{{Name: "a", Value: "a"}},
					samples: []sample{{0, 0}, {2, 1}, {3, 2}},
				},
				{
					lset:    []storepb.Label{{Name: "a", Value: "b"}},
					samples: []sample{{2, 2}, {3, 3}, {4, 4}, {1, 1}, {2, 2}, {3, 3}}, // No sort merge.
				},
				{
					lset:    []storepb.Label{{Name: "a", Value: "c"}},
					samples: []sample{{100, 1}, {300, 3}, {400, 4}},
				},
			},
			expectedWarningsLen: 2,
		},
		{
			title: "same external labels are validated during upload and on querier storeset, proxy does not care",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					labels:  []storepb.Label{{Name: "ext", Value: "1"}},
					minTime: 1,
					maxTime: 300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 11}, {2, 22}, {3, 33}}),
						},
					},
					labels:  []storepb.Label{{Name: "ext", Value: "1"}},
					minTime: 1,
					maxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:    []storepb.Label{{Name: "a", Value: "b"}},
					samples: []sample{{1, 1}, {2, 2}, {3, 3}, {1, 11}, {2, 22}, {3, 33}},
				},
			},
		},
		{
			title: "partial response enabled",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					labels:  []storepb.Label{{Name: "ext", Value: "1"}},
					minTime: 1,
					maxTime: 300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespError: errors.New("error!"),
					},
					labels:  []storepb.Label{{Name: "ext", Value: "1"}},
					minTime: 1,
					maxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:    []storepb.Label{{Name: "a", Value: "b"}},
					samples: []sample{{1, 1}, {2, 2}, {3, 3}},
				},
			},
			expectedWarningsLen: 2,
		},
		{
			title: "partial response disabled",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					labels:  []storepb.Label{{Name: "ext", Value: "1"}},
					minTime: 1,
					maxTime: 300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespError: errors.New("error!"),
					},
					labels:  []storepb.Label{{Name: "ext", Value: "1"}},
					minTime: 1,
					maxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:                 1,
				MaxTime:                 300,
				Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
				PartialResponseDisabled: true,
			},
			expectedErr: errors.New("fetch series for [name:\"ext\" value:\"1\" ] test: error!"),
		},
	} {
		if ok := t.Run(tc.title, func(t *testing.T) {
			q := NewProxyStore(nil,
				func(_ context.Context) ([]Client, error) { return tc.storeAPIs, nil }, // what if err?
				tc.selectorLabels,
			)

			s := newStoreSeriesServer(context.Background())

			err := q.Series(tc.req, s)
			if tc.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tc.expectedErr.Error(), err.Error())
				return
			}

			testutil.Ok(t, err)

			seriesEqual(t, tc.expectedSeries, s.SeriesSet)
			testutil.Equals(t, tc.expectedWarningsLen, len(s.Warnings), "got %v", s.Warnings)
		}); !ok {
			return
		}
	}
}

func TestProxyStore_Series_RequestParamsProxied(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	m := &mockedStoreAPI{
		RespSeries: []*storepb.SeriesResponse{
			storepb.NewWarnSeriesResponse(errors.New("warning")),
		},
	}
	cls := []Client{
		&testClient{
			StoreClient: m,
			labels:      []storepb.Label{{Name: "ext", Value: "1"}},
			minTime:     1,
			maxTime:     300,
		},
	}
	q := NewProxyStore(nil,
		func(context.Context) ([]Client, error) { return cls, nil },
		nil,
	)

	ctx := context.Background()
	s := newStoreSeriesServer(ctx)

	req := &storepb.SeriesRequest{
		MinTime:                 1,
		MaxTime:                 300,
		Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
		PartialResponseDisabled: true,
		Aggregates: []storepb.Aggr{
			storepb.Aggr_COUNTER,
			storepb.Aggr_COUNT,
		},
		MaxResolutionWindow: 1234,
	}
	testutil.Ok(t, q.Series(req, s))

	testutil.Assert(t, proto.Equal(req, m.LastSeriesReq), "request was not proxied properly to underlying storeAPI: %s vs %s", req, m.LastSeriesReq)
}

func TestProxyStore_Series_RegressionFillResponseChannel(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	var cls []Client
	for i := 0; i < 10; i++ {
		cls = append(cls, &testClient{
			StoreClient: &mockedStoreAPI{
				RespError: errors.New("test error"),
			},
			minTime: 1,
			maxTime: 300,
		})
		cls = append(cls, &testClient{
			StoreClient: &mockedStoreAPI{
				RespSeries: []*storepb.SeriesResponse{
					storepb.NewWarnSeriesResponse(errors.New("warning")),
					storepb.NewWarnSeriesResponse(errors.New("warning")),
					storepb.NewWarnSeriesResponse(errors.New("warning")),
					storepb.NewWarnSeriesResponse(errors.New("warning")),
					storepb.NewWarnSeriesResponse(errors.New("warning")),
					storepb.NewWarnSeriesResponse(errors.New("warning")),
					storepb.NewWarnSeriesResponse(errors.New("warning")),
					storepb.NewWarnSeriesResponse(errors.New("warning")),
					storepb.NewWarnSeriesResponse(errors.New("warning")),
					storepb.NewWarnSeriesResponse(errors.New("warning")),
				},
			},
			minTime: 1,
			maxTime: 300,
		})

	}

	q := NewProxyStore(nil,
		func(context.Context) ([]Client, error) { return cls, nil },
		tlabels.FromStrings("fed", "a"),
	)

	ctx := context.Background()
	s := newStoreSeriesServer(ctx)

	testutil.Ok(t, q.Series(
		&storepb.SeriesRequest{
			MinTime:  1,
			MaxTime:  300,
			Matchers: []storepb.LabelMatcher{{Name: "fed", Value: "a", Type: storepb.LabelMatcher_EQ}},
		}, s,
	))
	testutil.Equals(t, 0, len(s.SeriesSet))
	testutil.Equals(t, 110, len(s.Warnings))
}

func TestProxyStore_LabelValues(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	m1 := &mockedStoreAPI{
		RespLabelValues: &storepb.LabelValuesResponse{
			Values:   []string{"1", "2"},
			Warnings: []string{"warning"},
		},
	}
	cls := []Client{
		&testClient{StoreClient: m1},
		&testClient{StoreClient: &mockedStoreAPI{
			RespLabelValues: &storepb.LabelValuesResponse{
				Values: []string{"3", "4"},
			},
		}},
	}
	q := NewProxyStore(nil,
		func(context.Context) ([]Client, error) { return cls, nil },
		nil,
	)

	ctx := context.Background()
	req := &storepb.LabelValuesRequest{
		Label: "a",
		PartialResponseDisabled: true,
	}
	resp, err := q.LabelValues(ctx, req)
	testutil.Ok(t, err)
	testutil.Assert(t, proto.Equal(req, m1.LastLabelValuesReq), "request was not proxied properly to underlying storeAPI: %s vs %s", req, m1.LastLabelValuesReq)

	testutil.Equals(t, []string{"1", "2", "3", "4"}, resp.Values)
	testutil.Equals(t, 1, len(resp.Warnings))
}

type rawSeries struct {
	lset    []storepb.Label
	samples []sample
}

func seriesEqual(t *testing.T, expected []rawSeries, got []storepb.Series) {
	testutil.Equals(t, len(expected), len(got), "got: %v", got)

	for i, series := range got {
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
			s: &testClient{labels: []storepb.Label{{Name: "a", Value: "b"}}},
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
			s: &testClient{labels: []storepb.Label{{Name: "a", Value: "b"}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			},
			ok: true,
		},
		{
			s: &testClient{labels: []storepb.Label{{Name: "a", Value: "b"}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "c"},
			},
			ok: false,
		},
		{
			s: &testClient{labels: []storepb.Label{{Name: "a", Value: "b"}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_RE, Name: "a", Value: "b|c"},
			},
			ok: true,
		},
		{
			s: &testClient{labels: []storepb.Label{{Name: "a", Value: "b"}}},
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

// mockedStoreAPI is test gRPC store API client.
type mockedStoreAPI struct {
	RespSeries      []*storepb.SeriesResponse
	RespLabelValues *storepb.LabelValuesResponse
	RespError       error

	LastSeriesReq      *storepb.SeriesRequest
	LastLabelValuesReq *storepb.LabelValuesRequest
}

func (s *mockedStoreAPI) Info(ctx context.Context, req *storepb.InfoRequest, _ ...grpc.CallOption) (*storepb.InfoResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *mockedStoreAPI) Series(ctx context.Context, req *storepb.SeriesRequest, _ ...grpc.CallOption) (storepb.Store_SeriesClient, error) {
	s.LastSeriesReq = req

	return &StoreSeriesClient{ctx: ctx, respSet: s.RespSeries}, s.RespError
}

func (s *mockedStoreAPI) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest, _ ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *mockedStoreAPI) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest, _ ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	s.LastLabelValuesReq = req

	return s.RespLabelValues, s.RespError
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
