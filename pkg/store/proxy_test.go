// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testClient struct {
	// Just to pass interface check.
	storepb.StoreClient

	labelSets []storepb.LabelSet
	minTime   int64
	maxTime   int64
}

func (c *testClient) LabelSets() []storepb.LabelSet {
	return c.labelSets
}

func (c *testClient) TimeRange() (int64, int64) {
	return c.minTime, c.maxTime
}

func (c *testClient) String() string {
	return "test"
}

func (c *testClient) Addr() string {
	return "testaddr"
}

func TestProxyStore_Info(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := NewProxyStore(nil,
		nil,
		func() []Client { return nil },
		component.Query,
		nil, 0*time.Second,
	)

	resp, err := q.Info(ctx, &storepb.InfoRequest{})
	testutil.Ok(t, err)
	testutil.Equals(t, []storepb.LabelSet(nil), resp.LabelSets)
	testutil.Equals(t, storepb.StoreType_QUERY, resp.StoreType)
	testutil.Equals(t, int64(0), resp.MinTime)
	testutil.Equals(t, int64(math.MaxInt64), resp.MaxTime)
}

func TestProxyStore_Series(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	for _, tc := range []struct {
		title          string
		storeAPIs      []Client
		selectorLabels labels.Labels

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
					minTime:   1,
					maxTime:   300,
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
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
					minTime:   1,
					maxTime:   300,
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   []storepb.Label{{Name: "a", Value: "a"}},
					chunks: [][]sample{{{0, 0}, {2, 1}, {3, 2}}},
				},
			},
		},
		{
			title: "storeAPI available for time range; available series for any external label matcher",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{4, 3}}, []sample{{0, 0}, {2, 1}, {3, 2}}),
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
					lset:   []storepb.Label{{Name: "a", Value: "a"}},
					chunks: [][]sample{{{4, 3}}, {{0, 0}, {2, 1}, {3, 2}}}, // No sort merge.
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
			selectorLabels: labels.FromStrings("ext", "2"),
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
					lset:   []storepb.Label{{Name: "a", Value: "a"}},
					chunks: [][]sample{{{0, 0}, {2, 1}, {3, 2}}},
				},
			},
		},
		{
			title: "complex scenario with storeAPIs warnings",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}, []sample{{4, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{5, 4}}), // Continuations of the same series.
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
					lset:   []storepb.Label{{Name: "a", Value: "a"}},
					chunks: [][]sample{{{0, 0}, {2, 1}, {3, 2}}, {{4, 3}}},
				},
				{
					lset:   []storepb.Label{{Name: "a", Value: "a"}},
					chunks: [][]sample{{{5, 4}}},
				},
				{
					lset:   []storepb.Label{{Name: "a", Value: "b"}},
					chunks: [][]sample{{{2, 2}, {3, 3}, {4, 4}}, {{1, 1}, {2, 2}, {3, 3}}}, // No sort merge.
				},
				{
					lset:   []storepb.Label{{Name: "a", Value: "c"}},
					chunks: [][]sample{{{100, 1}, {300, 3}, {400, 4}}},
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
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 11}, {2, 22}, {3, 33}}),
						},
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   []storepb.Label{{Name: "a", Value: "b"}},
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}, {{1, 11}, {2, 22}, {3, 33}}},
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
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespError: errors.New("error!"),
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   []storepb.Label{{Name: "a", Value: "b"}},
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
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
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespError: errors.New("error!"),
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
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
				nil,
				func() []Client { return tc.storeAPIs },
				component.Query,
				tc.selectorLabels,
				0*time.Second,
			)

			s := newStoreSeriesServer(context.Background())

			err := q.Series(tc.req, s)
			if tc.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tc.expectedErr.Error(), err.Error())
				return
			}

			testutil.Ok(t, err)

			seriesEquals(t, tc.expectedSeries, s.SeriesSet)
			testutil.Equals(t, tc.expectedWarningsLen, len(s.Warnings), "got %v", s.Warnings)
		}); !ok {
			return
		}
	}
}

func TestProxyStore_SeriesSlowStores(t *testing.T) {
	enable := os.Getenv("THANOS_ENABLE_STORE_READ_TIMEOUT_TESTS")
	if enable == "" {
		t.Skip("enable THANOS_ENABLE_STORE_READ_TIMEOUT_TESTS to run store-read-timeout tests")
	}

	defer leaktest.CheckTimeout(t, 20*time.Second)()

	for _, tc := range []struct {
		title          string
		storeAPIs      []Client
		selectorLabels labels.Labels

		req *storepb.SeriesRequest

		expectedSeries      []rawSeries
		expectedErr         error
		expectedWarningsLen int
	}{
		{
			title: "partial response disabled; 1st store is slow, 2nd store is fast;",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration: 10 * time.Second,
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:                 1,
				MaxTime:                 300,
				Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
				PartialResponseDisabled: true,
			},
			expectedErr: errors.New("test: failed to receive any data in 4s from test: context deadline exceeded"),
		},
		{
			title: "partial response disabled; 1st store is fast, 2nd store is slow;",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration: 10 * time.Second,
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:                 1,
				MaxTime:                 300,
				Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
				PartialResponseDisabled: true,
			},
			expectedErr: errors.New("test: failed to receive any data in 4s from test: context deadline exceeded"),
		},
		{
			title: "partial response disabled; 1st store is slow on 2nd series, 2nd store is fast;",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{3, 1}, {4, 2}, {5, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{6, 1}, {7, 2}, {8, 3}}),
						},
						RespDuration:    10 * time.Second,
						SlowSeriesIndex: 2,
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:                 1,
				MaxTime:                 300,
				Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
				PartialResponseDisabled: true,
			},
			expectedErr: errors.New("test: failed to receive any data in 4s from test: context deadline exceeded"),
		},
		{
			title: "partial response disabled; 1st store is fast to respond, 2nd store is slow on 2nd series;",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{3, 1}, {4, 2}, {5, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{6, 1}, {7, 2}, {8, 3}}),
						},
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration:    10 * time.Second,
						SlowSeriesIndex: 2,
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:                 1,
				MaxTime:                 300,
				Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
				PartialResponseDisabled: true,
			},
			expectedErr: errors.New("test: failed to receive any data in 4s from test: context deadline exceeded"),
		},
		{
			title: "partial response enabled; 1st store is slow to respond, 2nd store is fast;",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration: 10 * time.Second,
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   []storepb.Label{{Name: "b", Value: "c"}},
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
			expectedWarningsLen: 2,
		},
		{
			title: "partial response enabled; 1st store is fast, 2nd store is slow;",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration: 10 * time.Second,
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   []storepb.Label{{Name: "a", Value: "b"}},
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
			expectedWarningsLen: 2,
		},
		{
			title: "partial response enabled; 1st store is fast, 2-3 is slow, 4th is fast;",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration: 10 * time.Second,
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("c", "d"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration: 10 * time.Second,
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("d", "f"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   []storepb.Label{{Name: "a", Value: "b"}},
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
				{
					lset:   []storepb.Label{{Name: "d", Value: "f"}},
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
			expectedWarningsLen: 4,
		},
		{
			title: "partial response enabled; 1st store is slow on 2nd series, 2nd store is fast",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{7, 1}, {8, 2}, {9, 3}}),
						},
						RespDuration:    10 * time.Second,
						SlowSeriesIndex: 2,
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   []storepb.Label{{Name: "a", Value: "b"}},
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
				{
					lset:   []storepb.Label{{Name: "b", Value: "c"}},
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
			expectedWarningsLen: 3,
		},
		{
			title: "partial response disabled; all stores respond 3s",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{7, 1}, {8, 2}, {9, 3}}),
						},
						RespDuration: 3 * time.Second,
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:                 1,
				MaxTime:                 300,
				Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
				PartialResponseDisabled: true,
			},
			expectedSeries: []rawSeries{
				{
					lset:   []storepb.Label{{Name: "a", Value: "b"}},
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
			expectedErr: errors.New("test: failed to receive any data from test: context deadline exceeded"),
		},
		{
			title: "partial response enabled; all stores respond 3s",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{7, 1}, {8, 2}, {9, 3}}),
						},
						RespDuration: 3 * time.Second,
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{1, 1}, {2, 2}, {3, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{7, 1}, {8, 2}, {9, 3}}),
						},
						RespDuration: 3 * time.Second,
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   []storepb.Label{{Name: "a", Value: "b"}},
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
				{
					lset:   []storepb.Label{{Name: "b", Value: "c"}},
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
			expectedWarningsLen: 2,
		},
	} {
		if ok := t.Run(tc.title, func(t *testing.T) {
			q := NewProxyStore(nil,
				nil,
				func() []Client { return tc.storeAPIs },
				component.Query,
				tc.selectorLabels,
				4*time.Second,
			)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s := newStoreSeriesServer(ctx)

			t0 := time.Now()
			err := q.Series(tc.req, s)
			elapsedTime := time.Since(t0)
			if tc.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tc.expectedErr.Error(), err.Error())
				return
			}

			testutil.Ok(t, err)

			seriesEquals(t, tc.expectedSeries, s.SeriesSet)
			testutil.Equals(t, tc.expectedWarningsLen, len(s.Warnings), "got %v", s.Warnings)

			testutil.Assert(t, elapsedTime < 5010*time.Millisecond, fmt.Sprintf("Request has taken %f, expected: <%d, it seems that responseTimeout doesn't work properly.", elapsedTime.Seconds(), 5))
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
			labelSets:   []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
			minTime:     1,
			maxTime:     300,
		},
	}
	q := NewProxyStore(nil,
		nil,
		func() []Client { return cls },
		component.Query,
		nil,
		0*time.Second,
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
		nil,
		func() []Client { return cls },
		component.Query,
		labels.FromStrings("fed", "a"),
		0*time.Second,
	)

	ctx := context.Background()
	s := newStoreSeriesServer(ctx)

	testutil.Ok(t, q.Series(
		&storepb.SeriesRequest{
			MinTime:  1,
			MaxTime:  300,
			Matchers: []storepb.LabelMatcher{{Name: "any", Value: ".*", Type: storepb.LabelMatcher_RE}},
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
		nil,
		func() []Client { return cls },
		component.Query,
		nil,
		0*time.Second,
	)

	ctx := context.Background()
	req := &storepb.LabelValuesRequest{
		Label:                   "a",
		PartialResponseDisabled: true,
	}
	resp, err := q.LabelValues(ctx, req)
	testutil.Ok(t, err)
	testutil.Assert(t, proto.Equal(req, m1.LastLabelValuesReq), "request was not proxied properly to underlying storeAPI: %s vs %s", req, m1.LastLabelValuesReq)

	testutil.Equals(t, []string{"1", "2", "3", "4"}, resp.Values)
	testutil.Equals(t, 1, len(resp.Warnings))
}

func TestProxyStore_LabelNames(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	for _, tc := range []struct {
		title     string
		storeAPIs []Client

		req *storepb.LabelNamesRequest

		expectedNames       []string
		expectedErr         error
		expectedWarningsLen int
	}{
		{
			title: "label_names partial response disabled",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespLabelNames: &storepb.LabelNamesResponse{
							Names: []string{"a", "b"},
						},
					},
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespLabelNames: &storepb.LabelNamesResponse{
							Names: []string{"a", "c", "d"},
						},
					},
				},
			},
			req: &storepb.LabelNamesRequest{
				PartialResponseDisabled: true,
			},
			expectedNames:       []string{"a", "b", "c", "d"},
			expectedWarningsLen: 0,
		},
		{
			title: "label_names partial response disabled, but returns error",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespLabelNames: &storepb.LabelNamesResponse{
							Names: []string{"a", "b"},
						},
					},
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespError: errors.New("error!"),
					},
				},
			},
			req: &storepb.LabelNamesRequest{
				PartialResponseDisabled: true,
			},
			expectedErr: errors.New("fetch label names from store test: error!"),
		},
		{
			title: "label_names partial response enabled",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespLabelNames: &storepb.LabelNamesResponse{
							Names: []string{"a", "b"},
						},
					},
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespError: errors.New("error!"),
					},
				},
			},
			req: &storepb.LabelNamesRequest{
				PartialResponseDisabled: false,
			},
			expectedNames:       []string{"a", "b"},
			expectedWarningsLen: 1,
		},
	} {
		if ok := t.Run(tc.title, func(t *testing.T) {
			q := NewProxyStore(
				nil,
				nil,
				func() []Client { return tc.storeAPIs },
				component.Query,
				nil,
				0*time.Second,
			)

			ctx := context.Background()
			resp, err := q.LabelNames(ctx, tc.req)
			if tc.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tc.expectedErr.Error(), err.Error())
				return
			}
			testutil.Ok(t, err)

			testutil.Equals(t, tc.expectedNames, resp.Names)
			testutil.Equals(t, tc.expectedWarningsLen, len(resp.Warnings), "got %v", resp.Warnings)
		}); !ok {
			return
		}
	}
}

type rawSeries struct {
	lset   []storepb.Label
	chunks [][]sample
}

func seriesEquals(t *testing.T, expected []rawSeries, got []storepb.Series) {
	testutil.Equals(t, len(expected), len(got), "got: %v", got)

	for i, series := range got {
		testutil.Equals(t, expected[i].lset, series.Labels)
		testutil.Equals(t, len(expected[i].chunks), len(series.Chunks), "unexpected number of chunks for series %v", series.Labels)

		for k, chk := range series.Chunks {
			c, err := chunkenc.FromData(chunkenc.EncXOR, chk.Raw.Data)
			testutil.Ok(t, err)

			j := 0
			iter := c.Iterator(nil)
			for iter.Next() {
				testutil.Assert(t, j < len(expected[i].chunks[k]), "more samples than expected for %v chunk %d", series.Labels, k)

				tv, v := iter.At()
				testutil.Equals(t, expected[i].chunks[k][j], sample{tv, v})
				j++
			}
			testutil.Ok(t, iter.Err())
			testutil.Equals(t, len(expected[i].chunks[k]), j)
		}
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
			s: &testClient{labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "a", Value: "b"}}}}},
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
			s: &testClient{labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "a", Value: "b"}}}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			},
			ok: true,
		},
		{
			s: &testClient{labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "a", Value: "b"}}}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "c"},
			},
			ok: false,
		},
		{
			s: &testClient{labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "a", Value: "b"}}}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_RE, Name: "a", Value: "b|c"},
			},
			ok: true,
		},
		{
			s: &testClient{labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "a", Value: "b"}}}}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_NEQ, Name: "a", Value: ""},
			},
			ok: true,
		},
		{
			s: &testClient{labelSets: []storepb.LabelSet{
				{Labels: []storepb.Label{{Name: "a", Value: "b"}}},
				{Labels: []storepb.Label{{Name: "a", Value: "c"}}},
				{Labels: []storepb.Label{{Name: "a", Value: "d"}}},
			}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "e"},
			},
			ok: false,
		},
		{
			s: &testClient{labelSets: []storepb.LabelSet{
				{Labels: []storepb.Label{{Name: "a", Value: "b"}}},
				{Labels: []storepb.Label{{Name: "a", Value: "c"}}},
				{Labels: []storepb.Label{{Name: "a", Value: "d"}}},
			}},
			ms: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "c"},
			},
			ok: true,
		},
		{
			s: &testClient{labelSets: []storepb.LabelSet{
				{Labels: []storepb.Label{{Name: "a", Value: "b"}}},
				{Labels: []storepb.Label{{Name: "a", Value: "c"}}},
				{Labels: []storepb.Label{{Name: "a", Value: "d"}}},
			}},
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

	Size int64
}

func newStoreSeriesServer(ctx context.Context) *storeSeriesServer {
	return &storeSeriesServer{ctx: ctx}
}

func (s *storeSeriesServer) Send(r *storepb.SeriesResponse) error {
	s.Size += int64(r.Size())

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

// rulesServer is test gRPC storeAPI series server.
type rulesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Rules_RulesServer

	ctx context.Context

	Groups   []storepb.RuleGroup
	Warnings []string

	Size int64
}

func newRulesServer(ctx context.Context) *rulesServer {
	return &rulesServer{ctx: ctx}
}

func (s *rulesServer) Send(r *storepb.RulesResponse) error {
	s.Size += int64(r.Size())

	if r.GetWarning() != "" {
		s.Warnings = append(s.Warnings, r.GetWarning())
		return nil
	}

	if r.GetGroup() == nil {
		return errors.New("no grup")
	}
	s.Groups = append(s.Groups, *r.GetGroup())
	return nil
}

func (s *rulesServer) Context() context.Context {
	return s.ctx
}

// mockedStoreAPI is test gRPC store API client.
type mockedStoreAPI struct {
	RespSeries      []*storepb.SeriesResponse
	RespLabelValues *storepb.LabelValuesResponse
	RespLabelNames  *storepb.LabelNamesResponse
	RespError       error
	RespDuration    time.Duration
	// Index of series in store to slow response.
	SlowSeriesIndex int

	LastSeriesReq      *storepb.SeriesRequest
	LastLabelValuesReq *storepb.LabelValuesRequest
	LastLabelNamesReq  *storepb.LabelNamesRequest
}

func (s *mockedStoreAPI) Info(ctx context.Context, req *storepb.InfoRequest, _ ...grpc.CallOption) (*storepb.InfoResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *mockedStoreAPI) Series(ctx context.Context, req *storepb.SeriesRequest, _ ...grpc.CallOption) (storepb.Store_SeriesClient, error) {
	s.LastSeriesReq = req

	return &StoreSeriesClient{ctx: ctx, respSet: s.RespSeries, respDur: s.RespDuration, slowSeriesIndex: s.SlowSeriesIndex}, s.RespError
}

func (s *mockedStoreAPI) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest, _ ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	s.LastLabelNamesReq = req

	return s.RespLabelNames, s.RespError
}

func (s *mockedStoreAPI) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest, _ ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	s.LastLabelValuesReq = req

	return s.RespLabelValues, s.RespError
}

// StoreSeriesClient is test gRPC storeAPI series client.
type StoreSeriesClient struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesClient
	ctx             context.Context
	i               int
	respSet         []*storepb.SeriesResponse
	respDur         time.Duration
	slowSeriesIndex int
}

func (c *StoreSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	if c.respDur != 0 && (c.slowSeriesIndex == c.i || c.slowSeriesIndex == 0) {
		time.Sleep(c.respDur)
	}

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
func storeSeriesResponse(t testing.TB, lset labels.Labels, smplChunks ...[]sample) *storepb.SeriesResponse {
	var s storepb.Series

	for _, l := range lset {
		s.Labels = append(s.Labels, storepb.Label{Name: l.Name, Value: l.Value})
	}

	for _, smpls := range smplChunks {
		c := chunkenc.NewXORChunk()
		a, err := c.Appender()
		testutil.Ok(t, err)

		for _, smpl := range smpls {
			a.Append(smpl.t, smpl.v)
		}

		ch := storepb.AggrChunk{
			MinTime: smpls[0].t,
			MaxTime: smpls[len(smpls)-1].t,
			Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: c.Bytes()},
		}

		s.Chunks = append(s.Chunks, ch)
	}
	return storepb.NewSeriesResponse(&s)
}

func TestMergeLabels(t *testing.T) {
	ls := []storepb.Label{{Name: "a", Value: "b"}, {Name: "b", Value: "c"}}
	selector := labels.Labels{{Name: "a", Value: "c"}, {Name: "c", Value: "d"}}
	expected := labels.Labels{{Name: "a", Value: "c"}, {Name: "b", Value: "c"}, {Name: "c", Value: "d"}}

	res := mergeLabels(ls, selector)
	resLabels := storepb.LabelsToPromLabels(res)
	sort.Sort(expected)
	sort.Sort(resLabels)

	testutil.Equals(t, expected, resLabels)
}
