// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
	"github.com/thanos-io/thanos/pkg/testutil"
)

type testClient struct {
	// Just to pass interface check.
	storepb.StoreClient

	labelSets []storepb.LabelSet
	minTime   int64
	maxTime   int64
}

func (c testClient) LabelSets() []storepb.LabelSet {
	return c.labelSets
}

func (c testClient) TimeRange() (int64, int64) {
	return c.minTime, c.maxTime
}

func (c testClient) String() string {
	return "test"
}

func (c testClient) Addr() string {
	return "testaddr"
}

func TestProxyStore_Info(t *testing.T) {
	defer testutil.TolerantVerifyLeak(t)

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
	testutil.Equals(t, int64(0), resp.MaxTime)
}

func TestProxyStore_Series(t *testing.T) {
	defer testutil.TolerantVerifyLeak(t)

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
					chunks: [][]sample{{{0, 0}, {2, 1}, {3, 2}}, {{4, 3}}, {{5, 4}}},
				},
				{
					lset:   []storepb.Label{{Name: "a", Value: "b"}},
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}, {{2, 2}, {3, 3}, {4, 4}}},
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
					chunks: [][]sample{{{1, 11}, {2, 22}, {3, 33}}, {{1, 1}, {2, 2}, {3, 3}}},
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

	defer testutil.TolerantVerifyLeak(t)

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
			title: "partial response disabled; 1st errors out after some delay; 2nd store is fast",
			storeAPIs: []Client{
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration:       2 * time.Second,
						SlowSeriesIndex:    1,
						injectedError:      errors.New("test"),
						injectedErrorIndex: 1,
					},
					labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext", Value: "1"}}}},
					minTime:   1,
					maxTime:   300,
				},
				&testClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),

							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),

							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{4, 1}, {5, 2}, {6, 3}}),
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
			expectedErr: errors.New("test: receive series from test: test"),
		},
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
	defer testutil.TolerantVerifyLeak(t)

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
	defer testutil.TolerantVerifyLeak(t)

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
	defer testutil.TolerantVerifyLeak(t)

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
		Start:                   timestamp.FromTime(minTime),
		End:                     timestamp.FromTime(maxTime),
	}
	resp, err := q.LabelValues(ctx, req)
	testutil.Ok(t, err)
	testutil.Assert(t, proto.Equal(req, m1.LastLabelValuesReq), "request was not proxied properly to underlying storeAPI: %s vs %s", req, m1.LastLabelValuesReq)

	testutil.Equals(t, []string{"1", "2", "3", "4"}, resp.Values)
	testutil.Equals(t, 1, len(resp.Warnings))
}

func TestProxyStore_LabelNames(t *testing.T) {
	defer testutil.TolerantVerifyLeak(t)

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
				Start:                   timestamp.FromTime(minTime),
				End:                     timestamp.FromTime(maxTime),
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
				Start:                   timestamp.FromTime(minTime),
				End:                     timestamp.FromTime(maxTime),
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
				Start:                   timestamp.FromTime(minTime),
				End:                     timestamp.FromTime(maxTime),
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

func TestProxyStore_storeMatch(t *testing.T) {
	defer testutil.TolerantVerifyLeak(t)

	storeAPIs := []Client{
		&testClient{
			StoreClient: &mockedStoreAPI{
				RespSeries: []*storepb.SeriesResponse{
					storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{4, 3}}, []sample{{0, 0}, {2, 1}, {3, 2}}),
				},
			},
			minTime: 1,
			maxTime: 300,
		},
	}
	req := &storepb.SeriesRequest{
		MinTime:  1,
		MaxTime:  300,
		Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
	}
	expectedSeries := []rawSeries{
		{
			lset:   []storepb.Label{{Name: "a", Value: "a"}},
			chunks: [][]sample{{{4, 3}}, {{0, 0}, {2, 1}, {3, 2}}}, // No sort merge.
		},
	}
	q := NewProxyStore(nil,
		nil,
		func() []Client { return storeAPIs },
		component.Query,
		nil,
		0*time.Second,
	)

	ctx := context.Background()
	s := newStoreSeriesServer(ctx)

	err := q.Series(req, s)
	testutil.Ok(t, err)
	seriesEquals(t, expectedSeries, s.SeriesSet)

	matcher := [][]storepb.LabelMatcher{{storepb.LabelMatcher{Type: storepb.LabelMatcher_EQ, Name: "__address__", Value: "nothisone"}}}
	s = newStoreSeriesServer(context.WithValue(ctx, StoreMatcherKey, matcher))

	err = q.Series(req, s)
	testutil.Ok(t, err)
	testutil.Equals(t, s.Warnings[0], "No StoreAPIs matched for this query")

	matcher = [][]storepb.LabelMatcher{{storepb.LabelMatcher{Type: storepb.LabelMatcher_EQ, Name: "__address__", Value: "testaddr"}}}
	s = newStoreSeriesServer(context.WithValue(ctx, StoreMatcherKey, matcher))

	err = q.Series(req, s)
	testutil.Ok(t, err)
	seriesEquals(t, expectedSeries, s.SeriesSet)

}

type rawSeries struct {
	lset   []storepb.Label
	chunks [][]sample
}

func seriesEquals(t *testing.T, expected []rawSeries, got []storepb.Series) {
	testutil.Equals(t, len(expected), len(got), "got unexpected number of series: \n %v", got)

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
		ok, err := storeMatches(c.s, c.mint, c.maxt, nil, c.ms...)
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
	HintsSet  []*types.Any

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

	if r.GetSeries() != nil {
		s.SeriesSet = append(s.SeriesSet, *r.GetSeries())
		return nil
	}

	if r.GetHints() != nil {
		s.HintsSet = append(s.HintsSet, r.GetHints())
		return nil
	}

	// Unsupported field, skip.
	return nil
}

func (s *storeSeriesServer) Context() context.Context {
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

	// injectedError will be injected into Recv() if not nil.
	injectedError      error
	injectedErrorIndex int
}

func (s *mockedStoreAPI) Info(context.Context, *storepb.InfoRequest, ...grpc.CallOption) (*storepb.InfoResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *mockedStoreAPI) Series(ctx context.Context, req *storepb.SeriesRequest, _ ...grpc.CallOption) (storepb.Store_SeriesClient, error) {
	s.LastSeriesReq = req

	return &StoreSeriesClient{injectedErrorIndex: s.injectedErrorIndex, injectedError: s.injectedError, ctx: ctx, respSet: s.RespSeries, respDur: s.RespDuration, slowSeriesIndex: s.SlowSeriesIndex}, s.RespError
}

func (s *mockedStoreAPI) LabelNames(_ context.Context, req *storepb.LabelNamesRequest, _ ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	s.LastLabelNamesReq = req

	return s.RespLabelNames, s.RespError
}

func (s *mockedStoreAPI) LabelValues(_ context.Context, req *storepb.LabelValuesRequest, _ ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
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

	injectedError      error
	injectedErrorIndex int
}

func (c *StoreSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	if c.respDur != 0 && (c.slowSeriesIndex == c.i || c.slowSeriesIndex == 0) {
		time.Sleep(c.respDur)
	}
	if c.injectedError != nil && (c.injectedErrorIndex == c.i || c.injectedErrorIndex == 0) {
		return nil, c.injectedError
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

func TestProxySeries(t *testing.T) {
	tb := testutil.NewTB(t)
	storetestutil.RunSeriesInterestingCases(tb, 200e3, 200e3, func(t testutil.TB, samplesPerSeries, series int) {
		benchProxySeries(t, samplesPerSeries, series)
	})
}

func BenchmarkProxySeries(b *testing.B) {
	tb := testutil.NewTB(b)
	storetestutil.RunSeriesInterestingCases(tb, 10e6, 10e5, func(t testutil.TB, samplesPerSeries, series int) {
		benchProxySeries(t, samplesPerSeries, series)
	})
}

func benchProxySeries(t testutil.TB, totalSamples, totalSeries int) {
	tmpDir, err := ioutil.TempDir("", "testorbench-proxyseries")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	const numOfClients = 4

	samplesPerSeriesPerClient := totalSamples / numOfClients
	if samplesPerSeriesPerClient == 0 {
		samplesPerSeriesPerClient = 1
	}
	seriesPerClient := totalSeries / numOfClients
	if seriesPerClient == 0 {
		seriesPerClient = 1
	}

	random := rand.New(rand.NewSource(120))
	clients := make([]Client, numOfClients)
	for j := range clients {
		var resps []*storepb.SeriesResponse

		head, created := storetestutil.CreateHeadWithSeries(t, j, storetestutil.HeadGenOptions{
			TSDBDir:          filepath.Join(tmpDir, fmt.Sprintf("%d", j)),
			SamplesPerSeries: samplesPerSeriesPerClient,
			Series:           seriesPerClient,
			Random:           random,
			SkipChunks:       t.IsBenchmark(),
		})
		testutil.Ok(t, head.Close())

		for i := 0; i < len(created); i++ {
			resps = append(resps, storepb.NewSeriesResponse(created[i]))
		}

		clients[j] = &testClient{
			StoreClient: &mockedStoreAPI{
				RespSeries: resps,
			},
			minTime: math.MinInt64,
			maxTime: math.MaxInt64,
		}
	}

	logger := log.NewNopLogger()
	store := &ProxyStore{
		logger:          logger,
		stores:          func() []Client { return clients },
		metrics:         newProxyStoreMetrics(nil),
		responseTimeout: 0,
	}

	var allResps []*storepb.SeriesResponse
	var expected []*storepb.Series
	lastLabels := storepb.Series{}
	for _, c := range clients {
		m := c.(*testClient).StoreClient.(*mockedStoreAPI)

		// NOTE: Proxy will merge all series with same labels without any frame limit (https://github.com/thanos-io/thanos/issues/2332).
		for _, r := range m.RespSeries {
			allResps = append(allResps, r)

			x := storepb.Series{Labels: r.GetSeries().Labels}
			if x.String() == lastLabels.String() {
				expected[len(expected)-1].Chunks = append(expected[len(expected)-1].Chunks, r.GetSeries().Chunks...)
				continue
			}
			lastLabels = x
			expected = append(expected, r.GetSeries())
		}

	}

	chunkLen := len(allResps[len(allResps)-1].GetSeries().Chunks)
	maxTime := allResps[len(allResps)-1].GetSeries().Chunks[chunkLen-1].MaxTime
	storetestutil.TestServerSeries(t, store,
		&storetestutil.SeriesCase{
			Name: fmt.Sprintf("%d client with %d samples, %d series each", numOfClients, samplesPerSeriesPerClient, seriesPerClient),
			Req: &storepb.SeriesRequest{
				MinTime: 0,
				MaxTime: maxTime,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
			},
			ExpectedSeries: expected,
		},
	)

	// Change client to just one.
	store.stores = func() []Client {
		return []Client{&testClient{
			StoreClient: &mockedStoreAPI{
				// All responses.
				RespSeries: allResps,
			},
			labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext1", Value: "1"}}}},
			minTime:   math.MinInt64,
			maxTime:   math.MaxInt64,
		}}
	}

	// In this we expect exactly the same response as input.
	expected = expected[:0]
	for _, r := range allResps {
		expected = append(expected, r.GetSeries())
	}
	storetestutil.TestServerSeries(t, store,
		&storetestutil.SeriesCase{
			Name: fmt.Sprintf("single client with %d samples, %d series", totalSamples, totalSeries),
			Req: &storepb.SeriesRequest{
				MinTime: 0,
				MaxTime: maxTime,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
			},
			ExpectedSeries: expected,
		},
	)
}

func TestProxyStore_NotLeakingOnPrematureFinish(t *testing.T) {
	defer testutil.TolerantVerifyLeak(t)

	clients := []Client{
		&testClient{
			StoreClient: &mockedStoreAPI{
				RespSeries: []*storepb.SeriesResponse{
					// Ensure more than 10 (internal respCh channel).
					storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("a", "c"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("a", "d"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("a", "e"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("a", "f"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("a", "g"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("a", "h"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("a", "i"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("a", "j"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				},
			},
			minTime: math.MinInt64,
			maxTime: math.MaxInt64,
		},
		&testClient{
			StoreClient: &mockedStoreAPI{
				RespSeries: []*storepb.SeriesResponse{
					storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("b", "b"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("b", "d"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("b", "e"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("b", "f"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("b", "g"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("b", "h"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("b", "i"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storeSeriesResponse(t, labels.FromStrings("b", "j"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				},
			},
			minTime: math.MinInt64,
			maxTime: math.MaxInt64,
		},
	}

	logger := log.NewNopLogger()
	p := &ProxyStore{
		logger:          logger,
		stores:          func() []Client { return clients },
		metrics:         newProxyStoreMetrics(nil),
		responseTimeout: 0,
	}

	t.Run("failling send", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		// We mimic failing series server, but practically context cancel will do the same.
		testutil.NotOk(t, p.Series(&storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{}}, PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT}, &mockedSeriesServer{
			ctx: ctx,
			send: func(*storepb.SeriesResponse) error {
				cancel()
				return ctx.Err()
			},
		}))
		testutil.NotOk(t, ctx.Err())
	})
}

func TestProxyStore_allowListMatch(t *testing.T) {
	c := testClient{}

	lm := storepb.LabelMatcher{Type: storepb.LabelMatcher_EQ, Name: "__address__", Value: "testaddr"}
	matcher := [][]storepb.LabelMatcher{{lm}}
	match, err := storeMatchMetadata(c, matcher)
	testutil.Ok(t, err)
	testutil.Assert(t, match)

	lm = storepb.LabelMatcher{Type: storepb.LabelMatcher_EQ, Name: "__address__", Value: "wrong"}
	matcher = [][]storepb.LabelMatcher{{lm}}
	match, err = storeMatchMetadata(c, matcher)
	testutil.Ok(t, err)
	testutil.Assert(t, !match)

	matcher = [][]storepb.LabelMatcher{{}}
	match, err = storeMatchMetadata(c, matcher)
	testutil.Ok(t, err)
	testutil.Assert(t, match)
}
