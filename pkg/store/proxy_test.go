// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
)

type mockedSeriesServer struct {
	storepb.Store_SeriesServer
	ctx context.Context

	send func(*storepb.SeriesResponse) error
}

func (s *mockedSeriesServer) Send(r *storepb.SeriesResponse) error {
	return s.send(r)
}
func (s *mockedSeriesServer) Context() context.Context { return s.ctx }

type mockedStartTimeDB struct {
	*tsdb.DBReadOnly
	startTime int64
}

func (db *mockedStartTimeDB) StartTime() (int64, error) { return db.startTime, nil }

func TestProxyStore_Info(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := NewProxyStore(nil,
		nil,
		func() []Client { return nil },
		component.Query,
		nil, 0*time.Second, RetrievalStrategy(EagerRetrieval),
	)

	resp, err := q.Info(ctx, &storepb.InfoRequest{})
	testutil.Ok(t, err)
	testutil.Equals(t, []labelpb.ZLabelSet(nil), resp.LabelSets)
	testutil.Equals(t, storepb.StoreType_QUERY, resp.StoreType)
	testutil.Equals(t, int64(0), resp.MinTime)
	testutil.Equals(t, int64(0), resp.MaxTime)
}

func TestProxyStore_Series(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	for _, tc := range []struct {
		title          string
		storeAPIs      []Client
		selectorLabels labels.Labels

		req                *storepb.SeriesRequest
		storeDebugMatchers [][]*labels.Matcher

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
			expectedWarningsLen: 0, // No store matched for this query.
		},
		{
			title: "no storeAPI available for 301-302 time range",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  301,
				MaxTime:  400,
				Matchers: []storepb.LabelMatcher{{Name: "a", Value: "a", Type: storepb.LabelMatcher_EQ}},
			},
			expectedWarningsLen: 0, // No store matched for this query.
		},
		{
			title: "storeAPI available for time range; no series for ext=2 external label matcher",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "2", Type: storepb.LabelMatcher_EQ}},
			},
			expectedWarningsLen: 0, // No store matched for this query.
		},
		{
			title: "storeAPI available for time range; available series for ext=1 external label matcher",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{0, 0}, {2, 1}, {3, 2}}},
				},
			},
		},
		{
			title: "storeAPI available for time range; available series for any external label matcher",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{4, 3}}, []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{0, 0}, {2, 1}, {3, 2}}, {{4, 3}}},
				},
			},
		},
		{
			title: "storeAPI available for time range; available series for any external label matcher, but selector blocks",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
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
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
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
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{0, 0}, {2, 1}, {3, 2}}},
				},
			},
		},
		{
			title: "complex scenario with storeAPIs warnings",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}, []sample{{4, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{5, 4}}), // Continuations of the same series.
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{2, 2}, {3, 3}, {4, 4}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
						},
					},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "c"), []sample{{100, 1}, {300, 3}, {400, 4}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "outside"), []sample{{1, 1}}),
						},
					},
					// Outside range for store itself.
					MinTime: 301,
					MaxTime: 302,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{0, 0}, {2, 1}, {3, 2}}, {{4, 3}}, {{5, 4}}},
				},
				{
					lset:   labels.FromStrings("a", "b"),
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}, {{2, 2}, {3, 3}, {4, 4}}},
				},
				{
					lset:   labels.FromStrings("a", "c"),
					chunks: [][]sample{{{100, 1}, {300, 3}, {400, 4}}},
				},
			},
			expectedWarningsLen: 2,
		},
		{
			title: "storeAPI available for time range; available two duplicated series for ext=1 external label matcher from 2 storeAPIs",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{1, 4}, {2, 5}, {3, 6}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{0, 0}, {2, 1}, {3, 2}}, {{1, 4}, {2, 5}, {3, 6}}},
				},
			},
		},
		{
			title: "storeAPI available for time range; available a few duplicated series for ext=1 external label matcher, mixed storeAPIs",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "1", "w", "1"), []sample{{5, 5}, {7, 7}}),
							storeSeriesResponse(t, labels.FromStrings("a", "1", "w", "1"), []sample{{0, 0}, {2, 1}, {3, 2}}),
							storeSeriesResponse(t, labels.FromStrings("a", "1", "w", "1"), []sample{{5, 5}, {6, 6}, {7, 7}}),
							storeSeriesResponse(t, labels.FromStrings("a", "1", "x", "1"), []sample{{2, 2}, {3, 3}, {4, 4}}, []sample{{1, 1}, {2, 2}, {3, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "1", "x", "1"), []sample{{100, 1}, {300, 3}, {400, 4}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "1", "w", "1"), []sample{{2, 1}}),
							storeSeriesResponse(t, labels.FromStrings("a", "1", "w", "1"), []sample{{5, 5}, {6, 6}, {7, 7}}),
							storeSeriesResponse(t, labels.FromStrings("a", "1", "x", "2"), []sample{{10, 10}, {30, 30}, {40, 40}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "1", "w", "1"),
					chunks: [][]sample{{{0, 0}, {2, 1}, {3, 2}}, {{2, 1}}, {{5, 5}, {6, 6}, {7, 7}}, {{5, 5}, {7, 7}}},
				},
				{
					lset:   labels.FromStrings("a", "1", "x", "1"),
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}, {{2, 2}, {3, 3}, {4, 4}}, {{100, 1}, {300, 3}, {400, 4}}},
				},
				{
					lset:   labels.FromStrings("a", "1", "x", "2"),
					chunks: [][]sample{{{10, 10}, {30, 30}, {40, 40}}},
				},
			},
		},
		{
			title: "same external labels are validated during upload and on querier storeset, proxy does not care",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 11}, {2, 22}, {3, 33}}),
						},
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "b"),
					chunks: [][]sample{{{1, 11}, {2, 22}, {3, 33}}, {{1, 1}, {2, 2}, {3, 3}}},
				},
			},
		},
		{
			title: "partial response enabled",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespError: errors.New("error!"),
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "b"),
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
			expectedWarningsLen: 2,
		},
		{
			title: "partial response disabled",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespError: errors.New("error!"),
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:                 1,
				MaxTime:                 300,
				Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
				PartialResponseDisabled: true,
				PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
			},
			expectedErr: errors.New("fetch series for {ext=\"1\"} : error!"),
		},
		{
			title: "storeAPI available for time range; available series for ext=1 external label matcher; allowed by store debug matcher",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					Name:    "testaddr",
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			storeDebugMatchers: [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "__address__", "testaddr")}},

			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{0, 0}, {2, 1}, {3, 2}}},
				},
			},
		},
		{
			title: "storeAPI available for time range; available series for ext=1 external label matcher; blocked by store debug matcher.",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					Name:    "testaddr",
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			storeDebugMatchers:  [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "__address__", "foo")}},
			expectedWarningsLen: 0, // No stores match.
		},
		{
			title: "sharded series response",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{0, 0}, {2, 1}, {3, 2}}),
							storeSeriesResponse(t, labels.FromStrings("a", "c"), []sample{{0, 0}, {2, 1}, {3, 2}}),
						},
					},
					MinTime: 1,
					MaxTime: 300,
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
				ShardInfo: &storepb.ShardInfo{
					ShardIndex:  0,
					TotalShards: 2,
					By:          true,
					Labels:      []string{"a"},
				},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{0, 0}, {2, 1}, {3, 2}}},
				},
				{
					lset:   labels.FromStrings("a", "b"),
					chunks: [][]sample{{{0, 0}, {2, 1}, {3, 2}}},
				},
			},
		},
	} {
		t.Run(tc.title, func(t *testing.T) {
			for _, replicaLabelSupport := range []bool{false, true} {
				t.Run(fmt.Sprintf("replica_support=%v", replicaLabelSupport), func(t *testing.T) {
					for _, s := range tc.storeAPIs {
						cl := s.(*storetestutil.TestClient)
						cl.WithoutReplicaLabelsEnabled = replicaLabelSupport
					}
					for _, strategy := range []RetrievalStrategy{EagerRetrieval, LazyRetrieval} {
						t.Run(string(strategy), func(t *testing.T) {
							q := NewProxyStore(nil,
								nil,
								func() []Client { return tc.storeAPIs },
								component.Query,
								tc.selectorLabels,
								5*time.Second, strategy,
							)

							ctx := context.Background()
							if len(tc.storeDebugMatchers) > 0 {
								ctx = context.WithValue(ctx, StoreMatcherKey, tc.storeDebugMatchers)
							}

							s := newStoreSeriesServer(ctx)
							err := q.Series(tc.req, s)
							if tc.expectedErr != nil {
								testutil.NotOk(t, err)
								testutil.Equals(t, tc.expectedErr.Error(), err.Error())
								return
							}
							testutil.Ok(t, err)

							seriesEquals(t, tc.expectedSeries, s.SeriesSet)
							testutil.Equals(t, tc.expectedWarningsLen, len(s.Warnings), "got %v warnings", s.Warnings)
						})
					}
				})
			}
		})
	}
}

func TestProxyStore_SeriesSlowStores(t *testing.T) {
	enable := os.Getenv("THANOS_ENABLE_STORE_READ_TIMEOUT_TESTS")
	if enable == "" {
		t.Skip("enable THANOS_ENABLE_STORE_READ_TIMEOUT_TESTS to run store-read-timeout tests")
	}

	defer custom.TolerantVerifyLeak(t)

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
				&storetestutil.TestClient{
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
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
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
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:                 1,
				MaxTime:                 300,
				Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
				PartialResponseDisabled: true,
				PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
			},
			expectedErr: errors.New(`rpc error: code = Aborted desc = warning`),
		},
		{
			title: "partial response disabled; 1st store is slow, 2nd store is fast;",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration: 10 * time.Second,
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:                 1,
				MaxTime:                 300,
				Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
				PartialResponseDisabled: true,
				PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
			},
			expectedErr: errors.New("rpc error: code = Aborted desc = failed to receive any data in 4s from test: context canceled"),
		},
		{
			title: "partial response disabled; 1st store is fast, 2nd store is slow;",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration: 10 * time.Second,
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:                 1,
				MaxTime:                 300,
				Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
				PartialResponseDisabled: true,
				PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
			},
			expectedErr: errors.New("rpc error: code = Aborted desc = warning"),
		},
		{
			title: "partial response disabled; 1st store is slow on 2nd series, 2nd store is fast;",
			storeAPIs: []Client{
				&storetestutil.TestClient{
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
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:                 1,
				MaxTime:                 300,
				Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
				PartialResponseDisabled: true,
				PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
			},
			expectedErr: errors.New("rpc error: code = Aborted desc = warning"),
		},
		{
			title: "partial response disabled; 1st store is fast to respond, 2nd store is slow on 2nd series;",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{3, 1}, {4, 2}, {5, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{6, 1}, {7, 2}, {8, 3}}),
						},
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration:    10 * time.Second,
						SlowSeriesIndex: 2,
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:                 1,
				MaxTime:                 300,
				Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
				PartialResponseDisabled: true,
				PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
			},
			expectedErr: errors.New("rpc error: code = Aborted desc = warning"),
		},
		{
			title: "partial response enabled; 1st store is slow to respond, 2nd store is fast;",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration: 10 * time.Second,
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("b", "c"),
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
			expectedWarningsLen: 2,
		},
		{
			title: "partial response enabled; 1st store is fast, 2nd store is slow;",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration: 10 * time.Second,
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "b"),
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
			expectedWarningsLen: 2,
		},
		{
			title: "partial response enabled; 1st store is fast, 2-3 is slow, 4th is fast;",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration: 10 * time.Second,
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("c", "d"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
						RespDuration: 10 * time.Second,
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("d", "f"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "b"),
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
				{
					lset:   labels.FromStrings("d", "f"),
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
			expectedWarningsLen: 4,
		},
		{
			title: "partial response enabled; 1st store is slow on 2nd series, 2nd store is fast",
			storeAPIs: []Client{
				&storetestutil.TestClient{
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
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storepb.NewWarnSeriesResponse(errors.New("warning")),
							storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{1, 1}, {2, 2}, {3, 3}}),
						},
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "b"),
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
				{
					lset:   labels.FromStrings("b", "c"),
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
			expectedWarningsLen: 3,
		},
		{
			title: "partial response disabled; all stores respond 3s",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{7, 1}, {8, 2}, {9, 3}}),
						},
						RespDuration: 3 * time.Second,
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:                 1,
				MaxTime:                 300,
				Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
				PartialResponseDisabled: true,
				PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "b"),
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
			expectedErr: errors.New("rpc error: code = Aborted desc = receive series from test: context deadline exceeded"),
		},
		{
			title: "partial response enabled; all stores respond 3s",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{7, 1}, {8, 2}, {9, 3}}),
						},
						RespDuration: 3 * time.Second,
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespSeries: []*storepb.SeriesResponse{
							storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{1, 1}, {2, 2}, {3, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{4, 1}, {5, 2}, {6, 3}}),
							storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{7, 1}, {8, 2}, {9, 3}}),
						},
						RespDuration: 3 * time.Second,
					},
					ExtLset: []labels.Labels{labels.FromStrings("ext", "1")},
					MinTime: 1,
					MaxTime: 300,
				},
			},
			req: &storepb.SeriesRequest{
				MinTime:  1,
				MaxTime:  300,
				Matchers: []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "b"),
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
				{
					lset:   labels.FromStrings("b", "c"),
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
			expectedWarningsLen: 2,
		},
	} {
		if ok := t.Run(tc.title, func(t *testing.T) {
			for _, strategy := range []RetrievalStrategy{EagerRetrieval, LazyRetrieval} {
				if ok := t.Run(string(strategy), func(t *testing.T) {
					q := NewProxyStore(nil,
						nil,
						func() []Client { return tc.storeAPIs },
						component.Query,
						tc.selectorLabels,
						4*time.Second, strategy,
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
		}); !ok {
			return
		}
	}

	// Wait until the last goroutine exits which is stuck on time.Sleep().
	// Otherwise, goleak complains.
	time.Sleep(5 * time.Second)
}

func TestProxyStore_Series_RequestParamsProxied(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	m := &mockedStoreAPI{
		RespSeries: []*storepb.SeriesResponse{
			storepb.NewWarnSeriesResponse(errors.New("warning")),
		},
	}
	cls := []Client{
		&storetestutil.TestClient{
			StoreClient: m,
			ExtLset:     []labels.Labels{labels.FromStrings("ext", "1")},
			MinTime:     1,
			MaxTime:     300,
		},
	}
	q := NewProxyStore(nil,
		nil,
		func() []Client { return cls },
		component.Query,
		nil,
		1*time.Second, EagerRetrieval,
	)

	ctx := context.Background()
	s := newStoreSeriesServer(ctx)

	req := &storepb.SeriesRequest{
		MinTime:                 1,
		MaxTime:                 300,
		Matchers:                []storepb.LabelMatcher{{Name: "ext", Value: "1", Type: storepb.LabelMatcher_EQ}},
		PartialResponseDisabled: false,
		Aggregates: []storepb.Aggr{
			storepb.Aggr_COUNTER,
			storepb.Aggr_COUNT,
		},
		PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
		MaxResolutionWindow:     1234,
	}
	testutil.Ok(t, q.Series(req, s))

	testutil.Assert(t, proto.Equal(req, m.LastSeriesReq), "request was not proxied properly to underlying storeAPI: %s vs %s", req, m.LastSeriesReq)
}

func TestProxyStore_Series_RegressionFillResponseChannel(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	var cls []Client
	for i := 0; i < 10; i++ {
		cls = append(cls, &storetestutil.TestClient{
			StoreClient: &mockedStoreAPI{
				RespError: errors.New("test error"),
			},
			MinTime: 1,
			MaxTime: 300,
		})
		cls = append(cls, &storetestutil.TestClient{
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
			MinTime: 1,
			MaxTime: 300,
		})

	}

	q := NewProxyStore(nil,
		nil,
		func() []Client { return cls },
		component.Query,
		labels.FromStrings("fed", "a"),
		5*time.Second, EagerRetrieval,
	)

	ctx := context.Background()
	s := newStoreSeriesServer(ctx)

	testutil.Ok(t, q.Series(
		&storepb.SeriesRequest{
			MinTime:                 1,
			MaxTime:                 300,
			Matchers:                []storepb.LabelMatcher{{Name: "any", Value: ".*", Type: storepb.LabelMatcher_RE}},
			PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
		}, s,
	))
	testutil.Equals(t, 0, len(s.SeriesSet))
	testutil.Equals(t, 110, len(s.Warnings))
}

func TestProxyStore_LabelValues(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	m1 := &mockedStoreAPI{
		RespLabelValues: &storepb.LabelValuesResponse{
			Values:   []string{"1", "2"},
			Warnings: []string{"warning"},
		},
	}
	cls := []Client{
		&storetestutil.TestClient{StoreClient: m1},
		&storetestutil.TestClient{StoreClient: &mockedStoreAPI{
			RespLabelValues: &storepb.LabelValuesResponse{
				Values: []string{"3", "4"},
			},
		}},
		&storetestutil.TestClient{StoreClient: &mockedStoreAPI{
			RespLabelValues: &storepb.LabelValuesResponse{
				Values: []string{"5", "6"},
			}},
			MinTime: timestamp.FromTime(time.Now().Add(-1 * time.Minute)),
			MaxTime: timestamp.FromTime(time.Now()),
		},
	}
	q := NewProxyStore(nil,
		nil,
		func() []Client { return cls },
		component.Query,
		nil,
		0*time.Second, EagerRetrieval,
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

	testutil.Equals(t, []string{"1", "2", "3", "4", "5", "6"}, resp.Values)
	testutil.Equals(t, 1, len(resp.Warnings))

	// Request outside the time range of the last store client.
	req = &storepb.LabelValuesRequest{
		Label:                   "a",
		PartialResponseDisabled: true,
		Start:                   timestamp.FromTime(minTime),
		End:                     timestamp.FromTime(time.Now().Add(-1 * time.Hour)),
	}
	resp, err = q.LabelValues(ctx, req)
	testutil.Ok(t, err)
	testutil.Assert(t, proto.Equal(req, m1.LastLabelValuesReq), "request was not proxied properly to underlying storeAPI: %s vs %s", req, m1.LastLabelValuesReq)

	testutil.Equals(t, []string{"1", "2", "3", "4"}, resp.Values)
	testutil.Equals(t, 1, len(resp.Warnings))
}

func TestProxyStore_LabelNames(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	for _, tc := range []struct {
		title     string
		storeAPIs []Client

		req                *storepb.LabelNamesRequest
		storeDebugMatchers [][]*labels.Matcher

		expectedNames       []string
		expectedErr         error
		expectedWarningsLen int
	}{
		{
			title: "label_names partial response disabled",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespLabelNames: &storepb.LabelNamesResponse{
							Names: []string{"a", "b"},
						},
					},
				},
				&storetestutil.TestClient{
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
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespLabelNames: &storepb.LabelNamesResponse{
							Names: []string{"a", "b"},
						},
					},
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespError: errors.New("error!"),
					},
					Name: "test",
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
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespLabelNames: &storepb.LabelNamesResponse{
							Names: []string{"a", "b"},
						},
					},
				},
				&storetestutil.TestClient{
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
		{
			title: "stores filtered by time range",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespLabelNames: &storepb.LabelNamesResponse{
							Names: []string{"a", "b"},
						},
					},
					MinTime: timestamp.FromTime(time.Now().Add(-4 * time.Hour)),
					MaxTime: timestamp.FromTime(time.Now().Add(-3 * time.Hour)),
				},
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespLabelNames: &storepb.LabelNamesResponse{
							Names: []string{"c", "d"},
						},
					},
					MinTime: timestamp.FromTime(time.Now().Add(-2 * time.Hour)),
					MaxTime: timestamp.FromTime(time.Now().Add(-1 * time.Hour)),
				},
			},
			req: &storepb.LabelNamesRequest{
				Start:                   timestamp.FromTime(time.Now().Add(-1 * time.Minute)),
				End:                     timestamp.FromTime(time.Now()),
				PartialResponseDisabled: false,
			},
			expectedNames:       nil,
			expectedWarningsLen: 0,
		},
		{
			title: "store matchers blocks",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespLabelNames: &storepb.LabelNamesResponse{
							Names: []string{"a", "b"},
						},
					},
					Name: "testaddr",
				},
			},
			req: &storepb.LabelNamesRequest{
				Start:                   timestamp.FromTime(minTime),
				End:                     timestamp.FromTime(maxTime),
				PartialResponseDisabled: false,
			},
			storeDebugMatchers:  [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "__address__", "foo")}},
			expectedNames:       nil,
			expectedWarningsLen: 0,
		},
		{
			title: "store matchers allows",
			storeAPIs: []Client{
				&storetestutil.TestClient{
					StoreClient: &mockedStoreAPI{
						RespLabelNames: &storepb.LabelNamesResponse{
							Names: []string{"a", "b"},
						},
					},
					Name: "testaddr",
				},
			},
			req: &storepb.LabelNamesRequest{
				Start:                   timestamp.FromTime(minTime),
				End:                     timestamp.FromTime(maxTime),
				PartialResponseDisabled: false,
			},
			storeDebugMatchers:  [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "__address__", "testaddr")}},
			expectedNames:       []string{"a", "b"},
			expectedWarningsLen: 0,
		},
	} {
		if ok := t.Run(tc.title, func(t *testing.T) {
			q := NewProxyStore(
				nil,
				nil,
				func() []Client { return tc.storeAPIs },
				component.Query,
				nil,
				5*time.Second, EagerRetrieval,
			)

			ctx := context.Background()
			if len(tc.storeDebugMatchers) > 0 {
				ctx = context.WithValue(ctx, StoreMatcherKey, tc.storeDebugMatchers)
			}
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
	lset   labels.Labels
	chunks [][]sample
}

func seriesEquals(t *testing.T, expected []rawSeries, got []storepb.Series) {
	testutil.Equals(t, len(expected), len(got), "got unexpected number of series: \n %v", got)

	ret := make([]rawSeries, len(got))
	for i, s := range got {
		r := rawSeries{
			lset: labelpb.ZLabelsToPromLabels(s.Labels),
		}
		for _, chk := range s.Chunks {
			var samples []sample

			c, err := chunkenc.FromData(chunkenc.EncXOR, chk.Raw.Data)
			testutil.Ok(t, err)

			iter := c.Iterator(nil)
			for iter.Next() != chunkenc.ValNone {
				tv, v := iter.At()
				samples = append(samples, sample{tv, v})
			}
			testutil.Ok(t, iter.Err())

			r.chunks = append(r.chunks, samples)
		}
		ret[i] = r
	}

	for i := range ret {
		testutil.Equals(t, expected[i], ret[i])
	}
}

func TestStoreMatches(t *testing.T) {
	for _, c := range []struct {
		s          Client
		mint, maxt int64
		ms         []*labels.Matcher

		expectedMatch  bool
		expectedReason string
	}{
		{
			s: &storetestutil.TestClient{ExtLset: []labels.Labels{labels.FromStrings("a", "b")}},
			ms: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "b", "1"),
			},
			maxt:           -1,
			expectedMatch:  false,
			expectedReason: "does not have data within this time period: [0,-1]. Store time ranges: [0,0]",
		},
		{
			s: &storetestutil.TestClient{ExtLset: []labels.Labels{labels.FromStrings("a", "b")}},
			ms: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "b", "1"),
			},
			maxt:          1,
			expectedMatch: true,
		},
		{
			s:              &storetestutil.TestClient{MinTime: 100, MaxTime: 200},
			mint:           201,
			maxt:           300,
			expectedMatch:  false,
			expectedReason: "does not have data within this time period: [201,300]. Store time ranges: [100,200]",
		},
		{
			s:             &storetestutil.TestClient{MinTime: 100, MaxTime: 200},
			mint:          200,
			maxt:          300,
			expectedMatch: true,
		},
		{
			s:              &storetestutil.TestClient{MinTime: 100, MaxTime: 200},
			mint:           50,
			maxt:           99,
			expectedMatch:  false,
			expectedReason: "does not have data within this time period: [50,99]. Store time ranges: [100,200]",
		},
		{
			s:             &storetestutil.TestClient{MinTime: 100, MaxTime: 200},
			mint:          50,
			maxt:          101,
			expectedMatch: true,
		},
		{
			s: &storetestutil.TestClient{ExtLset: []labels.Labels{labels.FromStrings("a", "b")}},
			ms: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "a", "b"),
			},
			maxt:          1,
			expectedMatch: true,
		},
		{
			s: &storetestutil.TestClient{ExtLset: []labels.Labels{labels.FromStrings("a", "b")}},
			ms: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "a", "c"),
			},
			maxt:           1,
			expectedMatch:  false,
			expectedReason: "external labels [{a=\"b\"}] does not match request label matchers: [a=\"c\"]",
		},
		{
			s: &storetestutil.TestClient{ExtLset: []labels.Labels{labels.FromStrings("a", "b")}},
			ms: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "a", "b|c"),
			},
			maxt:          1,
			expectedMatch: true,
		},
		{
			s: &storetestutil.TestClient{ExtLset: []labels.Labels{labels.FromStrings("a", "b")}},
			ms: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotRegexp, "a", ""),
			},
			maxt:          1,
			expectedMatch: true,
		},
		{
			s: &storetestutil.TestClient{ExtLset: []labels.Labels{
				labels.FromStrings("a", "b"),
				labels.FromStrings("a", "c"),
				labels.FromStrings("a", "d"),
			}},
			ms: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "a", "e"),
			},
			maxt:           1,
			expectedMatch:  false,
			expectedReason: "external labels [{a=\"b\"} {a=\"c\"} {a=\"d\"}] does not match request label matchers: [a=\"e\"]",
		},
		{
			s: &storetestutil.TestClient{ExtLset: []labels.Labels{
				labels.FromStrings("a", "b"),
				labels.FromStrings("a", "c"),
				labels.FromStrings("a", "d"),
			}},
			ms: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "a", "c"),
			},
			maxt:          1,
			expectedMatch: true,
		},
		{
			s: &storetestutil.TestClient{ExtLset: []labels.Labels{
				labels.FromStrings("a", "b"),
				labels.FromStrings("a", "c"),
				labels.FromStrings("a", "d"),
			}},
			ms: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotRegexp, "a", ""),
			},
			maxt:          1,
			expectedMatch: true,
		},
	} {
		t.Run("", func(t *testing.T) {
			ok, reason := storeMatches(context.TODO(), c.s, c.mint, c.maxt, c.ms...)
			testutil.Equals(t, c.expectedMatch, ok)
			testutil.Equals(t, c.expectedReason, reason)

		})
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
		select {
		case <-time.After(c.respDur):
		case <-c.ctx.Done():
			return nil, c.ctx.Err()
		}
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

	s.Labels = append(s.Labels, labelpb.ZLabelsFromPromLabels(lset)...)

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
	tmpDir := t.TempDir()

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

		clients[j] = &storetestutil.TestClient{
			StoreClient: &mockedStoreAPI{
				RespSeries: resps,
			},
			MinTime:                     math.MinInt64,
			MaxTime:                     math.MaxInt64,
			WithoutReplicaLabelsEnabled: true,
		}
	}

	logger := log.NewNopLogger()
	store := &ProxyStore{
		logger:            logger,
		stores:            func() []Client { return clients },
		metrics:           newProxyStoreMetrics(nil),
		responseTimeout:   5 * time.Second,
		retrievalStrategy: EagerRetrieval,
	}

	var allResps []*storepb.SeriesResponse
	var expected []*storepb.Series
	lastLabels := storepb.Series{}
	for _, c := range clients {
		m := c.(*storetestutil.TestClient).StoreClient.(*mockedStoreAPI)

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
	var maxTime int64
	if len(allResps[len(allResps)-1].GetSeries().Chunks) == 0 {
		maxTime = math.MaxInt64
	} else {
		maxTime = allResps[len(allResps)-1].GetSeries().Chunks[chunkLen-1].MaxTime
	}
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

	// Change client to one, containing all series.
	store.stores = func() []Client {
		return []Client{&storetestutil.TestClient{
			StoreClient: &mockedStoreAPI{
				// All responses.
				RespSeries: allResps,
			},
			ExtLset:                     []labels.Labels{labels.FromStrings("ext1", "1")},
			MinTime:                     math.MinInt64,
			MaxTime:                     math.MaxInt64,
			WithoutReplicaLabelsEnabled: true,
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
	defer custom.TolerantVerifyLeak(t)

	clients := []Client{
		&storetestutil.TestClient{
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
			MinTime: math.MinInt64,
			MaxTime: math.MaxInt64,
		},
		&storetestutil.TestClient{
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
			MinTime: math.MinInt64,
			MaxTime: math.MaxInt64,
		},
	}

	logger := log.NewNopLogger()
	p := &ProxyStore{
		logger:            logger,
		stores:            func() []Client { return clients },
		metrics:           newProxyStoreMetrics(nil),
		responseTimeout:   0,
		retrievalStrategy: EagerRetrieval,
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

func TestProxyStore_storeMatchMetadata(t *testing.T) {
	c := storetestutil.TestClient{Name: "testaddr"}
	c.IsLocalStore = true

	ok, reason := storeMatchDebugMetadata(c, [][]*labels.Matcher{{}})
	testutil.Assert(t, !ok)
	testutil.Equals(t, "the store is not remote, cannot match __address__", reason)

	// Change client to remote.
	c.IsLocalStore = false

	ok, reason = storeMatchDebugMetadata(c, [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "__address__", "wrong")}})
	testutil.Assert(t, !ok)
	testutil.Equals(t, "__address__ testaddr does not match debug store metadata matchers: [[__address__=\"wrong\"]]", reason)

	ok, reason = storeMatchDebugMetadata(c, [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "__address__", "testaddr")}})
	testutil.Assert(t, ok)
	testutil.Equals(t, "", reason)
}

func TestDedupRespHeap_Deduplication(t *testing.T) {
	t.Parallel()

	for _, tcase := range []struct {
		responses []*storepb.SeriesResponse
		testFn    func(responses []*storepb.SeriesResponse, h *dedupResponseHeap)
		tname     string
	}{
		{
			tname:     "edge case with zero responses",
			responses: []*storepb.SeriesResponse{},
			testFn: func(responses []*storepb.SeriesResponse, h *dedupResponseHeap) {
				testutil.Equals(t, false, h.Next())

				callAtExpectPanic := func() {
					defer func() {
						testutil.Assert(t, recover() != nil, "expected a panic from At()")
					}()

					h.At()
				}
				callAtExpectPanic()
			},
		},
		{
			tname: "edge case with only one response",
			responses: []*storepb.SeriesResponse{
				{
					Result: &storepb.SeriesResponse_Series{
						Series: &storepb.Series{
							Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("foo", "bar")),
							Chunks: []storepb.AggrChunk{
								{
									Raw: &storepb.Chunk{
										Type: storepb.Chunk_XOR,
										Data: []byte(`abcdefgh`),
									},
								},
							},
						},
					},
				},
			},
			testFn: func(responses []*storepb.SeriesResponse, h *dedupResponseHeap) {
				testutil.Equals(t, true, h.Next())
				resp := h.At()
				testutil.Equals(t, responses[0], resp)
				testutil.Equals(t, false, h.Next())
			},
		},
		{
			tname: "dedups identical series",
			responses: []*storepb.SeriesResponse{
				{
					Result: &storepb.SeriesResponse_Series{
						Series: &storepb.Series{
							Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("foo", "bar")),
							Chunks: []storepb.AggrChunk{
								{
									Raw: &storepb.Chunk{
										Type: storepb.Chunk_XOR,
										Data: []byte(`abcdefgh`),
									},
								},
							},
						},
					},
				},
				{
					Result: &storepb.SeriesResponse_Series{
						Series: &storepb.Series{
							Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("foo", "bar")),
							Chunks: []storepb.AggrChunk{
								{
									Raw: &storepb.Chunk{
										Type: storepb.Chunk_XOR,
										Hash: xxhash.Sum64([]byte(`abcdefgh`)),
										Data: []byte(`abcdefgh`),
									},
								},
							},
						},
					},
				},
			},
			testFn: func(responses []*storepb.SeriesResponse, h *dedupResponseHeap) {
				testutil.Equals(t, true, h.Next())
				resp := h.At()
				testutil.Equals(t, responses[0], resp)
				testutil.Equals(t, false, h.Next())
			},
		},
	} {
		t.Run(tcase.tname, func(t *testing.T) {
			h := NewDedupResponseHeap(NewProxyResponseHeap(
				&eagerRespSet{
					wg:                &sync.WaitGroup{},
					bufferedResponses: tcase.responses,
				},
			))
			tcase.testFn(tcase.responses, h)
		})
	}

}
