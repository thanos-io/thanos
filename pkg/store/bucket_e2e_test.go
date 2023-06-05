// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/storage"
	"google.golang.org/grpc/codes"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/objtesting"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/model"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

var (
	minTime            = time.Unix(0, 0)
	maxTime, _         = time.Parse(time.RFC3339, "9999-12-31T23:59:59Z")
	minTimeDuration    = model.TimeOrDurationValue{Time: &minTime}
	maxTimeDuration    = model.TimeOrDurationValue{Time: &maxTime}
	allowAllFilterConf = &FilterConfig{
		MinTime: minTimeDuration,
		MaxTime: maxTimeDuration,
	}
)

type swappableCache struct {
	ptr storecache.IndexCache
}

func (c *swappableCache) SwapWith(ptr2 storecache.IndexCache) {
	c.ptr = ptr2
}

func (c *swappableCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte) {
	c.ptr.StorePostings(blockID, l, v)
}

func (c *swappableCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label) (map[labels.Label][]byte, []labels.Label) {
	return c.ptr.FetchMultiPostings(ctx, blockID, keys)
}

func (c *swappableCache) StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte) {
	c.ptr.StoreExpandedPostings(blockID, matchers, v)
}

// FetchExpandedPostings fetches expanded postings.
func (c *swappableCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, matchers []*labels.Matcher) ([]byte, bool) {
	return c.ptr.FetchExpandedPostings(ctx, blockID, matchers)
}

func (c *swappableCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte) {
	c.ptr.StoreSeries(blockID, id, v)
}

func (c *swappableCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef) (map[storage.SeriesRef][]byte, []storage.SeriesRef) {
	return c.ptr.FetchMultiSeries(ctx, blockID, ids)
}

type storeSuite struct {
	store            *BucketStore
	minTime, maxTime int64
	cache            *swappableCache

	logger log.Logger
}

func prepareTestBlocks(t testing.TB, now time.Time, count int, dir string, bkt objstore.Bucket,
	series []labels.Labels, extLset labels.Labels) (minTime, maxTime int64) {
	ctx := context.Background()
	logger := log.NewNopLogger()

	for i := 0; i < count; i++ {
		mint := timestamp.FromTime(now)
		now = now.Add(2 * time.Hour)
		maxt := timestamp.FromTime(now)

		if minTime == 0 {
			minTime = mint
		}
		maxTime = maxt

		// Create two blocks per time slot. Only add 10 samples each so only one chunk
		// gets created each. This way we can easily verify we got 10 chunks per series below.
		id1, err := e2eutil.CreateBlock(ctx, dir, series[:4], 10, mint, maxt, extLset, 0, metadata.NoneFunc)
		testutil.Ok(t, err)
		id2, err := e2eutil.CreateBlock(ctx, dir, series[4:], 10, mint, maxt, extLset, 0, metadata.NoneFunc)
		testutil.Ok(t, err)

		dir1, dir2 := filepath.Join(dir, id1.String()), filepath.Join(dir, id2.String())

		// Replace labels to the meta of the second block.
		meta, err := metadata.ReadFromDir(dir2)
		testutil.Ok(t, err)
		meta.Thanos.Labels = map[string]string{"ext2": "value2"}
		testutil.Ok(t, meta.WriteToDir(logger, dir2))

		testutil.Ok(t, block.Upload(ctx, logger, bkt, dir1, metadata.NoneFunc))
		testutil.Ok(t, block.Upload(ctx, logger, bkt, dir2, metadata.NoneFunc))

		testutil.Ok(t, os.RemoveAll(dir1))
		testutil.Ok(t, os.RemoveAll(dir2))
	}

	return
}

func prepareStoreWithTestBlocks(t testing.TB, dir string, bkt objstore.Bucket, manyParts bool, chunksLimiterFactory ChunksLimiterFactory, seriesLimiterFactory SeriesLimiterFactory, bytesLimiterFactory BytesLimiterFactory, relabelConfig []*relabel.Config, filterConf *FilterConfig) *storeSuite {
	series := []labels.Labels{
		labels.FromStrings("a", "1", "b", "1"),
		labels.FromStrings("a", "1", "b", "2"),
		labels.FromStrings("a", "2", "b", "1"),
		labels.FromStrings("a", "2", "b", "2"),
		labels.FromStrings("a", "1", "c", "1"),
		labels.FromStrings("a", "1", "c", "2"),
		labels.FromStrings("a", "2", "c", "1"),
		labels.FromStrings("a", "2", "c", "2"),
	}
	extLset := labels.FromStrings("ext1", "value1")

	minTime, maxTime := prepareTestBlocks(t, time.Now(), 3, dir, bkt, series, extLset)

	s := &storeSuite{
		logger:  log.NewLogfmtLogger(os.Stderr),
		cache:   &swappableCache{},
		minTime: minTime,
		maxTime: maxTime,
	}

	metaFetcher, err := block.NewMetaFetcher(s.logger, 20, objstore.WithNoopInstr(bkt), dir, nil, []block.MetadataFilter{
		block.NewTimePartitionMetaFilter(filterConf.MinTime, filterConf.MaxTime),
		block.NewLabelShardedMetaFilter(relabelConfig),
	})
	testutil.Ok(t, err)

	reg := prometheus.NewRegistry()
	store, err := NewBucketStore(
		objstore.WithNoopInstr(bkt),
		metaFetcher,
		dir,
		chunksLimiterFactory,
		seriesLimiterFactory,
		bytesLimiterFactory,
		NewGapBasedPartitioner(PartitionerMaxGapSize),
		20,
		true,
		DefaultPostingOffsetInMemorySampling,
		true,
		true,
		time.Minute,
		WithLogger(s.logger),
		WithIndexCache(s.cache),
		WithFilterConfig(filterConf),
		WithRegistry(reg),
	)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, store.Close()) }()

	s.store = store

	if manyParts {
		s.store.partitioner = naivePartitioner{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Check if the blocks are being loaded.
	start := time.Now()
	time.Sleep(time.Second * 1)
	testutil.Ok(t, store.SyncBlocks(ctx))

	// Get the value of the metric 'thanos_bucket_store_blocks_last_loaded_timestamp_seconds' to capture the timestamp of the last loaded block.
	m := gatherFamily(t, reg, "thanos_bucket_store_blocks_last_loaded_timestamp_seconds")
	lastUploaded := time.Unix(int64(m.Metric[0].Gauge.GetValue()), 0)

	if lastUploaded.Before(start) {
		t.Fatalf("no blocks are loaded ")
	}

	return s
}

func gatherFamily(t testing.TB, reg prometheus.Gatherer, familyName string) *dto.MetricFamily {

	families, err := reg.Gather()
	testutil.Ok(t, err)

	for _, f := range families {
		if f.GetName() == familyName {
			return f
		}
	}

	t.Fatalf("could not find family %s", familyName)
	return nil
}

func testBucketStore_e2e(t *testing.T, ctx context.Context, s *storeSuite) {
	t.Helper()

	mint, maxt := s.store.TimeRange()
	testutil.Equals(t, s.minTime, mint)
	testutil.Equals(t, s.maxTime, maxt)

	vals, err := s.store.LabelValues(ctx, &storepb.LabelValuesRequest{
		Label: "a",
		Start: timestamp.FromTime(minTime),
		End:   timestamp.FromTime(maxTime),
	})
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"1", "2"}, vals.Values)

	// TODO(bwplotka): Add those test cases to TSDB querier_test.go as well, there are no tests for matching.
	for i, tcase := range []struct {
		req              *storepb.SeriesRequest
		expected         [][]labelpb.ZLabel
		expectedChunkLen int
	}{
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]labelpb.ZLabel{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
				},
				MinTime:              mint,
				MaxTime:              maxt,
				WithoutReplicaLabels: []string{"ext1", "ext2"},
			},
			expectedChunkLen: 3,
			expected: [][]labelpb.ZLabel{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]labelpb.ZLabel{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_NRE, Name: "a", Value: "2"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]labelpb.ZLabel{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_NRE, Name: "a", Value: "not_existing"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]labelpb.ZLabel{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_NRE, Name: "not_existing", Value: "1"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]labelpb.ZLabel{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "2"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]labelpb.ZLabel{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
			},
		},
		{
			// Matching by external label should work as well.
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
					{Type: storepb.LabelMatcher_EQ, Name: "ext2", Value: "value2"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]labelpb.ZLabel{
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
					{Type: storepb.LabelMatcher_EQ, Name: "ext2", Value: "wrong-value"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_NEQ, Name: "a", Value: "2"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]labelpb.ZLabel{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_NEQ, Name: "a", Value: "not_existing"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]labelpb.ZLabel{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
			},
		},
		// Regression https://github.com/thanos-io/thanos/issues/833.
		// Problem: Matcher that was selecting NO series, was ignored instead of passed as emptyPosting to Intersect.
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
					{Type: storepb.LabelMatcher_RE, Name: "non_existing", Value: "something"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
		},
		// Test skip-chunk option.
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
				MinTime:    mint,
				MaxTime:    maxt,
				SkipChunks: true,
			},
			expectedChunkLen: 0,
			expected: [][]labelpb.ZLabel{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
			},
		},
	} {
		if ok := t.Run(fmt.Sprint(i), func(t *testing.T) {
			srv := newStoreSeriesServer(ctx)

			testutil.Ok(t, s.store.Series(tcase.req, srv))
			testutil.Equals(t, len(tcase.expected), len(srv.SeriesSet))

			for i, s := range srv.SeriesSet {
				testutil.Equals(t, tcase.expected[i], s.Labels)
				testutil.Equals(t, tcase.expectedChunkLen, len(s.Chunks))
			}
		}); !ok {
			return
		}
	}
}

func TestBucketStore_e2e(t *testing.T) {
	objtesting.ForeachStore(t, func(t *testing.T, bkt objstore.Bucket) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dir := t.TempDir()

		s := prepareStoreWithTestBlocks(t, dir, bkt, false, NewChunksLimiterFactory(0), NewSeriesLimiterFactory(0), NewBytesLimiterFactory(0), emptyRelabelConfig, allowAllFilterConf)

		if ok := t.Run("no index cache", func(t *testing.T) {
			s.cache.SwapWith(noopCache{})
			testBucketStore_e2e(t, ctx, s)
		}); !ok {
			return
		}

		if ok := t.Run("with large, sufficient index cache", func(t *testing.T) {
			indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(s.logger, nil, storecache.InMemoryIndexCacheConfig{
				MaxItemSize: 1e5,
				MaxSize:     2e5,
			})
			testutil.Ok(t, err)
			s.cache.SwapWith(indexCache)
			testBucketStore_e2e(t, ctx, s)
		}); !ok {
			return
		}

		t.Run("with small index cache", func(t *testing.T) {
			indexCache2, err := storecache.NewInMemoryIndexCacheWithConfig(s.logger, nil, storecache.InMemoryIndexCacheConfig{
				MaxItemSize: 50,
				MaxSize:     100,
			})
			testutil.Ok(t, err)
			s.cache.SwapWith(indexCache2)
			testBucketStore_e2e(t, ctx, s)
		})
	})
}

type naivePartitioner struct{}

func (g naivePartitioner) Partition(length int, rng func(int) (uint64, uint64)) (parts []Part) {
	for i := 0; i < length; i++ {
		s, e := rng(i)
		parts = append(parts, Part{Start: s, End: e, ElemRng: [2]int{i, i + 1}})
	}
	return parts
}

// Naive partitioner splits the array equally (it does not combine anything).
// This tests if our, sometimes concurrent, fetches for different parts works.
// Regression test against: https://github.com/thanos-io/thanos/issues/829.
func TestBucketStore_ManyParts_e2e(t *testing.T) {
	objtesting.ForeachStore(t, func(t *testing.T, bkt objstore.Bucket) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dir := t.TempDir()

		s := prepareStoreWithTestBlocks(t, dir, bkt, true, NewChunksLimiterFactory(0), NewSeriesLimiterFactory(0), NewBytesLimiterFactory(0), emptyRelabelConfig, allowAllFilterConf)

		indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(s.logger, nil, storecache.InMemoryIndexCacheConfig{
			MaxItemSize: 1e5,
			MaxSize:     2e5,
		})
		testutil.Ok(t, err)
		s.cache.SwapWith(indexCache)

		testBucketStore_e2e(t, ctx, s)
	})
}

func TestBucketStore_TimePartitioning_e2e(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bkt := objstore.NewInMemBucket()

	dir := t.TempDir()

	hourAfter := time.Now().Add(1 * time.Hour)
	filterMaxTime := model.TimeOrDurationValue{Time: &hourAfter}

	// The query will fetch 2 series from 2 blocks, so we do expect to hit a total of 4 chunks.
	expectedChunks := uint64(2 * 2)

	s := prepareStoreWithTestBlocks(t, dir, bkt, false, NewChunksLimiterFactory(expectedChunks), NewSeriesLimiterFactory(0), NewBytesLimiterFactory(0), emptyRelabelConfig, &FilterConfig{
		MinTime: minTimeDuration,
		MaxTime: filterMaxTime,
	})
	testutil.Ok(t, s.store.SyncBlocks(ctx))

	mint, maxt := s.store.TimeRange()
	testutil.Equals(t, s.minTime, mint)
	testutil.Equals(t, filterMaxTime.PrometheusTimestamp(), maxt)

	req := &storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
		},
		MinTime: mint,
		MaxTime: timestamp.FromTime(time.Now().AddDate(0, 0, 1)),
	}

	expectedLabels := [][]labelpb.ZLabel{
		{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
		{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
	}

	s.cache.SwapWith(noopCache{})
	srv := newStoreSeriesServer(ctx)

	testutil.Ok(t, s.store.Series(req, srv))
	testutil.Equals(t, len(expectedLabels), len(srv.SeriesSet))

	for i, s := range srv.SeriesSet {
		testutil.Equals(t, expectedLabels[i], s.Labels)

		// prepareTestBlocks makes 3 chunks containing 2 hour data,
		// we should only get 1, as we are filtering by time.
		testutil.Equals(t, 1, len(s.Chunks))
	}
}

func TestBucketStore_Series_ChunksLimiter_e2e(t *testing.T) {
	// The query will fetch 2 series from 6 blocks, so we do expect to hit a total of 12 chunks.
	expectedChunks := uint64(2 * 6)

	cases := map[string]struct {
		maxChunksLimit uint64
		maxSeriesLimit uint64
		maxBytesLimit  int64
		expectedErr    string
		code           codes.Code
	}{
		"should succeed if the max chunks limit is not exceeded": {
			maxChunksLimit: expectedChunks,
		},
		"should fail if the max chunks limit is exceeded - ResourceExhausted": {
			maxChunksLimit: expectedChunks - 1,
			expectedErr:    "exceeded chunks limit",
			code:           codes.ResourceExhausted,
		},
		"should fail if the max series limit is exceeded - ResourceExhausted": {
			maxChunksLimit: expectedChunks,
			expectedErr:    "exceeded series limit",
			maxSeriesLimit: 1,
			code:           codes.ResourceExhausted,
		},
		"should fail if the max bytes limit is exceeded - ResourceExhausted": {
			maxChunksLimit: expectedChunks,
			expectedErr:    "exceeded bytes limit",
			maxSeriesLimit: 2,
			maxBytesLimit:  1,
			code:           codes.ResourceExhausted,
		},
	}

	for testName, testData := range cases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			bkt := objstore.NewInMemBucket()

			dir := t.TempDir()

			s := prepareStoreWithTestBlocks(t, dir, bkt, false, NewChunksLimiterFactory(testData.maxChunksLimit), NewSeriesLimiterFactory(testData.maxSeriesLimit), NewBytesLimiterFactory(units.Base2Bytes(testData.maxBytesLimit)), emptyRelabelConfig, allowAllFilterConf)
			testutil.Ok(t, s.store.SyncBlocks(ctx))

			req := &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
				MinTime: minTimeDuration.PrometheusTimestamp(),
				MaxTime: maxTimeDuration.PrometheusTimestamp(),
			}

			s.cache.SwapWith(noopCache{})
			srv := newStoreSeriesServer(ctx)
			err := s.store.Series(req, srv)

			if testData.expectedErr == "" {
				testutil.Ok(t, err)
			} else {
				testutil.NotOk(t, err)
				testutil.Assert(t, strings.Contains(err.Error(), testData.expectedErr))
				status, ok := status.FromError(err)
				testutil.Equals(t, true, ok)
				testutil.Equals(t, testData.code, status.Code())
			}
		})
	}
}

func TestBucketStore_LabelNames_e2e(t *testing.T) {
	objtesting.ForeachStore(t, func(t *testing.T, bkt objstore.Bucket) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dir := t.TempDir()

		s := prepareStoreWithTestBlocks(t, dir, bkt, false, NewChunksLimiterFactory(0), NewSeriesLimiterFactory(0), NewBytesLimiterFactory(0), emptyRelabelConfig, allowAllFilterConf)
		s.cache.SwapWith(noopCache{})

		mint, maxt := s.store.TimeRange()
		testutil.Equals(t, s.minTime, mint)
		testutil.Equals(t, s.maxTime, maxt)

		for name, tc := range map[string]struct {
			req      *storepb.LabelNamesRequest
			expected []string
		}{
			"basic labelNames": {
				req: &storepb.LabelNamesRequest{
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
				},
				expected: []string{"a", "b", "c", "ext1", "ext2"}, // ext2 is added by the prepareStoreWithTestBlocks function.
			},
			"outside the time range": {
				req: &storepb.LabelNamesRequest{
					Start: timestamp.FromTime(time.Now().Add(-24 * time.Hour)),
					End:   timestamp.FromTime(time.Now().Add(-23 * time.Hour)),
				},
				expected: nil,
			},
			"matcher matching everything": {
				req: &storepb.LabelNamesRequest{
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Matchers: []storepb.LabelMatcher{
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "a",
							Value: "1",
						},
					},
				},
				expected: []string{"a", "b", "c", "ext1", "ext2"},
			},
			"b=1 matcher": {
				req: &storepb.LabelNamesRequest{
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Matchers: []storepb.LabelMatcher{
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "b",
							Value: "1",
						},
					},
				},
				expected: []string{"a", "b", "ext1"},
			},

			"b='' matcher": {
				req: &storepb.LabelNamesRequest{
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Matchers: []storepb.LabelMatcher{
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "b",
							Value: "",
						},
					},
				},
				expected: []string{"a", "c", "ext2"},
			},
			"outside the time range, with matcher": {
				req: &storepb.LabelNamesRequest{
					Start: timestamp.FromTime(time.Now().Add(-24 * time.Hour)),
					End:   timestamp.FromTime(time.Now().Add(-23 * time.Hour)),
					Matchers: []storepb.LabelMatcher{
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "a",
							Value: "1",
						},
					},
				},
				expected: nil,
			},
		} {
			t.Run(name, func(t *testing.T) {
				vals, err := s.store.LabelNames(ctx, tc.req)
				for _, b := range s.store.blocks {
					waitTimeout(t, &b.pendingReaders, 5*time.Second)
				}

				testutil.Ok(t, err)

				testutil.Equals(t, tc.expected, vals.Names)
			})
		}
	})
}

func TestBucketStore_LabelNames_SeriesLimiter_e2e(t *testing.T) {
	cases := map[string]struct {
		maxSeriesLimit uint64
		expectedErr    string
		code           codes.Code
	}{
		"should succeed if the max series limit is not exceeded": {
			maxSeriesLimit: math.MaxUint64,
		},
		"should fail if the max series limit is exceeded - ResourceExhausted": {
			expectedErr:    "exceeded series limit",
			maxSeriesLimit: 1,
			code:           codes.ResourceExhausted,
		},
	}

	for testName, testData := range cases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			bkt := objstore.NewInMemBucket()
			dir := t.TempDir()
			s := prepareStoreWithTestBlocks(t, dir, bkt, false, NewChunksLimiterFactory(0), NewSeriesLimiterFactory(testData.maxSeriesLimit), NewBytesLimiterFactory(0), emptyRelabelConfig, allowAllFilterConf)
			testutil.Ok(t, s.store.SyncBlocks(ctx))
			req := &storepb.LabelNamesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
				Start: minTimeDuration.PrometheusTimestamp(),
				End:   maxTimeDuration.PrometheusTimestamp(),
			}

			s.cache.SwapWith(noopCache{})

			_, err := s.store.LabelNames(context.Background(), req)

			if testData.expectedErr == "" {
				testutil.Ok(t, err)
			} else {
				testutil.NotOk(t, err)
				testutil.Assert(t, strings.Contains(err.Error(), testData.expectedErr))

				status, ok := status.FromError(err)
				testutil.Equals(t, true, ok)
				testutil.Equals(t, testData.code, status.Code())
			}
		})
	}
}

func TestBucketStore_LabelValues_e2e(t *testing.T) {
	objtesting.ForeachStore(t, func(t *testing.T, bkt objstore.Bucket) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dir := t.TempDir()

		s := prepareStoreWithTestBlocks(t, dir, bkt, false, NewChunksLimiterFactory(0), NewSeriesLimiterFactory(0), NewBytesLimiterFactory(0), emptyRelabelConfig, allowAllFilterConf)
		s.cache.SwapWith(noopCache{})

		mint, maxt := s.store.TimeRange()
		testutil.Equals(t, s.minTime, mint)
		testutil.Equals(t, s.maxTime, maxt)

		for name, tc := range map[string]struct {
			req      *storepb.LabelValuesRequest
			expected []string
		}{
			"label a": {
				req: &storepb.LabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
				},
				expected: []string{"1", "2"},
			},
			"label a, outside time range": {
				req: &storepb.LabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(time.Now().Add(-24 * time.Hour)),
					End:   timestamp.FromTime(time.Now().Add(-23 * time.Hour)),
				},
				expected: nil,
			},
			"label a, a=1": {
				req: &storepb.LabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Matchers: []storepb.LabelMatcher{
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "a",
							Value: "1",
						},
					},
				},
				expected: []string{"1"},
			},
			"label a, a=2, c=2": {
				req: &storepb.LabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Matchers: []storepb.LabelMatcher{
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "a",
							Value: "2",
						},
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "c",
							Value: "2",
						},
					},
				},
				expected: []string{"2"},
			},
			"label ext1": {
				req: &storepb.LabelValuesRequest{
					Label: "ext1",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
				},
				expected: []string{"value1"},
			},
			"label ext1, c=1": {
				req: &storepb.LabelValuesRequest{
					Label: "ext1",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Matchers: []storepb.LabelMatcher{
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "c",
							Value: "1",
						},
					},
				},
				expected: nil, // ext1 is replaced with ext2 for series with c
			},
		} {
			t.Run(name, func(t *testing.T) {
				vals, err := s.store.LabelValues(ctx, tc.req)
				for _, b := range s.store.blocks {
					waitTimeout(t, &b.pendingReaders, 5*time.Second)
				}

				testutil.Ok(t, err)

				testutil.Equals(t, tc.expected, emptyToNil(vals.Values))
			})
		}
	})
}

func TestBucketStore_LabelValues_SeriesLimiter_e2e(t *testing.T) {
	cases := map[string]struct {
		maxSeriesLimit uint64
		expectedErr    string
		code           codes.Code
	}{
		"should succeed if the max chunks limit is not exceeded": {
			maxSeriesLimit: math.MaxUint64,
		},
		"should fail if the max series limit is exceeded - ResourceExhausted": {
			expectedErr:    "exceeded series limit",
			maxSeriesLimit: 1,
			code:           codes.ResourceExhausted,
		},
	}

	for testName, testData := range cases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			bkt := objstore.NewInMemBucket()

			dir := t.TempDir()

			s := prepareStoreWithTestBlocks(t, dir, bkt, false, NewChunksLimiterFactory(0), NewSeriesLimiterFactory(testData.maxSeriesLimit), NewBytesLimiterFactory(0), emptyRelabelConfig, allowAllFilterConf)
			testutil.Ok(t, s.store.SyncBlocks(ctx))

			req := &storepb.LabelValuesRequest{
				Label: "a",
				Start: minTimeDuration.PrometheusTimestamp(),
				End:   maxTimeDuration.PrometheusTimestamp(),
				Matchers: []storepb.LabelMatcher{
					{
						Type:  storepb.LabelMatcher_EQ,
						Name:  "a",
						Value: "1",
					},
				},
			}

			s.cache.SwapWith(noopCache{})

			_, err := s.store.LabelValues(context.Background(), req)

			if testData.expectedErr == "" {
				testutil.Ok(t, err)
			} else {
				testutil.NotOk(t, err)
				testutil.Assert(t, strings.Contains(err.Error(), testData.expectedErr))

				status, ok := status.FromError(err)
				testutil.Equals(t, true, ok)
				testutil.Equals(t, testData.code, status.Code())
			}
		})
	}
}

func emptyToNil(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return values
}

func waitTimeout(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return
	case <-time.After(timeout):
		t.Fatalf("timeout waiting wg for %v", timeout)
	}
}
