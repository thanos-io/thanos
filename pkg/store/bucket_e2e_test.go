package store

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/objtesting"
	"github.com/thanos-io/thanos/pkg/runutil"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

type noopCache struct{}

func (noopCache) SetPostings(b ulid.ULID, l labels.Label, v []byte)   {}
func (noopCache) Postings(b ulid.ULID, l labels.Label) ([]byte, bool) { return nil, false }
func (noopCache) SetSeries(b ulid.ULID, id uint64, v []byte)          {}
func (noopCache) Series(b ulid.ULID, id uint64) ([]byte, bool)        { return nil, false }

type swappableCache struct {
	ptr indexCache
}

func (c *swappableCache) SwapWith(ptr2 indexCache) {
	c.ptr = ptr2
}

func (c *swappableCache) SetPostings(b ulid.ULID, l labels.Label, v []byte) {
	c.ptr.SetPostings(b, l, v)
}

func (c *swappableCache) Postings(b ulid.ULID, l labels.Label) ([]byte, bool) {
	return c.ptr.Postings(b, l)
}

func (c *swappableCache) SetSeries(b ulid.ULID, id uint64, v []byte) {
	c.ptr.SetSeries(b, id, v)
}

func (c *swappableCache) Series(b ulid.ULID, id uint64) ([]byte, bool) {
	return c.ptr.Series(b, id)
}

type storeSuite struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup

	store            *BucketStore
	minTime, maxTime int64
	cache            *swappableCache

	logger log.Logger
}

func (s *storeSuite) Close() {
	s.cancel()
	s.wg.Wait()
}

func prepareStoreWithTestBlocks(t testing.TB, dir string, bkt objstore.Bucket, manyParts bool, maxSampleCount uint64) *storeSuite {
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

	start := time.Now()
	now := start

	ctx, cancel := context.WithCancel(context.Background())
	s := &storeSuite{
		cancel: cancel,
		logger: log.NewLogfmtLogger(os.Stderr),
		cache:  &swappableCache{},
	}
	blocks := 0
	for i := 0; i < 3; i++ {
		mint := timestamp.FromTime(now)
		now = now.Add(2 * time.Hour)
		maxt := timestamp.FromTime(now)

		if s.minTime == 0 {
			s.minTime = mint
		}
		s.maxTime = maxt

		// Create two blocks per time slot. Only add 10 samples each so only one chunk
		// gets created each. This way we can easily verify we got 10 chunks per series below.
		id1, err := testutil.CreateBlock(ctx, dir, series[:4], 10, mint, maxt, extLset, 0)
		testutil.Ok(t, err)
		id2, err := testutil.CreateBlock(ctx, dir, series[4:], 10, mint, maxt, extLset, 0)
		testutil.Ok(t, err)

		dir1, dir2 := filepath.Join(dir, id1.String()), filepath.Join(dir, id2.String())

		// Add labels to the meta of the second block.
		meta, err := metadata.Read(dir2)
		testutil.Ok(t, err)
		meta.Thanos.Labels = map[string]string{"ext2": "value2"}
		testutil.Ok(t, metadata.Write(s.logger, dir2, meta))

		testutil.Ok(t, block.Upload(ctx, s.logger, bkt, dir1))
		testutil.Ok(t, block.Upload(ctx, s.logger, bkt, dir2))
		blocks += 2

		testutil.Ok(t, os.RemoveAll(dir1))
		testutil.Ok(t, os.RemoveAll(dir2))
	}

	store, err := NewBucketStore(s.logger, nil, bkt, dir, s.cache, 0, maxSampleCount, 20, false, 20)
	testutil.Ok(t, err)

	s.store = store

	if manyParts {
		s.store.partitioner = naivePartitioner{}
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		if err := runutil.Repeat(100*time.Millisecond, ctx.Done(), func() error {
			return store.SyncBlocks(ctx)
		}); err != nil && errors.Cause(err) != context.Canceled {
			t.Error(err)
			t.FailNow()
		}
	}()

	rctx, rcancel := context.WithTimeout(ctx, 30*time.Second)
	defer rcancel()

	testutil.Ok(t, runutil.Retry(100*time.Millisecond, rctx.Done(), func() error {
		if store.numBlocks() < blocks {
			return errors.New("not all blocks loaded")
		}
		return nil
	}))

	return s
}

func testBucketStore_e2e(t testing.TB, ctx context.Context, s *storeSuite) {
	mint, maxt := s.store.TimeRange()
	testutil.Equals(t, s.minTime, mint)
	testutil.Equals(t, s.maxTime, maxt)

	vals, err := s.store.LabelValues(ctx, &storepb.LabelValuesRequest{Label: "a"})
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"1", "2"}, vals.Values)

	// TODO(bwplotka): Add those test cases to TSDB querier_test.go as well, there are no tests for matching.
	for i, tcase := range []struct {
		req      *storepb.SeriesRequest
		expected [][]storepb.Label
	}{
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expected: [][]storepb.Label{
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
					{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expected: [][]storepb.Label{
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
			expected: [][]storepb.Label{
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
			expected: [][]storepb.Label{
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
			expected: [][]storepb.Label{
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
			expected: [][]storepb.Label{
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
			expected: [][]storepb.Label{
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
			expected: [][]storepb.Label{
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
			expected: [][]storepb.Label{
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
	} {
		t.Log("Run ", i)

		srv := newStoreSeriesServer(ctx)

		testutil.Ok(t, s.store.Series(tcase.req, srv))
		testutil.Equals(t, len(tcase.expected), len(srv.SeriesSet))

		for i, s := range srv.SeriesSet {
			testutil.Equals(t, tcase.expected[i], s.Labels)
			testutil.Equals(t, 3, len(s.Chunks))
		}
	}
}

func TestBucketStore_e2e(t *testing.T) {
	objtesting.ForeachStore(t, func(t testing.TB, bkt objstore.Bucket) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dir, err := ioutil.TempDir("", "test_bucketstore_e2e")
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

		s := prepareStoreWithTestBlocks(t, dir, bkt, false, 0)
		defer s.Close()

		t.Log("Test with no index cache")
		s.cache.SwapWith(noopCache{})
		testBucketStore_e2e(t, ctx, s)

		t.Log("Test with large, sufficient index cache")
		indexCache, err := storecache.NewIndexCache(s.logger, nil, storecache.Opts{
			MaxItemSizeBytes: 1e5,
			MaxSizeBytes:     2e5,
		})
		testutil.Ok(t, err)
		s.cache.SwapWith(indexCache)
		testBucketStore_e2e(t, ctx, s)

		t.Log("Test with small index cache")
		indexCache2, err := storecache.NewIndexCache(s.logger, nil, storecache.Opts{
			MaxItemSizeBytes: 50,
			MaxSizeBytes:     100,
		})
		testutil.Ok(t, err)
		s.cache.SwapWith(indexCache2)
		testBucketStore_e2e(t, ctx, s)
	})
}

type naivePartitioner struct{}

func (g naivePartitioner) Partition(length int, rng func(int) (uint64, uint64)) (parts []part) {
	for i := 0; i < length; i++ {
		s, e := rng(i)
		parts = append(parts, part{start: s, end: e, elemRng: [2]int{i, i + 1}})
	}
	return parts
}

// Naive partitioner splits the array equally (it does not combine anything).
// This tests if our, sometimes concurrent, fetches for different parts works.
// Regression test against: https://github.com/thanos-io/thanos/issues/829
func TestBucketStore_ManyParts_e2e(t *testing.T) {
	objtesting.ForeachStore(t, func(t testing.TB, bkt objstore.Bucket) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dir, err := ioutil.TempDir("", "test_bucketstore_e2e")
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

		s := prepareStoreWithTestBlocks(t, dir, bkt, true, 0)
		defer s.Close()

		indexCache, err := storecache.NewIndexCache(s.logger, nil, storecache.Opts{
			MaxItemSizeBytes: 1e5,
			MaxSizeBytes:     2e5,
		})
		testutil.Ok(t, err)
		s.cache.SwapWith(indexCache)

		testBucketStore_e2e(t, ctx, s)
	})
}
