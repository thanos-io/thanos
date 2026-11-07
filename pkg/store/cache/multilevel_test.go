// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type recordingIndexCache struct {
	IndexCache
	postingsRequests [][]labels.Label
	seriesRequests   [][]storage.SeriesRef
	expandedRequests int
	afterFetch       func()
}

func newRecordingIndexCache(t testing.TB) *recordingIndexCache {
	t.Helper()
	cache, err := NewInMemoryIndexCacheWithConfig(log.NewNopLogger(), nil, prometheus.NewRegistry(), InMemoryIndexCacheConfig{
		MaxSize: 4 * 1024 * 1024, MaxItemSize: 128 * 1024,
	})
	testutil.Ok(t, err)
	return &recordingIndexCache{IndexCache: cache}
}

func (c *recordingIndexCache) FetchMultiPostings(ctx context.Context, block ulid.ULID, keys []labels.Label, tenant string) (map[labels.Label][]byte, []labels.Label) {
	c.postingsRequests = append(c.postingsRequests, append([]labels.Label(nil), keys...))
	hits, misses := c.IndexCache.FetchMultiPostings(ctx, block, keys, tenant)
	if c.afterFetch != nil {
		c.afterFetch()
		// Model a backend abandoning the rest of a batch on cancellation.
		misses = nil
	}
	return hits, misses
}

func (c *recordingIndexCache) FetchMultiSeries(ctx context.Context, block ulid.ULID, ids []storage.SeriesRef, tenant string) (map[storage.SeriesRef][]byte, []storage.SeriesRef) {
	c.seriesRequests = append(c.seriesRequests, append([]storage.SeriesRef(nil), ids...))
	hits, misses := c.IndexCache.FetchMultiSeries(ctx, block, ids, tenant)
	if c.afterFetch != nil {
		c.afterFetch()
		misses = nil
	}
	return hits, misses
}

func (c *recordingIndexCache) FetchExpandedPostings(ctx context.Context, block ulid.ULID, matchers []*labels.Matcher, tenant string) ([]byte, bool) {
	c.expandedRequests++
	return c.IndexCache.FetchExpandedPostings(ctx, block, matchers, tenant)
}

func TestMultiLevelIndexCachePostings(t *testing.T) {
	block := ulid.MustNew(1, nil)
	ctx := context.Background()
	const tenant = "test-tenant"
	keys := []labels.Label{{Name: "a", Value: "1"}, {Name: "a", Value: "2"}, {Name: "a", Value: "3"}, {Name: "a", Value: "4"}}
	levels := []*recordingIndexCache{newRecordingIndexCache(t), newRecordingIndexCache(t), newRecordingIndexCache(t)}
	metrics := NewCommonMetrics(prometheus.NewRegistry())
	cache := newMultiLevelIndexCache([]IndexCache{levels[0], levels[1], levels[2]}, metrics)

	for i, level := range levels {
		level.StorePostings(block, keys[i], []byte{byte(i)}, tenant)
	}
	hits, misses := cache.FetchMultiPostings(ctx, block, keys, tenant)
	testutil.Equals(t, map[labels.Label][]byte{keys[0]: {0}, keys[1]: {1}, keys[2]: {2}}, hits)
	testutil.Equals(t, keys[3:], misses)
	for i, level := range levels {
		testutil.Equals(t, [][]labels.Label{keys[i:]}, level.postingsRequests)
	}

	// Hits from both lower levels must now be available in L1.
	hits, misses = cache.FetchMultiPostings(ctx, block, keys[:3], tenant)
	testutil.Equals(t, 3, len(hits))
	testutil.Equals(t, 0, len(misses))
	testutil.Equals(t, 1, len(levels[1].postingsRequests))
	testutil.Equals(t, 1, len(levels[2].postingsRequests))
	testutil.Equals(t, float64(7), promtest.ToFloat64(metrics.RequestTotal.WithLabelValues(CacheTypePostings, tenant)))
	testutil.Equals(t, float64(6), promtest.ToFloat64(metrics.HitsTotal.WithLabelValues(CacheTypePostings, tenant)))

	cache.StorePostings(block, keys[3], []byte{3}, tenant)
	for _, level := range levels {
		hits, misses := level.FetchMultiPostings(ctx, block, keys[3:], tenant)
		testutil.Equals(t, []byte{3}, hits[keys[3]])
		testutil.Equals(t, 0, len(misses))
	}
}

func TestMultiLevelIndexCacheSeries(t *testing.T) {
	block := ulid.MustNew(1, nil)
	ctx := context.Background()
	const tenant = "test-tenant"
	ids := []storage.SeriesRef{1, 2, 3, 4}
	levels := []*recordingIndexCache{newRecordingIndexCache(t), newRecordingIndexCache(t), newRecordingIndexCache(t)}
	cache := newMultiLevelIndexCache([]IndexCache{levels[0], levels[1], levels[2]}, NewCommonMetrics(prometheus.NewRegistry()))
	for i, level := range levels {
		level.StoreSeries(block, ids[i], []byte{byte(i)}, tenant)
	}
	hits, misses := cache.FetchMultiSeries(ctx, block, ids, tenant)
	testutil.Equals(t, map[storage.SeriesRef][]byte{1: {0}, 2: {1}, 3: {2}}, hits)
	testutil.Equals(t, ids[3:], misses)
	for i, level := range levels {
		testutil.Equals(t, [][]storage.SeriesRef{ids[i:]}, level.seriesRequests)
	}

	hits, misses = cache.FetchMultiSeries(ctx, block, ids[:3], tenant)
	testutil.Equals(t, 3, len(hits))
	testutil.Equals(t, 0, len(misses))
	testutil.Equals(t, 1, len(levels[1].seriesRequests))
	testutil.Equals(t, 1, len(levels[2].seriesRequests))

	cache.StoreSeries(block, ids[3], []byte{3}, tenant)
	for _, level := range levels {
		hits, _ := level.FetchMultiSeries(ctx, block, ids[3:], tenant)
		testutil.Equals(t, []byte{3}, hits[ids[3]])
	}
}

func TestMultiLevelIndexCacheExpandedPostings(t *testing.T) {
	block := ulid.MustNew(1, nil)
	ctx := context.Background()
	const tenant = "test-tenant"
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "1")}
	levels := []*recordingIndexCache{newRecordingIndexCache(t), newRecordingIndexCache(t), newRecordingIndexCache(t)}
	cache := newMultiLevelIndexCache([]IndexCache{levels[0], levels[1], levels[2]}, NewCommonMetrics(prometheus.NewRegistry()))
	value := []byte{1, 2, 3}
	levels[2].StoreExpandedPostings(block, matchers, value, tenant)
	for range 2 {
		got, ok := cache.FetchExpandedPostings(ctx, block, matchers, tenant)
		testutil.Assert(t, ok)
		testutil.Equals(t, value, got)
	}
	testutil.Equals(t, 2, levels[0].expandedRequests)
	testutil.Equals(t, 1, levels[1].expandedRequests)
	testutil.Equals(t, 1, levels[2].expandedRequests)

	other := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "2")}
	_, ok := cache.FetchExpandedPostings(ctx, block, other, tenant)
	testutil.Assert(t, !ok)
	cache.StoreExpandedPostings(block, other, value, tenant)
	for _, level := range levels {
		got, ok := level.FetchExpandedPostings(ctx, block, other, tenant)
		testutil.Assert(t, ok)
		testutil.Equals(t, value, got)
	}
}

func TestMultiLevelIndexCacheCanceledAndEmptyRequests(t *testing.T) {
	block := ulid.MustNew(1, nil)
	keys := []labels.Label{{Name: "a", Value: "1"}}
	ids := []storage.SeriesRef{1}
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "1")}
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%t", canceled), func(t *testing.T) {
			keys, ids := keys, ids
			l1, l2 := newRecordingIndexCache(t), newRecordingIndexCache(t)
			cache := newMultiLevelIndexCache([]IndexCache{l1, l2}, NewCommonMetrics(prometheus.NewRegistry()))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if canceled {
				cancel()
			} else {
				keys, ids = nil, nil
			}
			hits, misses := cache.FetchMultiPostings(ctx, block, keys, "tenant")
			testutil.Equals(t, 0, len(hits))
			testutil.Equals(t, keys, misses)
			seriesHits, seriesMisses := cache.FetchMultiSeries(ctx, block, ids, "tenant")
			testutil.Equals(t, 0, len(seriesHits))
			testutil.Equals(t, ids, seriesMisses)
			if canceled {
				_, ok := cache.FetchExpandedPostings(ctx, block, matchers, "tenant")
				testutil.Assert(t, !ok)
			}
			for _, level := range []*recordingIndexCache{l1, l2} {
				testutil.Equals(t, 0, len(level.postingsRequests)+len(level.seriesRequests)+level.expandedRequests)
			}
		})
	}
}

func TestMultiLevelIndexCacheCancellationBetweenLevels(t *testing.T) {
	block := ulid.MustNew(1, nil)
	ids := []storage.SeriesRef{1, 2}
	keys := []labels.Label{{Name: "a", Value: "1"}, {Name: "a", Value: "2"}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l1, l2 := newRecordingIndexCache(t), newRecordingIndexCache(t)
	l1.afterFetch = cancel
	cache := newMultiLevelIndexCache([]IndexCache{l1, l2}, NewCommonMetrics(prometheus.NewRegistry()))
	l1.StoreSeries(block, ids[0], []byte{1}, "tenant")
	hits, misses := cache.FetchMultiSeries(ctx, block, ids, "tenant")
	testutil.Equals(t, map[storage.SeriesRef][]byte{1: {1}}, hits)
	testutil.Equals(t, ids[1:], misses)
	testutil.Equals(t, 0, len(l2.seriesRequests))

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	l1.afterFetch = cancel
	l1.StorePostings(block, keys[0], []byte{1}, "tenant")
	postings, missing := cache.FetchMultiPostings(ctx, block, keys, "tenant")
	testutil.Equals(t, map[labels.Label][]byte{keys[0]: {1}}, postings)
	testutil.Equals(t, keys[1:], missing)
	testutil.Equals(t, 0, len(l2.postingsRequests))
}

func TestMultiLevelIndexCacheEvictionAndConcurrentAccess(t *testing.T) {
	reg := prometheus.NewRegistry()
	cache, err := NewIndexCache(log.NewNopLogger(), []byte(`
type: MULTILEVEL
config:
  caches:
    - type: IN-MEMORY
      config:
        max_size: 32B
        max_item_size: 32B
    - type: IN-MEMORY
      config:
        max_size: 1MB
        max_item_size: 1KB
`), reg)
	testutil.Ok(t, err)
	block := ulid.MustNew(1, nil)
	ctx := context.Background()
	cache.StoreSeries(block, 1, []byte{1}, "tenant")
	cache.StoreSeries(block, 2, []byte{2}, "tenant")
	for _, id := range []storage.SeriesRef{1, 2, 1} {
		hits, misses := cache.FetchMultiSeries(ctx, block, []storage.SeriesRef{id}, "tenant")
		testutil.Equals(t, []byte{byte(id)}, hits[id])
		testutil.Equals(t, 0, len(misses))
	}
	// Concurrent reads and promotion must remain safe while L1 repeatedly evicts.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i := range 50 {
				id := storage.SeriesRef(i%2 + 1)
				cache.StoreSeries(block, id, []byte{byte(id)}, "tenant")
				hits, misses := cache.FetchMultiSeries(ctx, block, []storage.SeriesRef{id}, "tenant")
				if len(misses) != 0 || len(hits[id]) != 1 || hits[id][0] != byte(id) {
					t.Errorf("unexpected cache result: hits=%v misses=%v", hits, misses)
					return
				}
			}
		})
	}
	wg.Wait()
}

type recordingRemoteCacheClient struct {
	*mockedMemcachedClient
	requests [][]string
}

func (c *recordingRemoteCacheClient) GetMulti(ctx context.Context, keys []string) map[string][]byte {
	c.requests = append(c.requests, append([]string(nil), keys...))
	return c.mockedMemcachedClient.GetMulti(ctx, keys)
}

func TestMultiLevelIndexCacheRemotePromotionAndFailure(t *testing.T) {
	block := ulid.MustNew(1, nil)
	ctx := context.Background()
	const tenant = "tenant"
	client := &recordingRemoteCacheClient{mockedMemcachedClient: newMockedMemcachedClient(nil)}
	remote, err := NewRemoteIndexCache(log.NewNopLogger(), client, nil, prometheus.NewRegistry(), memcachedDefaultTTL)
	testutil.Ok(t, err)
	l1, l3 := newRecordingIndexCache(t), newRecordingIndexCache(t)
	cache := newMultiLevelIndexCache([]IndexCache{l1, remote, l3}, NewCommonMetrics(prometheus.NewRegistry()))
	key := labels.Label{Name: "a", Value: "1"}
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "1")}
	value := []byte{1, 2, 3}
	remote.StorePostings(block, key, value, tenant)
	remote.StoreSeries(block, 1, value, tenant)
	remote.StoreExpandedPostings(block, matchers, value, tenant)

	for range 2 {
		postings, misses := cache.FetchMultiPostings(ctx, block, []labels.Label{key}, tenant)
		testutil.Equals(t, value, postings[key])
		testutil.Equals(t, 0, len(misses))
		series, seriesMisses := cache.FetchMultiSeries(ctx, block, []storage.SeriesRef{1}, tenant)
		testutil.Equals(t, value, series[1])
		testutil.Equals(t, 0, len(seriesMisses))
		expanded, ok := cache.FetchExpandedPostings(ctx, block, matchers, tenant)
		testutil.Assert(t, ok)
		testutil.Equals(t, value, expanded)
		// Simulate L2 expiry and failure after the first reads warmed L1.
		client.cache = map[string][]byte{}
		client.mockedGetMultiErr = errors.New("remote unavailable")
	}
	testutil.Equals(t, 3, len(client.requests))
	testutil.Equals(t, 0, len(l3.postingsRequests)+len(l3.seriesRequests)+l3.expandedRequests)

	// A failed remote level must preserve L1 hits and allow later levels to fill
	// the remaining keys in one batch.
	l3.StoreSeries(block, 2, []byte{2}, tenant)
	series, misses := cache.FetchMultiSeries(ctx, block, []storage.SeriesRef{1, 2, 3}, tenant)
	testutil.Equals(t, map[storage.SeriesRef][]byte{1: value, 2: {2}}, series)
	testutil.Equals(t, []storage.SeriesRef{3}, misses)
	testutil.Equals(t, []string{
		CacheKey{Block: block.String(), Key: CacheKeySeries(2)}.String(),
		CacheKey{Block: block.String(), Key: CacheKeySeries(3)}.String(),
	}, client.requests[3])
	testutil.Equals(t, [][]storage.SeriesRef{{2, 3}}, l3.seriesRequests)
}

func BenchmarkMultiLevelIndexCacheFetchMultiSeries(b *testing.B) {
	for _, size := range []int{1, 100, 1000} {
		for _, scenario := range []string{"inmemory", "L1-hit", "L2-hit", "all-miss"} {
			b.Run(fmt.Sprintf("%s/keys=%d", scenario, size), func(b *testing.B) {
				reg := prometheus.NewRegistry()
				config := DefaultInMemoryIndexCacheConfig
				if scenario == "L2-hit" {
					// Keep L1 empty so every iteration exercises lookup and promotion.
					config = InMemoryIndexCacheConfig{}
				}
				l1, err := NewInMemoryIndexCacheWithConfig(log.NewNopLogger(), nil, reg, config)
				testutil.Ok(b, err)
				var cache IndexCache = l1
				if scenario != "inmemory" {
					l2, err := NewInMemoryIndexCache(log.NewNopLogger(), nil, prometheus.NewRegistry(), nil)
					testutil.Ok(b, err)
					cache = newMultiLevelIndexCache([]IndexCache{l1, l2}, NewCommonMetrics(prometheus.NewRegistry()))
				}
				block := ulid.MustNew(1, nil)
				ids := make([]storage.SeriesRef, size)
				for i := range ids {
					ids[i] = storage.SeriesRef(i)
					if scenario != "all-miss" {
						cache.StoreSeries(block, ids[i], []byte(strconv.Itoa(i)), "tenant")
					}
				}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					cache.FetchMultiSeries(ctx, block, ids, "tenant")
				}
			})
		}
	}
}
