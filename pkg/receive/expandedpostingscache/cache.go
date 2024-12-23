// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Original code by Alan Protasio (https://github.com/alanprot) in the Cortex project.

package expandedpostingscache

import (
	"container/list"
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/oklog/ulid"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

type ExpandedPostingsCache interface {
	PostingsForMatchers(ctx context.Context, blockID ulid.ULID, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error)
	ExpireSeries(metric labels.Labels)
	tsdb.SeriesLifecycleCallback
}

type BlocksPostingsForMatchersCache struct {
	strippedLock []sync.RWMutex

	headCache   *fifoCache[[]storage.SeriesRef]
	blocksCache *fifoCache[[]storage.SeriesRef]

	headSeedByMetricName    []int
	postingsForMatchersFunc func(ctx context.Context, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error)
	timeNow                 func() time.Time

	metrics ExpandedPostingsCacheMetrics
}

var (
	rangeHeadULID = ulid.MustParse("0000000000XXXXXXXRANGEHEAD")
	headULID      = ulid.MustParse("0000000000XXXXXXXXXXXXHEAD")
)

const (
	// size of the seed array. Each seed is a 64bits int (8 bytes)
	// totaling 8mb.
	seedArraySize = 1024 * 1024

	numOfSeedsStripes = 512
)

type ExpandedPostingsCacheMetrics struct {
	CacheRequests       *prometheus.CounterVec
	CacheHits           *prometheus.CounterVec
	CacheEvicts         *prometheus.CounterVec
	NonCacheableQueries *prometheus.CounterVec
}

func NewPostingCacheMetrics(r prometheus.Registerer) ExpandedPostingsCacheMetrics {
	return ExpandedPostingsCacheMetrics{
		CacheRequests: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "expanded_postings_cache_requests_total",
			Help: "Total number of requests to the cache.",
		}, []string{"cache"}),
		CacheHits: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "expanded_postings_cache_hits_total",
			Help: "Total number of hit requests to the cache.",
		}, []string{"cache"}),
		CacheEvicts: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "expanded_postings_cache_evicts_total",
			Help: "Total number of evictions in the cache, excluding items that got evicted.",
		}, []string{"cache", "reason"}),
		NonCacheableQueries: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "expanded_postings_non_cacheable_queries_total",
			Help: "Total number of non cacheable queries.",
		}, []string{"cache"}),
	}
}

func NewBlocksPostingsForMatchersCache(metrics ExpandedPostingsCacheMetrics, headExpandedPostingsCacheSize uint64, blockExpandedPostingsCacheSize uint64, seedSize int64) *BlocksPostingsForMatchersCache {
	if seedSize <= 0 {
		seedSize = seedArraySize
	}

	return &BlocksPostingsForMatchersCache{
		headCache:               newFifoCache[[]storage.SeriesRef]("head", metrics, time.Now, headExpandedPostingsCacheSize),
		blocksCache:             newFifoCache[[]storage.SeriesRef]("block", metrics, time.Now, blockExpandedPostingsCacheSize),
		headSeedByMetricName:    make([]int, seedSize),
		strippedLock:            make([]sync.RWMutex, numOfSeedsStripes),
		postingsForMatchersFunc: tsdb.PostingsForMatchers,
		timeNow:                 time.Now,
		metrics:                 metrics,
	}
}

func (c *BlocksPostingsForMatchersCache) PostCreation(metric labels.Labels) {
	c.ExpireSeries(metric)
}

func (c *BlocksPostingsForMatchersCache) PostDeletion(metrics map[chunks.HeadSeriesRef]labels.Labels) {
	for _, metric := range metrics {
		c.ExpireSeries(metric)
	}
}

func (c *BlocksPostingsForMatchersCache) PreCreation(labels.Labels) error {
	return nil
}

func (c *BlocksPostingsForMatchersCache) ExpireSeries(metric labels.Labels) {
	var metricName string

	metric.Range(func(l labels.Label) {
		if l.Name != model.MetricNameLabel {
			return
		}
		metricName = l.Value
	})

	if metricName == "" {
		return
	}

	h := MemHashString(metricName)
	i := h % uint64(len(c.headSeedByMetricName))
	l := i % uint64(len(c.strippedLock))
	c.strippedLock[l].Lock()
	defer c.strippedLock[l].Unlock()
	c.headSeedByMetricName[i]++
}

func (c *BlocksPostingsForMatchersCache) PostingsForMatchers(ctx context.Context, blockID ulid.ULID, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error) {
	return c.fetchPostings(blockID, ix, ms...)(ctx)
}

func (c *BlocksPostingsForMatchersCache) fetchPostings(blockID ulid.ULID, ix tsdb.IndexReader, ms ...*labels.Matcher) func(context.Context) (index.Postings, error) {
	var seed string
	cache := c.blocksCache

	// If is a head block, lets add the seed on the cache key so we can
	// invalidate the cache when new series are created for this metric name
	if isHeadBlock(blockID) {
		cache = c.headCache

		metricName, ok := metricNameFromMatcher(ms)
		// Lets not cache head if we don;t find an equal matcher for the label __name__
		if !ok {
			c.metrics.NonCacheableQueries.WithLabelValues(cache.name).Inc()
			return func(ctx context.Context) (index.Postings, error) {
				return tsdb.PostingsForMatchers(ctx, ix, ms...)
			}
		}

		seed = c.getSeedForMetricName(metricName)
	}

	c.metrics.CacheRequests.WithLabelValues(cache.name).Inc()

	fetch := func() ([]storage.SeriesRef, int64, error) {
		// Use context.Background() as this promise is maybe shared across calls
		postings, err := c.postingsForMatchersFunc(context.Background(), ix, ms...)

		if err == nil {
			ids, err := index.ExpandPostings(postings)
			return ids, int64(len(ids) * 8), err
		}

		return nil, 0, err
	}

	key := c.cacheKey(seed, blockID, ms...)
	promise, loaded := cache.getPromiseForKey(key, fetch)
	if loaded {
		c.metrics.CacheHits.WithLabelValues(cache.name).Inc()
	}

	return c.result(promise)
}

func (c *BlocksPostingsForMatchersCache) result(ce *cacheEntryPromise[[]storage.SeriesRef]) func(ctx context.Context) (index.Postings, error) {
	return func(ctx context.Context) (index.Postings, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ce.done:
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return index.NewListPostings(ce.v), ce.err
		}
	}
}

func (c *BlocksPostingsForMatchersCache) getSeedForMetricName(metricName string) string {
	h := MemHashString(metricName)
	i := h % uint64(len(c.headSeedByMetricName))
	l := i % uint64(len(c.strippedLock))
	c.strippedLock[l].RLock()
	defer c.strippedLock[l].RUnlock()
	return strconv.Itoa(c.headSeedByMetricName[i])
}

func (c *BlocksPostingsForMatchersCache) cacheKey(seed string, blockID ulid.ULID, ms ...*labels.Matcher) string {
	slices.SortFunc(ms, func(i, j *labels.Matcher) int {
		if i.Type != j.Type {
			return int(i.Type - j.Type)
		}
		if i.Name != j.Name {
			return strings.Compare(i.Name, j.Name)
		}
		if i.Value != j.Value {
			return strings.Compare(i.Value, j.Value)
		}
		return 0
	})

	const (
		typeLen = 2
		sepLen  = 1
	)

	var size int
	for _, m := range ms {
		size += len(seed) + len(blockID.String()) + len(m.Name) + len(m.Value) + typeLen + 2*sepLen
	}
	sb := strings.Builder{}
	sb.Grow(size)
	sb.WriteString(seed)
	sb.WriteByte('|')
	sb.WriteString(blockID.String())
	for _, m := range ms {
		sb.WriteString(m.Name)
		sb.WriteString(m.Type.String())
		sb.WriteString(m.Value)
		sb.WriteByte('|')
	}
	key := sb.String()
	return key
}

func isHeadBlock(blockID ulid.ULID) bool {
	return blockID == rangeHeadULID || blockID == headULID
}

func metricNameFromMatcher(ms []*labels.Matcher) (string, bool) {
	for _, m := range ms {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual {
			return m.Value, true
		}
	}

	return "", false
}

// TODO(GiedriusS): convert Thanos caching system to be promised-based
// i.e. avoid multiple loads for same item. This is a copy from Cortex.
// Use as an inspiration.
type fifoCache[V any] struct {
	cachedValues *sync.Map
	timeNow      func() time.Time
	name         string
	metrics      ExpandedPostingsCacheMetrics

	ttl      time.Duration
	maxBytes int64

	// Fields from here should be locked
	cachedMtx   sync.RWMutex
	cached      *list.List
	cachedBytes int64
}

func newFifoCache[V any](name string, metrics ExpandedPostingsCacheMetrics, timeNow func() time.Time, maxBytes uint64) *fifoCache[V] {
	return &fifoCache[V]{
		cachedValues: new(sync.Map),
		cached:       list.New(),
		timeNow:      timeNow,
		name:         name,
		metrics:      metrics,
		ttl:          10 * time.Minute,
		maxBytes:     int64(maxBytes),
	}
}

func (c *fifoCache[V]) expire() {
	if c.ttl.Seconds() <= 0 {
		return
	}
	c.cachedMtx.RLock()
	if _, r := c.shouldEvictHead(); !r {
		c.cachedMtx.RUnlock()
		return
	}
	c.cachedMtx.RUnlock()
	c.cachedMtx.Lock()
	defer c.cachedMtx.Unlock()
	for reason, r := c.shouldEvictHead(); r; reason, r = c.shouldEvictHead() {
		c.metrics.CacheEvicts.WithLabelValues(c.name, reason).Inc()
		c.evictHead()
	}
}

func (c *fifoCache[V]) getPromiseForKey(k string, fetch func() (V, int64, error)) (*cacheEntryPromise[V], bool) {
	r := &cacheEntryPromise[V]{
		done: make(chan struct{}),
	}
	defer close(r.done)

	loaded, ok := c.cachedValues.LoadOrStore(k, r)

	if !ok {
		r.v, r.sizeBytes, r.err = fetch()
		r.sizeBytes += int64(len(k))
		r.ts = c.timeNow()
		c.created(k, r.sizeBytes)
		c.expire()
	}

	if ok {
		// If the promise is already in the cache, lets wait it to fetch the data.
		<-loaded.(*cacheEntryPromise[V]).done

		// If is cached but is expired, lets try to replace the cache value.
		if loaded.(*cacheEntryPromise[V]).isExpired(c.ttl, c.timeNow()) && c.cachedValues.CompareAndSwap(k, loaded, r) {
			c.metrics.CacheEvicts.WithLabelValues(c.name, "expired").Inc()
			r.v, r.sizeBytes, r.err = fetch()
			r.sizeBytes += int64(len(k))
			c.updateSize(loaded.(*cacheEntryPromise[V]).sizeBytes, r.sizeBytes)
			loaded = r
			r.ts = c.timeNow()
			ok = false
		}
	}

	return loaded.(*cacheEntryPromise[V]), ok
}

func (c *fifoCache[V]) shouldEvictHead() (string, bool) {
	h := c.cached.Front()
	if h == nil {
		return "", false
	}

	if c.cachedBytes > c.maxBytes {
		return "full", true
	}
	key := h.Value.(string)

	if l, ok := c.cachedValues.Load(key); ok {
		return "expired", l.(*cacheEntryPromise[V]).isExpired(c.ttl, c.timeNow())
	}

	return "", false
}

func (c *fifoCache[V]) evictHead() {
	front := c.cached.Front()
	c.cached.Remove(front)
	oldestKey := front.Value.(string)
	if oldest, loaded := c.cachedValues.LoadAndDelete(oldestKey); loaded {
		c.cachedBytes -= oldest.(*cacheEntryPromise[V]).sizeBytes
	}
}

func (c *fifoCache[V]) created(key string, sizeBytes int64) {
	if c.ttl <= 0 {
		c.cachedValues.Delete(key)
		return
	}
	c.cachedMtx.Lock()
	defer c.cachedMtx.Unlock()
	c.cached.PushBack(key)
	c.cachedBytes += sizeBytes
}

func (c *fifoCache[V]) updateSize(oldSize, newSizeBytes int64) {
	if oldSize == newSizeBytes {
		return
	}

	c.cachedMtx.Lock()
	defer c.cachedMtx.Unlock()
	c.cachedBytes += newSizeBytes - oldSize
}

func (c *fifoCache[V]) contains(k string) bool {
	_, ok := c.cachedValues.Load(k)
	return ok
}

type cacheEntryPromise[V any] struct {
	ts        time.Time
	sizeBytes int64

	done chan struct{}
	v    V
	err  error
}

func (ce *cacheEntryPromise[V]) isExpired(ttl time.Duration, now time.Time) bool {
	ts := ce.ts
	r := now.Sub(ts)
	return r >= ttl
}

func MemHashString(str string) uint64 {
	return xxhash.Sum64String(str)
}
