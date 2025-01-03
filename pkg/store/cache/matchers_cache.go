// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/singleflight"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

const DefaultCacheSize = 200

type NewItemFunc func() (*labels.Matcher, error)

type MatchersCache interface {
	// GetOrSet retrieves a matcher from cache or creates and stores it if not present.
	// If the matcher is not in cache, it uses the provided newItem function to create it.
	GetOrSet(key string, newItem NewItemFunc) (*labels.Matcher, error)
}

// Ensure implementations satisfy the interface.
var (
	_ MatchersCache = (*LruMatchersCache)(nil)
	_ MatchersCache = (*NoopMatcherCache)(nil)
)

// NoopMatcherCache is a no-op implementation of MatchersCache that doesn't cache anything.
type NoopMatcherCache struct{}

// NewNoopMatcherCache creates a new no-op matcher cache.
func NewNoopMatcherCache() MatchersCache {
	return &NoopMatcherCache{}
}

// GetOrSet implements MatchersCache by always creating a new matcher without caching.
func (n *NoopMatcherCache) GetOrSet(_ string, newItem NewItemFunc) (*labels.Matcher, error) {
	return newItem()
}

// LruMatchersCache implements MatchersCache with an LRU cache and metrics.
type LruMatchersCache struct {
	reg     prometheus.Registerer
	cache   *lru.Cache[string, *labels.Matcher]
	metrics *matcherCacheMetrics
	size    int
	sf      singleflight.Group
}

type MatcherCacheOption func(*LruMatchersCache)

func WithPromRegistry(reg prometheus.Registerer) MatcherCacheOption {
	return func(c *LruMatchersCache) {
		c.reg = reg
	}
}

func WithSize(size int) MatcherCacheOption {
	return func(c *LruMatchersCache) {
		c.size = size
	}
}

func NewMatchersCache(opts ...MatcherCacheOption) (*LruMatchersCache, error) {
	cache := &LruMatchersCache{
		size: DefaultCacheSize,
	}

	for _, opt := range opts {
		opt(cache)
	}
	cache.metrics = newMatcherCacheMetrics(cache.reg)

	lruCache, err := lru.NewWithEvict[string, *labels.Matcher](cache.size, cache.onEvict)
	if err != nil {
		return nil, err
	}
	cache.cache = lruCache

	return cache, nil
}

func (c *LruMatchersCache) GetOrSet(key string, newItem NewItemFunc) (*labels.Matcher, error) {
	c.metrics.requestsTotal.Inc()
	v, err, _ := c.sf.Do(key, func() (interface{}, error) {
		if item, ok := c.cache.Get(key); ok {
			c.metrics.hitsTotal.Inc()
			return item, nil
		}

		item, err := newItem()
		if err != nil {
			return nil, err
		}
		c.cache.Add(key, item)
		c.metrics.numItems.Set(float64(c.cache.Len()))
		return item, nil
	})

	if err != nil {
		return nil, err
	}
	return v.(*labels.Matcher), nil
}

func (c *LruMatchersCache) onEvict(_ string, _ *labels.Matcher) {
	c.metrics.evicted.Inc()
	c.metrics.numItems.Set(float64(c.cache.Len()))
}

type matcherCacheMetrics struct {
	requestsTotal prometheus.Counter
	hitsTotal     prometheus.Counter
	numItems      prometheus.Gauge
	maxItems      prometheus.Gauge
	evicted       prometheus.Counter
}

func newMatcherCacheMetrics(reg prometheus.Registerer) *matcherCacheMetrics {
	return &matcherCacheMetrics{
		requestsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_matchers_cache_requests_total",
			Help: "Total number of cache requests for series matchers",
		}),
		hitsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_matchers_cache_hits_total",
			Help: "Total number of cache hits for series matchers",
		}),
		numItems: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_matchers_cache_items",
			Help: "Total number of cached items",
		}),
		maxItems: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_matchers_cache_max_items",
			Help: "Maximum number of items that can be cached",
		}),
		evicted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_matchers_cache_evicted_total",
			Help: "Total number of items evicted from the cache",
		}),
	}
}

// MatchersToPromMatchersCached returns Prometheus matchers from proto matchers.
// Works analogously to MatchersToPromMatchers but uses cache to avoid unnecessary allocations and conversions.
// NOTE: It allocates memory.
func MatchersToPromMatchersCached(cache MatchersCache, ms ...storepb.LabelMatcher) ([]*labels.Matcher, error) {
	res := make([]*labels.Matcher, 0, len(ms))
	for i := range ms {
		pm, err := cache.GetOrSet(ms[i].String(), func() (*labels.Matcher, error) { return storepb.MatcherToPromMatcher(ms[i]) })
		if err != nil {
			return nil, err
		}
		res = append(res, pm)
	}
	return res, nil
}
