// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
)

const DefaultCacheSize = 200

type NewItemFunc func(matcher LabelMatcher) (*labels.Matcher, error)

type MatchersCache struct {
	reg     prometheus.Registerer
	cache   *lru.TwoQueueCache[LabelMatcher, *labels.Matcher]
	metrics *matcherCacheMetrics
	size    int
}

type MatcherCacheOption func(*MatchersCache)

func WithPromRegistry(reg prometheus.Registerer) MatcherCacheOption {
	return func(c *MatchersCache) {
		c.reg = reg
	}
}

func WithSize(size int) MatcherCacheOption {
	return func(c *MatchersCache) {
		c.size = size
	}
}

func NewMatchersCache(opts ...MatcherCacheOption) (*MatchersCache, error) {
	cache := &MatchersCache{
		reg:  prometheus.NewRegistry(),
		size: DefaultCacheSize,
	}

	for _, opt := range opts {
		opt(cache)
	}
	cache.metrics = newMatcherCacheMetrics(cache.reg)

	lruCache, err := lru.New2Q[LabelMatcher, *labels.Matcher](cache.size)
	if err != nil {
		return nil, err
	}
	cache.cache = lruCache

	return cache, nil
}

func (c *MatchersCache) GetOrSet(key LabelMatcher, newItem NewItemFunc) (*labels.Matcher, error) {
	c.metrics.requestsTotal.Inc()
	if item, ok := c.cache.Get(key); ok {
		c.metrics.hitsTotal.Inc()
		return item, nil
	}

	item, err := newItem(key)
	if err != nil {
		return nil, err
	}
	c.cache.Add(key, item)
	c.metrics.numItems.Set(float64(c.cache.Len()))

	return item, nil
}

type matcherCacheMetrics struct {
	requestsTotal prometheus.Counter
	hitsTotal     prometheus.Counter
	numItems      prometheus.Gauge
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
	}
}
