// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
)

const CachedMatcherTTL = 5 * time.Minute

type NewItemFunc func(matcher LabelMatcher) (*labels.Matcher, error)

type itemExpiration struct {
	mu        sync.Mutex
	expiresAt time.Time
}

func (e *itemExpiration) setExpiration(t time.Time) {
	e.mu.Lock()
	e.expiresAt = t
	e.mu.Unlock()
}

func newExpiration(expiresAt time.Time) *itemExpiration {
	return &itemExpiration{
		expiresAt: expiresAt,
	}
}

type MatchersCache struct {
	reg *prometheus.Registry
	now func() time.Time

	mu       sync.RWMutex
	ttl      time.Duration
	itemTTLs map[LabelMatcher]*itemExpiration
	cache    map[LabelMatcher]*labels.Matcher
	metrics  *matcherCacheMetrics
}

type MatcherCacheOption func(*MatchersCache)

func WithNowFunc(now func() time.Time) MatcherCacheOption {
	return func(c *MatchersCache) {
		c.now = now
	}
}

func WithPromRegistry(reg *prometheus.Registry) MatcherCacheOption {
	return func(c *MatchersCache) {
		c.reg = reg
	}
}

func NewMatchersCache(opts ...MatcherCacheOption) *MatchersCache {
	cache := &MatchersCache{
		reg: prometheus.NewRegistry(),
		now: time.Now,
		mu:  sync.RWMutex{},
		// This TTL should be sufficient to allow caching matchers for alerting queries.
		ttl:      CachedMatcherTTL,
		itemTTLs: make(map[LabelMatcher]*itemExpiration),
		cache:    make(map[LabelMatcher]*labels.Matcher),
	}

	for _, opt := range opts {
		opt(cache)
	}
	cache.metrics = newMatcherCacheMetrics(cache.reg)

	return cache
}

func (c *MatchersCache) GetOrSet(key LabelMatcher, newItem NewItemFunc) (*labels.Matcher, error) {
	expirationTime := c.now().Add(c.ttl)

	c.metrics.requestsTotal.Inc()
	c.mu.RLock()
	if item, ok := c.cache[key]; ok {
		c.metrics.hitsTotal.Inc()
		c.itemTTLs[key].setExpiration(expirationTime)
		c.mu.RUnlock()
		return item, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if item, ok := c.cache[key]; ok {
		c.metrics.hitsTotal.Inc()
		c.itemTTLs[key].setExpiration(expirationTime)
		return item, nil
	}

	item, err := newItem(key)
	if err != nil {
		return nil, err
	}
	c.cache[key] = item
	c.itemTTLs[key] = newExpiration(expirationTime)
	c.metrics.numItems.Inc()

	return item, nil
}

func (c *MatchersCache) RemoveExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	for key, expiration := range c.itemTTLs {
		if expiration.expiresAt.Before(now) {
			c.metrics.numItems.Dec()
			c.metrics.evictionsTotal.Inc()
			delete(c.cache, key)
			delete(c.itemTTLs, key)
		}
	}
}

type matcherCacheMetrics struct {
	requestsTotal  prometheus.Counter
	hitsTotal      prometheus.Counter
	evictionsTotal prometheus.Counter
	numItems       prometheus.Gauge
}

func newMatcherCacheMetrics(reg *prometheus.Registry) *matcherCacheMetrics {
	return &matcherCacheMetrics{
		requestsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "matchers_cache_requests_total",
			Help: "Total number of cache requests for series matchers",
		}),
		hitsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "matchers_cache_hits_total",
			Help: "Total number of cache hits for series matchers",
		}),
		evictionsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "matchers_cache_evictions_total",
			Help: "Total number of cache evictions",
		}),
		numItems: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "matchers_cache_items",
			Help: "Total number of cached items",
		}),
	}
}
