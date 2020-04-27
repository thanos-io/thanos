// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/thanos-io/thanos/pkg/cacheutil"
)

// MemcachedCache is a memcached-based cache.
type MemcachedCache struct {
	logger    log.Logger
	memcached cacheutil.MemcachedClient

	// Metrics.
	requests prometheus.Counter
	hits     prometheus.Counter
}

// NewMemcachedCache makes a new MemcachedCache.
func NewMemcachedCache(logger log.Logger, memcached cacheutil.MemcachedClient, reg prometheus.Registerer) *MemcachedCache {
	c := &MemcachedCache{
		logger:    logger,
		memcached: memcached,
	}

	c.requests = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_store_memcached_requests_total",
		Help: "Total number of items requests to the memcached.",
	})

	c.hits = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_store_memcached_hits_total",
		Help: "Total number of items requests to the cache that were a hit.",
	})

	level.Info(logger).Log("msg", "created memcached cache")

	return c
}

// Store data identified by keys.
// The function enqueues the request and returns immediately: the entry will be
// asynchronously stored in the cache.
func (c *MemcachedCache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	for key, val := range data {
		if err := c.memcached.SetAsync(ctx, key, val, ttl); err != nil {
			level.Error(c.logger).Log("msg", "failed to cache postings in memcached", "err", err)
		}
	}
}

// FetchMultiPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
// In case of error, it logs and return an empty cache hits map.
func (c *MemcachedCache) Fetch(ctx context.Context, keys []string) (map[string][]byte, []string) {
	// Fetch the keys from memcached in a single request.
	c.requests.Add(float64(len(keys)))
	results := c.memcached.GetMulti(ctx, keys)
	if len(results) == 0 {
		return nil, keys
	}

	// Construct the resulting hits map and list of missing keys. We iterate on the input
	// list of labels to be able to easily create the list of ones in a single iteration.
	var misses []string
	for _, key := range keys {
		_, ok := results[key]
		if !ok {
			misses = append(misses, key)
			continue
		}
	}

	c.hits.Add(float64(len(results)))
	return results, misses
}
