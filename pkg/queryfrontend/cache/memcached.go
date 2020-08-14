// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"time"

	cortexcache "github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/cacheutil"
)

const (
	memcachedDefaultTTL = 24 * time.Hour
)

// MemcachedCache is a memcached-based cache.
type MemcachedCache struct {
	logger    log.Logger
	memcached cacheutil.MemcachedClient
	validity  time.Duration

	// Metrics.
	requests prometheus.Counter
	hits     prometheus.Counter
}

type MemcachedResponseCacheConfig struct {
	Memcached cacheutil.MemcachedClientConfig `yaml:",inline"`
	Validity  time.Duration                   `yaml:"validity"`
}

// newMemcachedCache makes a new MemcachedCache and returns Cortex ResultsCacheConfig.
func newMemcachedCache(conf []byte, logger log.Logger, reg prometheus.Registerer) (*queryrange.ResultsCacheConfig, error) {
	var config MemcachedResponseCacheConfig
	if err := yaml.UnmarshalStrict(conf, &config); err != nil {
		return nil, err
	}

	memcachedClientConfig, err := yaml.Marshal(config.Memcached)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of memcached client configuration")
	}

	memcached, err := cacheutil.NewMemcachedClient(logger, "response-cache", memcachedClientConfig, reg)
	if err != nil {
		return nil, errors.Wrap(err, "create memcached client")
	}

	c := &MemcachedCache{
		logger:    logger,
		memcached: memcached,
		validity:  config.Validity,
	}

	if c.validity == 0 {
		c.validity = memcachedDefaultTTL

		level.Info(logger).Log("msg", "memcached cache valid time set to 0, use 24 hours instead")
	}

	c.requests = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_cache_memcached_requests_total",
		Help: "Total number of items requests to memcached.",
	})

	c.hits = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_cache_memcached_hits_total",
		Help: "Total number of items requests to the cache that were a hit.",
	})

	level.Info(logger).Log("msg", "created memcached cache")

	return &queryrange.ResultsCacheConfig{
		CacheConfig: cortexcache.Config{
			Cache: c,
		},
	}, nil
}

// Store data identified by keys.
// The function enqueues the request and returns immediately: the entry will be
// asynchronously stored in the cache.
func (c *MemcachedCache) Store(ctx context.Context, keys []string, bufs [][]byte) {
	var (
		firstErr error
		failed   int
	)

	for i, key := range keys {
		if err := c.memcached.SetAsync(ctx, key, bufs[i], c.validity); err != nil {
			failed++
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if firstErr != nil {
		level.Warn(c.logger).Log("msg", "failed to store one or more items into memcached", "failed", failed, "firstErr", firstErr)
	}
}

func (c *MemcachedCache) Fetch(ctx context.Context, keys []string) ([]string, [][]byte, []string) {
	found := make([]string, 0, len(keys))
	missed := make([]string, 0)
	bufs := make([][]byte, 0, len(keys))

	c.requests.Add(float64(len(keys)))
	items := c.memcached.GetMulti(ctx, keys)
	c.hits.Add(float64(len(items)))

	for _, key := range keys {
		item, ok := items[key]
		if ok {
			found = append(found, key)
			bufs = append(bufs, item)
		} else {
			missed = append(missed, key)
		}
	}

	return found, bufs, missed
}

func (c *MemcachedCache) Stop() {
	c.memcached.Stop()
}
