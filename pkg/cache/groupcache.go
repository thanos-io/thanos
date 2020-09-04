// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/groupcache"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/thanos/pkg/cacheutil"
)

// Groupcache is a golang/groupcache-based cache.
type Groupcache struct {
	logger log.Logger
	group  *groupcache.Group

	// Metrics: Custom collector!
	// requests prometheus.Counter
	// hits     prometheus.Counter

	// TODO(kakkoyun): Use groupcache stats. A collector maybe?
	// stats := g.group.Stats
	// g.group.CacheStats(groupcache.MainCache)
	// g.group.CacheStats(groupcache.HotCache)
}

func NewGroupcacheCache(logger log.Logger, _ prometheus.Registerer, name string, conf []byte) (*Groupcache, error) {
	config, err := cacheutil.ParseGroupcacheConfig(conf)
	if err != nil {
		return nil, err
	}

	return &Groupcache{
		logger: logger,
		group: groupcache.NewGroup(name, config.CacheBytes, groupcache.GetterFunc(
			func(ctx context.Context, key string, dest groupcache.Sink) error {
				// TODO(kakkoyun): !!
				return nil
			})),
	}, nil
}

// Store data into the cache.
func (g *Groupcache) Store(_ context.Context, _ map[string][]byte, _ time.Duration) {
	// no-op: Groupcache only works read-through only.
}

// Fetch multiple keys from cache. Returns map of input keys to data.
// If key isn't in the map, data for given key was not found.
func (g *Groupcache) Fetch(ctx context.Context, keys []string) map[string][]byte { // TODO(kakkoyun): Make sure strict timeout has passed.
	results := make(map[string][]byte)
	for _, key := range keys {
		var data []byte // TODO(kakkoyun): Use a pool.
		if err := g.group.Get(ctx, key, groupcache.AllocatingByteSliceSink(&data)); err != nil {
			level.Debug(g.logger).Log("msg", "groupcache get", "err", err)
			continue
		}
		results[key] = data
	}
	return results
}
