// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/thanos/pkg/tenancy"
)

// MultiLevelIndexCacheConfig defines an ordered hierarchy of index caches.
type MultiLevelIndexCacheConfig struct {
	Caches []IndexCacheConfig `yaml:"caches"`
}

// multiLevelIndexCache queries levels in order and promotes hits to earlier levels.
// Index entries are immutable and keyed by block ID, so promotion does not require
// synchronizing invalidations or expiry across levels. Each backend retains its
// own eviction policy and remote TTL.
type multiLevelIndexCache struct {
	caches        []IndexCache
	commonMetrics *CommonMetrics
}

var _ IndexCache = (*multiLevelIndexCache)(nil)

func newMultiLevelIndexCache(caches []IndexCache, commonMetrics *CommonMetrics) *multiLevelIndexCache {
	for _, itemType := range []string{CacheTypePostings, CacheTypeSeries, CacheTypeExpandedPostings} {
		commonMetrics.RequestTotal.WithLabelValues(itemType, tenancy.DefaultTenant)
		commonMetrics.HitsTotal.WithLabelValues(itemType, tenancy.DefaultTenant)
		commonMetrics.FetchLatency.WithLabelValues(itemType, tenancy.DefaultTenant)
	}
	return &multiLevelIndexCache{caches: caches, commonMetrics: commonMetrics}
}

func (c *multiLevelIndexCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte, tenant string) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(CacheTypePostings, tenant).Observe(float64(len(v)))
	for _, cache := range c.caches {
		cache.StorePostings(blockID, l, v, tenant)
	}
}

func (c *multiLevelIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label, tenant string) (hits map[labels.Label][]byte, misses []labels.Label) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(CacheTypePostings, tenant))
	defer timer.ObserveDuration()
	c.commonMetrics.RequestTotal.WithLabelValues(CacheTypePostings, tenant).Add(float64(len(keys)))

	misses = keys
	for i, cache := range c.caches {
		if len(misses) == 0 || ctx.Err() != nil {
			break
		}
		levelHits, levelMisses := cache.FetchMultiPostings(ctx, blockID, misses, tenant)
		misses = levelMisses
		if hits == nil {
			hits = levelHits
		} else {
			for key, value := range levelHits {
				hits[key] = value
			}
		}
		if i == 0 {
			continue
		}
		for key, value := range levelHits {
			if ctx.Err() != nil {
				break
			}
			for _, earlier := range c.caches[:i] {
				earlier.StorePostings(blockID, key, value, tenant)
			}
		}
	}
	if ctx.Err() != nil {
		// A backend may stop processing a batch when its context is canceled.
		misses = nil
		for _, key := range keys {
			if _, ok := hits[key]; !ok {
				misses = append(misses, key)
			}
		}
	}
	c.commonMetrics.HitsTotal.WithLabelValues(CacheTypePostings, tenant).Add(float64(len(hits)))
	return hits, misses
}

func (c *multiLevelIndexCache) StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte, tenant string) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(CacheTypeExpandedPostings, tenant).Observe(float64(len(v)))
	for _, cache := range c.caches {
		cache.StoreExpandedPostings(blockID, matchers, v, tenant)
	}
}

func (c *multiLevelIndexCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, matchers []*labels.Matcher, tenant string) ([]byte, bool) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(CacheTypeExpandedPostings, tenant))
	defer timer.ObserveDuration()
	c.commonMetrics.RequestTotal.WithLabelValues(CacheTypeExpandedPostings, tenant).Inc()

	for i, cache := range c.caches {
		if ctx.Err() != nil {
			break
		}
		value, ok := cache.FetchExpandedPostings(ctx, blockID, matchers, tenant)
		if !ok {
			continue
		}
		if ctx.Err() == nil {
			for _, earlier := range c.caches[:i] {
				earlier.StoreExpandedPostings(blockID, matchers, value, tenant)
			}
		}
		c.commonMetrics.HitsTotal.WithLabelValues(CacheTypeExpandedPostings, tenant).Inc()
		return value, true
	}
	return nil, false
}

func (c *multiLevelIndexCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte, tenant string) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(CacheTypeSeries, tenant).Observe(float64(len(v)))
	for _, cache := range c.caches {
		cache.StoreSeries(blockID, id, v, tenant)
	}
}

func (c *multiLevelIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef, tenant string) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(CacheTypeSeries, tenant))
	defer timer.ObserveDuration()
	c.commonMetrics.RequestTotal.WithLabelValues(CacheTypeSeries, tenant).Add(float64(len(ids)))

	misses = ids
	for i, cache := range c.caches {
		if len(misses) == 0 || ctx.Err() != nil {
			break
		}
		levelHits, levelMisses := cache.FetchMultiSeries(ctx, blockID, misses, tenant)
		misses = levelMisses
		if hits == nil {
			hits = levelHits
		} else {
			for key, value := range levelHits {
				hits[key] = value
			}
		}
		if i == 0 {
			continue
		}
		for key, value := range levelHits {
			if ctx.Err() != nil {
				break
			}
			for _, earlier := range c.caches[:i] {
				earlier.StoreSeries(blockID, key, value, tenant)
			}
		}
	}
	if ctx.Err() != nil {
		misses = nil
		for _, id := range ids {
			if _, ok := hits[id]; !ok {
				misses = append(misses, id)
			}
		}
	}
	c.commonMetrics.HitsTotal.WithLabelValues(CacheTypeSeries, tenant).Add(float64(len(hits)))
	return hits, misses
}
