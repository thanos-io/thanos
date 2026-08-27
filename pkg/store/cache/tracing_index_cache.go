// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/thanos/pkg/tracing"
)

type TracingIndexCache struct {
	name  string
	cache IndexCache
}

// NewTracingIndexCache creates an index cache wrapper with traces instrumentation.
func NewTracingIndexCache(name string, cache IndexCache) *TracingIndexCache {
	return &TracingIndexCache{name: name, cache: cache}
}

// StorePostings stores postings for a single series.
func (c *TracingIndexCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte, tenant string, ttl time.Duration) {
	c.cache.StorePostings(blockID, l, v, tenant, ttl)
}

// FetchMultiPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
func (c *TracingIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label, tenant string) (hits map[labels.Label][]byte, misses []labels.Label) {
	span, newCtx := tracing.StartSpan(ctx, "fetch_multi_postings", tracing.Tags{
		"name":      c.name,
		"block.id":  blockID.String(),
		"requested": len(keys),
	})
	defer span.Finish()
	hits, misses = c.cache.FetchMultiPostings(newCtx, blockID, keys, tenant)
	span.SetTag("hits", len(hits))
	dataBytes := 0
	for _, v := range hits {
		dataBytes += len(v)
	}
	span.SetTag("bytes", dataBytes)
	return hits, misses
}

// StoreExpandedPostings stores expanded postings for a set of label matchers.
func (c *TracingIndexCache) StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte, tenant string, ttl time.Duration) {
	c.cache.StoreExpandedPostings(blockID, matchers, v, tenant, ttl)
}

// FetchExpandedPostings fetches expanded postings and returns cached data and a boolean value representing whether it is a cache hit or not.
func (c *TracingIndexCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, matchers []*labels.Matcher, tenant string) (data []byte, exists bool) {
	span, newCtx := tracing.StartSpan(ctx, "fetch_expanded_postings", tracing.Tags{
		"name":     c.name,
		"block.id": blockID.String(),
	})
	defer span.Finish()
	data, exists = c.cache.FetchExpandedPostings(newCtx, blockID, matchers, tenant)
	if exists {
		span.SetTag("bytes", len(data))
	}
	return data, exists
}

// StoreSeries stores a single series. Skip instrumenting this method
// excessive spans as a single request can store millions of series.
func (c *TracingIndexCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte, tenant string, ttl time.Duration) {
	c.cache.StoreSeries(blockID, id, v, tenant, ttl)
}

// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
// and returns a map containing cache hits, along with a list of missing IDs.
func (c *TracingIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef, tenant string) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	span, newCtx := tracing.StartSpan(ctx, "fetch_multi_series", tracing.Tags{
		"name":      c.name,
		"block.id":  blockID.String(),
		"requested": len(ids),
	})
	defer span.Finish()
	hits, misses = c.cache.FetchMultiSeries(newCtx, blockID, ids, tenant)
	span.SetTag("hits", len(hits))
	dataBytes := 0
	for _, v := range hits {
		dataBytes += len(v)
	}
	span.SetTag("bytes", dataBytes)
	return hits, misses
}
