// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/golang/groupcache"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
)

// GroupcacheIndexCache is a golang/groupcache-based index cache.
type GroupcacheIndexCache struct {
	logger log.Logger

	postings *groupcache.Group
	series   *groupcache.Group

	// // Metrics. ??
	// requests *prometheus.CounterVec
	// hits     *prometheus.CounterVec
}

func NewGroupcacheCache(logger log.Logger, _ prometheus.Registerer) *GroupcacheIndexCache {
	return &GroupcacheIndexCache{logger: logger}
}

// StorePostings stores postings for a single series.
func (g *GroupcacheIndexCache) StorePostings(_ context.Context, _ ulid.ULID, _ labels.Label, _ []byte) {
	// no-op: Groupcache only works read-through only
}

// FetchMultiPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
func (g *GroupcacheIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, lbls []labels.Label) (map[labels.Label][]byte, []labels.Label) {
	hits := make(map[labels.Label][]byte)
	var misses []labels.Label
	for _, l := range lbls {
		key := cacheKey{blockID, cacheKeyPostings(l)}.string()
		var data []byte // TODO(kakkayun): Use a pool.
		if err := g.series.Get(ctx, key, groupcache.AllocatingByteSliceSink(&data)); err != nil {
			misses = append(misses, l)
		}
		hits[l] = data
	}
	return hits, misses
}

// StoreSeries stores a single series.
func (g *GroupcacheIndexCache) StoreSeries(_ context.Context, _ ulid.ULID, _ uint64, _ []byte) {
	// no-op: Groupcache only works read-through only
}

// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
// and returns a map containing cache hits, along with a list of missing IDs.
func (g *GroupcacheIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []uint64) (map[uint64][]byte, []uint64) {
	hits := make(map[uint64][]byte)
	var misses []uint64
	for _, id := range ids {
		key := cacheKey{blockID, cacheKeySeries(id)}.string()
		var data []byte // TODO(kakkoyun): Use a pool.
		if err := g.series.Get(ctx, key, groupcache.AllocatingByteSliceSink(&data)); err != nil {
			misses = append(misses, id)
		}
		hits[id] = data
	}
	return hits, misses
}
