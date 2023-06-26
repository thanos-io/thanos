// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/thanos/pkg/cacheutil"
)

const (
	memcachedDefaultTTL = 24 * time.Hour
)

const (
	compressionSchemeStreamedSnappy = "dss"
)

// RemoteIndexCache is a memcached-based index cache.
type RemoteIndexCache struct {
	logger    log.Logger
	memcached cacheutil.RemoteCacheClient

	compressionScheme string

	// Metrics.
	postingRequests         prometheus.Counter
	seriesRequests          prometheus.Counter
	expandedPostingRequests prometheus.Counter
	postingHits             prometheus.Counter
	seriesHits              prometheus.Counter
	expandedPostingHits     prometheus.Counter
}

// NewRemoteIndexCache makes a new RemoteIndexCache.
func NewRemoteIndexCache(logger log.Logger, cacheClient cacheutil.RemoteCacheClient, reg prometheus.Registerer) (*RemoteIndexCache, error) {
	c := &RemoteIndexCache{
		logger:            logger,
		memcached:         cacheClient,
		compressionScheme: compressionSchemeStreamedSnappy, // Hardcode it for now. Expose it once we support different types of compressions.
	}

	requests := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_requests_total",
		Help: "Total number of items requests to the cache.",
	}, []string{"item_type"})
	c.postingRequests = requests.WithLabelValues(cacheTypePostings)
	c.seriesRequests = requests.WithLabelValues(cacheTypeSeries)
	c.expandedPostingRequests = requests.WithLabelValues(cacheTypeExpandedPostings)

	hits := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_hits_total",
		Help: "Total number of items requests to the cache that were a hit.",
	}, []string{"item_type"})
	c.postingHits = hits.WithLabelValues(cacheTypePostings)
	c.seriesHits = hits.WithLabelValues(cacheTypeSeries)
	c.expandedPostingHits = hits.WithLabelValues(cacheTypeExpandedPostings)

	level.Info(logger).Log("msg", "created index cache")

	return c, nil
}

// StorePostings sets the postings identified by the ulid and label to the value v.
// The function enqueues the request and returns immediately: the entry will be
// asynchronously stored in the cache.
func (c *RemoteIndexCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte) {
	key := cacheKey{blockID.String(), cacheKeyPostings(l), c.compressionScheme}.string()
	if err := c.memcached.SetAsync(key, v, memcachedDefaultTTL); err != nil {
		level.Error(c.logger).Log("msg", "failed to cache postings in memcached", "err", err)
	}
}

// FetchMultiPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
// In case of error, it logs and return an empty cache hits map.
func (c *RemoteIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, lbls []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label) {
	keys := make([]string, 0, len(lbls))

	blockIDKey := blockID.String()
	for _, lbl := range lbls {
		key := cacheKey{blockIDKey, cacheKeyPostings(lbl), c.compressionScheme}.string()
		keys = append(keys, key)
	}

	// Fetch the keys from memcached in a single request.
	c.postingRequests.Add(float64(len(keys)))
	results := c.memcached.GetMulti(ctx, keys)
	if len(results) == 0 {
		return nil, lbls
	}

	// Construct the resulting hits map and list of missing keys. We iterate on the input
	// list of labels to be able to easily create the list of ones in a single iteration.
	hits = make(map[labels.Label][]byte, len(results))
	for i, lbl := range lbls {
		// Check if the key has been found in memcached. If not, we add it to the list
		// of missing keys.
		value, ok := results[keys[i]]
		if !ok {
			misses = append(misses, lbl)
			continue
		}

		hits[lbl] = value
	}

	c.postingHits.Add(float64(len(hits)))
	return hits, misses
}

// StoreExpandedPostings sets the postings identified by the ulid and label to the value v.
// The function enqueues the request and returns immediately: the entry will be
// asynchronously stored in the cache.
func (c *RemoteIndexCache) StoreExpandedPostings(blockID ulid.ULID, keys []*labels.Matcher, v []byte) {
	key := cacheKey{blockID.String(), cacheKeyExpandedPostings(labelMatchersToString(keys)), c.compressionScheme}.string()

	if err := c.memcached.SetAsync(key, v, memcachedDefaultTTL); err != nil {
		level.Error(c.logger).Log("msg", "failed to cache expanded postings in memcached", "err", err)
	}
}

// FetchExpandedPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
// In case of error, it logs and return an empty cache hits map.
func (c *RemoteIndexCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, lbls []*labels.Matcher) ([]byte, bool) {
	key := cacheKey{blockID.String(), cacheKeyExpandedPostings(labelMatchersToString(lbls)), c.compressionScheme}.string()

	// Fetch the keys from memcached in a single request.
	c.expandedPostingRequests.Add(1)
	results := c.memcached.GetMulti(ctx, []string{key})
	if len(results) == 0 {
		return nil, false
	}
	if res, ok := results[key]; ok {
		c.expandedPostingHits.Add(1)
		return res, true
	}
	return nil, false
}

// StoreSeries sets the series identified by the ulid and id to the value v.
// The function enqueues the request and returns immediately: the entry will be
// asynchronously stored in the cache.
func (c *RemoteIndexCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte) {
	key := cacheKey{blockID.String(), cacheKeySeries(id), ""}.string()

	if err := c.memcached.SetAsync(key, v, memcachedDefaultTTL); err != nil {
		level.Error(c.logger).Log("msg", "failed to cache series in memcached", "err", err)
	}
}

// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
// and returns a map containing cache hits, along with a list of missing IDs.
// In case of error, it logs and return an empty cache hits map.
func (c *RemoteIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	keys := make([]string, 0, len(ids))

	blockIDKey := blockID.String()
	for _, id := range ids {
		key := cacheKey{blockIDKey, cacheKeySeries(id), ""}.string()
		keys = append(keys, key)
	}

	// Fetch the keys from memcached in a single request.
	c.seriesRequests.Add(float64(len(ids)))
	results := c.memcached.GetMulti(ctx, keys)
	if len(results) == 0 {
		return nil, ids
	}

	// Construct the resulting hits map and list of missing keys. We iterate on the input
	// list of ids to be able to easily create the list of ones in a single iteration.
	hits = make(map[storage.SeriesRef][]byte, len(results))
	for i, id := range ids {
		// Check if the key has been found in memcached. If not, we add it to the list
		// of missing keys.
		value, ok := results[keys[i]]
		if !ok {
			misses = append(misses, id)
			continue
		}

		hits[id] = value
	}

	c.seriesHits.Add(float64(len(hits)))
	return hits, misses
}

// NewMemcachedIndexCache is alias NewRemoteIndexCache for compatible.
func NewMemcachedIndexCache(logger log.Logger, memcached cacheutil.RemoteCacheClient, reg prometheus.Registerer) (*RemoteIndexCache, error) {
	return NewRemoteIndexCache(logger, memcached, reg)
}
