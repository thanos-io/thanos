// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/tenancy"
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

	ttl time.Duration

	// Metrics.
	requestTotal  *prometheus.CounterVec
	hitsTotal     *prometheus.CounterVec
	dataSizeBytes *prometheus.HistogramVec
	fetchLatency  *prometheus.HistogramVec
}

// NewRemoteIndexCache makes a new RemoteIndexCache.
func NewRemoteIndexCache(logger log.Logger, cacheClient cacheutil.RemoteCacheClient, commonMetrics *CommonMetrics, reg prometheus.Registerer, ttl time.Duration) (*RemoteIndexCache, error) {
	c := &RemoteIndexCache{
		ttl:               ttl,
		logger:            logger,
		memcached:         cacheClient,
		compressionScheme: compressionSchemeStreamedSnappy, // Hardcode it for now. Expose it once we support different types of compressions.
	}

	if commonMetrics == nil {
		commonMetrics = NewCommonMetrics(reg)
	}

	c.requestTotal = commonMetrics.RequestTotal
	c.hitsTotal = commonMetrics.HitsTotal
	c.dataSizeBytes = commonMetrics.DataSizeBytes
	c.fetchLatency = commonMetrics.FetchLatency

	// Init requestTtotal and hitsTotal with default tenant
	c.requestTotal.WithLabelValues(CacheTypePostings, tenancy.DefaultTenant)
	c.requestTotal.WithLabelValues(CacheTypeSeries, tenancy.DefaultTenant)
	c.requestTotal.WithLabelValues(CacheTypeExpandedPostings, tenancy.DefaultTenant)

	c.hitsTotal.WithLabelValues(CacheTypePostings, tenancy.DefaultTenant)
	c.hitsTotal.WithLabelValues(CacheTypeSeries, tenancy.DefaultTenant)
	c.hitsTotal.WithLabelValues(CacheTypeExpandedPostings, tenancy.DefaultTenant)

	c.fetchLatency.WithLabelValues(CacheTypePostings, tenancy.DefaultTenant)
	c.fetchLatency.WithLabelValues(CacheTypeSeries, tenancy.DefaultTenant)
	c.fetchLatency.WithLabelValues(CacheTypeExpandedPostings, tenancy.DefaultTenant)

	level.Info(logger).Log("msg", "created index cache")

	return c, nil
}

// StorePostings sets the postings identified by the ulid and label to the value v.
// The function enqueues the request and returns immediately: the entry will be
// asynchronously stored in the cache.
func (c *RemoteIndexCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte, tenant string) {
	c.dataSizeBytes.WithLabelValues(CacheTypePostings, tenant).Observe(float64(len(v)))
	key := CacheKey{blockID.String(), CacheKeyPostings(l), c.compressionScheme}.String()
	if err := c.memcached.SetAsync(key, v, c.ttl); err != nil {
		level.Error(c.logger).Log("msg", "failed to cache postings in memcached", "err", err)
	}
}

// FetchMultiPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
// In case of error, it logs and return an empty cache hits map.
func (c *RemoteIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, lbls []labels.Label, tenant string) (hits [][]byte, misses []uint64) {
	timer := prometheus.NewTimer(c.fetchLatency.WithLabelValues(CacheTypePostings, tenant))
	defer timer.ObserveDuration()
	hits = make([][]byte, len(lbls))
	misses = make([]uint64, 0, len(lbls))

	keys := make([]string, 0, len(lbls))

	blockIDKey := blockID.String()
	for _, lbl := range lbls {
		key := CacheKey{blockIDKey, CacheKeyPostings(lbl), c.compressionScheme}.String()
		keys = append(keys, key)
	}

	// Fetch the keys from memcached in a single request.
	c.requestTotal.WithLabelValues(CacheTypePostings, tenant).Add(float64(len(keys)))

	results := c.memcached.GetMulti(ctx, keys)
	if len(results) == 0 {

		for i := range lbls {
			misses = append(misses, uint64(i))
		}
		return hits, misses
	}

	// Construct the resulting hits map and list of missing keys. We iterate on the input
	// list of labels to be able to easily create the list of ones in a single iteration.

	for i := range lbls {
		// Check if the key has been found in memcached. If not, we add it to the list
		// of missing keys.
		if value, ok := results[keys[i]]; ok {
			hits[i] = value
		} else {
			misses = append(misses, uint64(i))
		}
	}
	c.hitsTotal.WithLabelValues(CacheTypePostings, tenant).Add(float64(len(lbls) - len(misses)))
	return hits, misses
}

// StoreExpandedPostings sets the postings identified by the ulid and label to the value v.
// The function enqueues the request and returns immediately: the entry will be
// asynchronously stored in the cache.
func (c *RemoteIndexCache) StoreExpandedPostings(blockID ulid.ULID, keys []*labels.Matcher, v []byte, tenant string) {
	c.dataSizeBytes.WithLabelValues(CacheTypeExpandedPostings, tenant).Observe(float64(len(v)))
	key := CacheKey{blockID.String(), CacheKeyExpandedPostings(LabelMatchersToString(keys)), c.compressionScheme}.String()

	if err := c.memcached.SetAsync(key, v, c.ttl); err != nil {
		level.Error(c.logger).Log("msg", "failed to cache expanded postings in memcached", "err", err)
	}
}

// FetchExpandedPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
// In case of error, it logs and return an empty cache hits map.
func (c *RemoteIndexCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, lbls []*labels.Matcher, tenant string) ([]byte, bool) {
	timer := prometheus.NewTimer(c.fetchLatency.WithLabelValues(CacheTypeExpandedPostings, tenant))
	defer timer.ObserveDuration()

	key := CacheKey{blockID.String(), CacheKeyExpandedPostings(LabelMatchersToString(lbls)), c.compressionScheme}.String()

	// Fetch the keys from memcached in a single request.
	c.requestTotal.WithLabelValues(CacheTypeExpandedPostings, tenant).Add(1)
	results := c.memcached.GetMulti(ctx, []string{key})
	if len(results) == 0 {
		return nil, false
	}
	if res, ok := results[key]; ok {
		c.hitsTotal.WithLabelValues(CacheTypeExpandedPostings, tenant).Add(1)
		return res, true
	}
	return nil, false
}

// StoreSeries sets the series identified by the ulid and id to the value v.
// The function enqueues the request and returns immediately: the entry will be
// asynchronously stored in the cache.
func (c *RemoteIndexCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte, tenant string) {
	c.dataSizeBytes.WithLabelValues(CacheTypeSeries, tenant).Observe(float64(len(v)))
	key := CacheKey{blockID.String(), CacheKeySeries(id), ""}.String()

	if err := c.memcached.SetAsync(key, v, c.ttl); err != nil {
		level.Error(c.logger).Log("msg", "failed to cache series in memcached", "err", err)
	}
}

// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
// and returns a map containing cache hits, along with a list of missing IDs.
// In case of error, it logs and return an empty cache hits map.
func (c *RemoteIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef, tenant string) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	timer := prometheus.NewTimer(c.fetchLatency.WithLabelValues(CacheTypeSeries, tenant))
	defer timer.ObserveDuration()

	keys := make([]string, 0, len(ids))

	blockIDKey := blockID.String()
	for _, id := range ids {
		key := CacheKey{blockIDKey, CacheKeySeries(id), ""}.String()
		keys = append(keys, key)
	}

	// Fetch the keys from memcached in a single request.
	c.requestTotal.WithLabelValues(CacheTypeSeries, tenant).Add(float64(len(ids)))
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
	c.hitsTotal.WithLabelValues(CacheTypeSeries, tenant).Add(float64(len(hits)))
	return hits, misses
}

// NewMemcachedIndexCache is alias NewRemoteIndexCache for compatible.
func NewMemcachedIndexCache(logger log.Logger, memcached cacheutil.RemoteCacheClient, reg prometheus.Registerer) (*RemoteIndexCache, error) {
	return NewRemoteIndexCache(logger, memcached, nil, reg, memcachedDefaultTTL)
}
