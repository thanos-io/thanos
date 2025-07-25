// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	lru "github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/oklog/ulid/v2"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/tenancy"
)

var (
	DefaultInMemoryIndexCacheConfig = InMemoryIndexCacheConfig{
		MaxSize:     250 * 1024 * 1024,
		MaxItemSize: 125 * 1024 * 1024,
	}
)

const (
	maxInt = int(^uint(0) >> 1)

	// checkContextEveryNIterations is used in some tight loops to check if the context is done.
	checkContextEveryNIterations = 128
)

type InMemoryIndexCache struct {
	mtx sync.Mutex

	logger           log.Logger
	lru              *lru.LRU[CacheKey, []byte]
	maxSizeBytes     uint64
	maxItemSizeBytes uint64

	curSize uint64

	evicted          *prometheus.CounterVec
	added            *prometheus.CounterVec
	current          *prometheus.GaugeVec
	currentSize      *prometheus.GaugeVec
	totalCurrentSize *prometheus.GaugeVec
	overflow         *prometheus.CounterVec

	commonMetrics *CommonMetrics
}

// InMemoryIndexCacheConfig holds the in-memory index cache config.
type InMemoryIndexCacheConfig struct {
	// MaxSize represents overall maximum number of bytes cache can contain.
	MaxSize model.Bytes `yaml:"max_size"`
	// MaxItemSize represents maximum size of single item.
	MaxItemSize model.Bytes `yaml:"max_item_size"`
}

// parseInMemoryIndexCacheConfig unmarshals a buffer into a InMemoryIndexCacheConfig with default values.
func parseInMemoryIndexCacheConfig(conf []byte) (InMemoryIndexCacheConfig, error) {
	config := DefaultInMemoryIndexCacheConfig
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return InMemoryIndexCacheConfig{}, err
	}

	return config, nil
}

// NewInMemoryIndexCache creates a new thread-safe LRU cache for index entries and ensures the total cache
// size approximately does not exceed maxBytes.
func NewInMemoryIndexCache(logger log.Logger, commonMetrics *CommonMetrics, reg prometheus.Registerer, conf []byte) (*InMemoryIndexCache, error) {
	config, err := parseInMemoryIndexCacheConfig(conf)
	if err != nil {
		return nil, err
	}

	return NewInMemoryIndexCacheWithConfig(logger, commonMetrics, reg, config)
}

// NewInMemoryIndexCacheWithConfig creates a new thread-safe LRU cache for index entries and ensures the total cache
// size approximately does not exceed maxBytes.
func NewInMemoryIndexCacheWithConfig(logger log.Logger, commonMetrics *CommonMetrics, reg prometheus.Registerer, config InMemoryIndexCacheConfig) (*InMemoryIndexCache, error) {
	if config.MaxItemSize > config.MaxSize {
		return nil, errors.Errorf("max item size (%v) cannot be bigger than overall cache size (%v)", config.MaxItemSize, config.MaxSize)
	}

	if commonMetrics == nil {
		commonMetrics = NewCommonMetrics(reg)
	}

	c := &InMemoryIndexCache{
		logger:           logger,
		maxSizeBytes:     uint64(config.MaxSize),
		maxItemSizeBytes: uint64(config.MaxItemSize),
		commonMetrics:    commonMetrics,
	}

	c.evicted = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_evicted_total",
		Help: "Total number of items that were evicted from the index cache.",
	}, []string{"item_type"})
	c.evicted.WithLabelValues(CacheTypePostings)
	c.evicted.WithLabelValues(CacheTypeSeries)
	c.evicted.WithLabelValues(CacheTypeExpandedPostings)

	c.added = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_added_total",
		Help: "Total number of items that were added to the index cache.",
	}, []string{"item_type"})
	c.added.WithLabelValues(CacheTypePostings)
	c.added.WithLabelValues(CacheTypeSeries)
	c.added.WithLabelValues(CacheTypeExpandedPostings)

	c.commonMetrics.RequestTotal.WithLabelValues(CacheTypePostings, tenancy.DefaultTenant)
	c.commonMetrics.RequestTotal.WithLabelValues(CacheTypeSeries, tenancy.DefaultTenant)
	c.commonMetrics.RequestTotal.WithLabelValues(CacheTypeExpandedPostings, tenancy.DefaultTenant)

	c.overflow = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_overflowed_total",
		Help: "Total number of items that could not be added to the cache due to being too big.",
	}, []string{"item_type"})
	c.overflow.WithLabelValues(CacheTypePostings)
	c.overflow.WithLabelValues(CacheTypeSeries)
	c.overflow.WithLabelValues(CacheTypeExpandedPostings)

	c.commonMetrics.HitsTotal.WithLabelValues(CacheTypePostings, tenancy.DefaultTenant)
	c.commonMetrics.HitsTotal.WithLabelValues(CacheTypeSeries, tenancy.DefaultTenant)
	c.commonMetrics.HitsTotal.WithLabelValues(CacheTypeExpandedPostings, tenancy.DefaultTenant)

	c.current = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_items",
		Help: "Current number of items in the index cache.",
	}, []string{"item_type"})
	c.current.WithLabelValues(CacheTypePostings)
	c.current.WithLabelValues(CacheTypeSeries)
	c.current.WithLabelValues(CacheTypeExpandedPostings)

	c.currentSize = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_items_size_bytes",
		Help: "Current byte size of items in the index cache.",
	}, []string{"item_type"})
	c.currentSize.WithLabelValues(CacheTypePostings)
	c.currentSize.WithLabelValues(CacheTypeSeries)
	c.currentSize.WithLabelValues(CacheTypeExpandedPostings)

	c.totalCurrentSize = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_total_size_bytes",
		Help: "Current byte size of items (both value and key) in the index cache.",
	}, []string{"item_type"})
	c.totalCurrentSize.WithLabelValues(CacheTypePostings)
	c.totalCurrentSize.WithLabelValues(CacheTypeSeries)
	c.totalCurrentSize.WithLabelValues(CacheTypeExpandedPostings)

	_ = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_max_size_bytes",
		Help: "Maximum number of bytes to be held in the index cache.",
	}, func() float64 {
		return float64(c.maxSizeBytes)
	})
	_ = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_max_item_size_bytes",
		Help: "Maximum number of bytes for single entry to be held in the index cache.",
	}, func() float64 {
		return float64(c.maxItemSizeBytes)
	})

	// Initialize LRU cache with a high size limit since we will manage evictions ourselves
	// based on stored size using `RemoveOldest` method.
	l, err := lru.NewLRU[CacheKey, []byte](maxInt, c.onEvict)
	if err != nil {
		return nil, err
	}
	c.lru = l

	level.Info(logger).Log(
		"msg", "created in-memory index cache",
		"maxItemSizeBytes", c.maxItemSizeBytes,
		"maxSizeBytes", c.maxSizeBytes,
		"maxItems", "maxInt",
	)
	return c, nil
}

func (c *InMemoryIndexCache) onEvict(key CacheKey, val []byte) {
	k := key.KeyType()
	entrySize := sliceHeaderSize + uint64(len(val))

	c.evicted.WithLabelValues(k).Inc()
	c.current.WithLabelValues(k).Dec()
	c.currentSize.WithLabelValues(k).Sub(float64(entrySize))
	c.totalCurrentSize.WithLabelValues(k).Sub(float64(entrySize + key.Size()))

	c.curSize -= entrySize
}

func (c *InMemoryIndexCache) get(key CacheKey) ([]byte, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	v, ok := c.lru.Get(key)
	if !ok {
		return nil, false
	}
	return v, true
}

func (c *InMemoryIndexCache) set(typ string, key CacheKey, val []byte) {
	var size = sliceHeaderSize + uint64(len(val))

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, ok := c.lru.Get(key); ok {
		return
	}

	if !c.ensureFits(size, typ) {
		c.overflow.WithLabelValues(typ).Inc()
		return
	}

	// The caller may be passing in a sub-slice of a huge array. Copy the data
	// to ensure we don't waste huge amounts of space for something small.
	v := make([]byte, len(val))
	copy(v, val)
	c.lru.Add(key, v)

	c.added.WithLabelValues(typ).Inc()
	c.currentSize.WithLabelValues(typ).Add(float64(size))
	c.totalCurrentSize.WithLabelValues(typ).Add(float64(size + key.Size()))
	c.current.WithLabelValues(typ).Inc()
	c.curSize += size
}

// ensureFits tries to make sure that the passed slice will fit into the LRU cache.
// Returns true if it will fit.
func (c *InMemoryIndexCache) ensureFits(size uint64, typ string) bool {
	if size > c.maxItemSizeBytes {
		level.Debug(c.logger).Log(
			"msg", "item bigger than maxItemSizeBytes. Ignoring..",
			"maxItemSizeBytes", c.maxItemSizeBytes,
			"maxSizeBytes", c.maxSizeBytes,
			"curSize", c.curSize,
			"itemSize", size,
			"cacheType", typ,
		)
		return false
	}

	for c.curSize+size > c.maxSizeBytes {
		if _, _, ok := c.lru.RemoveOldest(); !ok {
			level.Error(c.logger).Log(
				"msg", "LRU has nothing more to evict, but we still cannot allocate the item. Resetting cache.",
				"maxItemSizeBytes", c.maxItemSizeBytes,
				"maxSizeBytes", c.maxSizeBytes,
				"curSize", c.curSize,
				"itemSize", size,
				"cacheType", typ,
			)
			c.reset()
		}
	}
	return true
}

func (c *InMemoryIndexCache) reset() {
	c.lru.Purge()
	c.current.Reset()
	c.currentSize.Reset()
	c.totalCurrentSize.Reset()
	c.curSize = 0
}

func copyString(s string) string {
	return string(unsafe.Slice(unsafe.StringData(s), len(s)))
}

// copyToKey is required as underlying strings might be mmaped.
func copyToKey(l labels.Label) CacheKeyPostings {
	return CacheKeyPostings(labels.Label{Value: copyString(l.Value), Name: copyString(l.Name)})
}

// StorePostings sets the postings identified by the ulid and label to the value v,
// if the postings already exists in the cache it is not mutated.
func (c *InMemoryIndexCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte, tenant string, _ time.Duration) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(CacheTypePostings, tenant).Observe(float64(len(v)))
	c.set(CacheTypePostings, CacheKey{Block: blockID.String(), Key: copyToKey(l)}, v)
}

// FetchMultiPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
func (c *InMemoryIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label, tenant string) (hits map[labels.Label][]byte, misses []labels.Label) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(CacheTypePostings, tenant))
	defer timer.ObserveDuration()

	hits = map[labels.Label][]byte{}

	blockIDKey := blockID.String()
	requests := 0
	hit := 0
	for i, key := range keys {
		if (i+1)%checkContextEveryNIterations == 0 {
			if ctx.Err() != nil {
				c.commonMetrics.RequestTotal.WithLabelValues(CacheTypePostings, tenant).Add(float64(requests))
				c.commonMetrics.HitsTotal.WithLabelValues(CacheTypePostings, tenant).Add(float64(hit))
				return hits, misses
			}
		}
		requests++
		if b, ok := c.get(CacheKey{blockIDKey, CacheKeyPostings(key), ""}); ok {
			hit++
			hits[key] = b
			continue
		}

		misses = append(misses, key)
	}
	c.commonMetrics.RequestTotal.WithLabelValues(CacheTypePostings, tenant).Add(float64(requests))
	c.commonMetrics.HitsTotal.WithLabelValues(CacheTypePostings, tenant).Add(float64(hit))

	return hits, misses
}

// StoreExpandedPostings stores expanded postings for a set of label matchers.
func (c *InMemoryIndexCache) StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte, tenant string, _ time.Duration) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(CacheTypeExpandedPostings, tenant).Observe(float64(len(v)))
	c.set(CacheTypeExpandedPostings, CacheKey{Block: blockID.String(), Key: CacheKeyExpandedPostings(LabelMatchersToString(matchers))}, v)
}

// FetchExpandedPostings fetches expanded postings and returns cached data and a boolean value representing whether it is a cache hit or not.
func (c *InMemoryIndexCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, matchers []*labels.Matcher, tenant string) ([]byte, bool) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(CacheTypeExpandedPostings, tenant))
	defer timer.ObserveDuration()

	if ctx.Err() != nil {
		return nil, false
	}
	c.commonMetrics.RequestTotal.WithLabelValues(CacheTypeExpandedPostings, tenant).Inc()
	if b, ok := c.get(CacheKey{blockID.String(), CacheKeyExpandedPostings(LabelMatchersToString(matchers)), ""}); ok {
		c.commonMetrics.HitsTotal.WithLabelValues(CacheTypeExpandedPostings, tenant).Inc()
		return b, true
	}
	return nil, false
}

// StoreSeries sets the series identified by the ulid and id to the value v,
// if the series already exists in the cache it is not mutated.
func (c *InMemoryIndexCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte, tenant string, _ time.Duration) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(CacheTypeSeries, tenant).Observe(float64(len(v)))
	c.set(CacheTypeSeries, CacheKey{blockID.String(), CacheKeySeries(id), ""}, v)
}

// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
// and returns a map containing cache hits, along with a list of missing IDs.
func (c *InMemoryIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef, tenant string) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(CacheTypeSeries, tenant))
	defer timer.ObserveDuration()

	hits = map[storage.SeriesRef][]byte{}

	blockIDKey := blockID.String()
	requests := 0
	hit := 0
	for i, id := range ids {
		if (i+1)%checkContextEveryNIterations == 0 {
			if ctx.Err() != nil {
				c.commonMetrics.RequestTotal.WithLabelValues(CacheTypeSeries, tenant).Add(float64(requests))
				c.commonMetrics.HitsTotal.WithLabelValues(CacheTypeSeries, tenant).Add(float64(hit))
				return hits, misses
			}
		}
		requests++
		if b, ok := c.get(CacheKey{blockIDKey, CacheKeySeries(id), ""}); ok {
			hit++
			hits[id] = b
			continue
		}

		misses = append(misses, id)
	}
	c.commonMetrics.RequestTotal.WithLabelValues(CacheTypeSeries, tenant).Add(float64(requests))
	c.commonMetrics.HitsTotal.WithLabelValues(CacheTypeSeries, tenant).Add(float64(hit))

	return hits, misses
}
