package store

import (
	"sync"

	lru "github.com/hashicorp/golang-lru/simplelru"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/labels"
)

const (
	cacheTypePostings = "postings"
	cacheTypeSeries   = "series"
)

type cacheItem struct {
	block ulid.ULID
	key   interface{}
}

func (c cacheItem) keyType() string {
	switch c.key.(type) {
	case cacheKeyPostings:
		return cacheTypePostings
	case cacheKeySeries:
		return cacheTypeSeries
	}
	return "<unknown>"
}

type cacheKeyPostings labels.Label
type cacheKeySeries uint64

type indexCache struct {
	mtx     sync.Mutex
	lru     *lru.LRU
	maxSize uint64
	curSize uint64

	requests    *prometheus.CounterVec
	hits        *prometheus.CounterVec
	added       *prometheus.CounterVec
	current     *prometheus.GaugeVec
	currentSize *prometheus.GaugeVec
}

// newIndexCache creates a new LRU cache for index entries and ensures the total cache
// size approximately does not exceed maxBytes.
func newIndexCache(reg prometheus.Registerer, maxBytes uint64) (*indexCache, error) {
	c := &indexCache{
		maxSize: maxBytes,
	}
	evicted := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_evicted_total",
		Help: "Total number of items that were evicted from the index cache.",
	}, []string{"item_type"})

	c.added = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_added_total",
		Help: "Total number of items that were added to the index cache.",
	}, []string{"item_type"})

	c.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_requests_total",
		Help: "Total number of requests to the cache.",
	}, []string{"item_type"})

	c.hits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_hits_total",
		Help: "Total number of requests to the cache that were a hit.",
	}, []string{"item_type"})

	c.current = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_items",
		Help: "Current number of items in the index cache.",
	}, []string{"item_type"})

	c.currentSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_items_size_bytes",
		Help: "Current byte size of items in the index cache.",
	}, []string{"item_type"})

	// Initialize eviction metric with 0.
	evicted.WithLabelValues(cacheTypePostings).Set(0)
	evicted.WithLabelValues(cacheTypeSeries).Set(0)

	// Initialize LRU cache with a high size limit since we will manage evictions ourselves
	// based on stored size.
	onEvict := func(key, val interface{}) {
		k := key.(cacheItem).keyType()
		v := val.([]byte)

		evicted.WithLabelValues(k).Inc()
		c.current.WithLabelValues(k).Dec()
		c.currentSize.WithLabelValues(k).Sub(float64(len(v)))

		c.curSize -= uint64(len(v))
	}
	l, err := lru.NewLRU(1e12, onEvict)
	if err != nil {
		return nil, err
	}
	c.lru = l

	if reg != nil {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "thanos_store_index_cache_max_size_bytes",
			Help: "Maximum number of bytes to be held in the index cache.",
		}, func() float64 {
			return float64(maxBytes)
		}))
		reg.MustRegister(c.requests, c.hits, c.added, evicted, c.current, c.currentSize)
	}
	return c, nil
}

func (c *indexCache) ensureFits(b []byte) {
	for c.curSize+uint64(len(b)) > c.maxSize {
		c.lru.RemoveOldest()
	}
}

func (c *indexCache) setPostings(b ulid.ULID, l labels.Label, v []byte) {
	c.added.WithLabelValues(cacheTypePostings).Inc()

	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.ensureFits(v)

	// The caller may be passing in a sub-slice of a huge array. Copy the data
	// to ensure we don't waste huge amounts of space for something small.
	cv := make([]byte, len(v))
	copy(cv, v)
	c.lru.Add(cacheItem{b, cacheKeyPostings(l)}, cv)

	c.currentSize.WithLabelValues(cacheTypePostings).Add(float64(len(v)))
}

func (c *indexCache) postings(b ulid.ULID, l labels.Label) ([]byte, bool) {
	c.requests.WithLabelValues(cacheTypePostings).Inc()

	c.mtx.Lock()
	defer c.mtx.Unlock()

	v, ok := c.lru.Get(cacheItem{b, cacheKeyPostings(l)})
	if !ok {
		return nil, false
	}
	c.hits.WithLabelValues(cacheTypePostings).Inc()
	return v.([]byte), true
}

func (c *indexCache) setSeries(b ulid.ULID, id uint64, v []byte) {
	c.added.WithLabelValues(cacheTypeSeries).Inc()

	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.ensureFits(v)

	// The caller may be passing in a sub-slice of a huge array. Copy the data
	// to ensure we don't waste huge amounts of space for something small.
	cv := make([]byte, len(v))
	copy(cv, v)
	c.lru.Add(cacheItem{b, cacheKeySeries(id)}, cv)

	c.currentSize.WithLabelValues(cacheTypeSeries).Add(float64(len(v)))
}

func (c *indexCache) series(b ulid.ULID, id uint64) ([]byte, bool) {
	c.requests.WithLabelValues(cacheTypeSeries).Inc()

	c.mtx.Lock()
	defer c.mtx.Unlock()

	v, ok := c.lru.Get(cacheItem{b, cacheKeySeries(id)})
	if !ok {
		return nil, false
	}
	c.hits.WithLabelValues(cacheTypeSeries).Inc()
	return v.([]byte), true
}
