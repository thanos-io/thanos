package storecache

import (
	"math"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	lru "github.com/hashicorp/golang-lru/simplelru"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/labels"
)

const (
	cacheTypePostings string = "Postings"
	cacheTypeSeries   string = "Series"

	sliceHeaderSize = 16
)

type cacheKey struct {
	block ulid.ULID
	key   interface{}
}

func (c cacheKey) keyType() string {
	switch c.key.(type) {
	case cacheKeyPostings:
		return cacheTypePostings
	case cacheKeySeries:
		return cacheTypeSeries
	}
	return "<unknown>"
}

func (c cacheKey) size() uint64 {
	switch k := c.key.(type) {
	case cacheKeyPostings:
		// ULID + 2 slice headers + number of chars in value and name.
		return 16 + 2*sliceHeaderSize + uint64(len(k.Value)+len(k.Name))
	case cacheKeySeries:
		return 16 + 8 // ULID + uint64.
	}
	return 0
}

type cacheKeyPostings labels.Label
type cacheKeySeries uint64

type IndexCache struct {
	mtx sync.Mutex

	logger           log.Logger
	lru              *lru.LRU
	maxSizeBytes     uint64
	maxItemSizeBytes uint64

	curSize uint64

	evicted          *prometheus.CounterVec
	requests         *prometheus.CounterVec
	hits             *prometheus.CounterVec
	added            *prometheus.CounterVec
	current          *prometheus.GaugeVec
	currentSize      *prometheus.GaugeVec
	totalCurrentSize *prometheus.GaugeVec
	overflow         *prometheus.CounterVec
}

type Opts struct {
	// MaxSizeBytes represents overall maximum number of bytes cache can contain.
	MaxSizeBytes uint64
	// MaxItemSizeBytes represents maximum size of single item.
	MaxItemSizeBytes uint64
}

// NewIndexCache creates a new thread-safe LRU cache for index entries and ensures the total cache
// size approximately does not exceed maxBytes.
func NewIndexCache(logger log.Logger, reg prometheus.Registerer, opts Opts) (*IndexCache, error) {
	if opts.MaxItemSizeBytes > opts.MaxSizeBytes {
		return nil, errors.Errorf("max item size (%v) cannot be bigger than overall cache size (%v)", opts.MaxItemSizeBytes, opts.MaxSizeBytes)
	}

	c := &IndexCache{
		logger:           logger,
		maxSizeBytes:     opts.MaxSizeBytes,
		maxItemSizeBytes: opts.MaxItemSizeBytes,
	}

	c.evicted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_evicted_total",
		Help: "Total number of items that were evicted from the index cache.",
	}, []string{"item_type"})
	c.evicted.WithLabelValues(cacheTypePostings)
	c.evicted.WithLabelValues(cacheTypeSeries)

	c.added = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_added_total",
		Help: "Total number of items that were added to the index cache.",
	}, []string{"item_type"})
	c.added.WithLabelValues(cacheTypePostings)
	c.added.WithLabelValues(cacheTypeSeries)

	c.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_requests_total",
		Help: "Total number of requests to the cache.",
	}, []string{"item_type"})
	c.requests.WithLabelValues(cacheTypePostings)
	c.requests.WithLabelValues(cacheTypeSeries)

	c.overflow = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_overflowed_total",
		Help: "Total number of items that could not be added to the cache due to being too big.",
	}, []string{"item_type"})
	c.overflow.WithLabelValues(cacheTypePostings)
	c.overflow.WithLabelValues(cacheTypeSeries)

	c.hits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_hits_total",
		Help: "Total number of requests to the cache that were a hit.",
	}, []string{"item_type"})
	c.hits.WithLabelValues(cacheTypePostings)
	c.hits.WithLabelValues(cacheTypeSeries)

	c.current = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_items",
		Help: "Current number of items in the index cache.",
	}, []string{"item_type"})
	c.current.WithLabelValues(cacheTypePostings)
	c.current.WithLabelValues(cacheTypeSeries)

	c.currentSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_items_size_bytes",
		Help: "Current byte size of items in the index cache.",
	}, []string{"item_type"})
	c.currentSize.WithLabelValues(cacheTypePostings)
	c.currentSize.WithLabelValues(cacheTypeSeries)

	c.totalCurrentSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_total_size_bytes",
		Help: "Current byte size of items (both value and key) in the index cache.",
	}, []string{"item_type"})
	c.totalCurrentSize.WithLabelValues(cacheTypePostings)
	c.totalCurrentSize.WithLabelValues(cacheTypeSeries)

	if reg != nil {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "thanos_store_index_cache_max_size_bytes",
			Help: "Maximum number of bytes to be held in the index cache.",
		}, func() float64 {
			return float64(c.maxSizeBytes)
		}))
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "thanos_store_index_cache_max_item_size_bytes",
			Help: "Maximum number of bytes for single entry to be held in the index cache.",
		}, func() float64 {
			return float64(c.maxItemSizeBytes)
		}))
		reg.MustRegister(c.requests, c.hits, c.added, c.evicted, c.current, c.currentSize, c.totalCurrentSize, c.overflow)
	}

	// Initialize LRU cache with a high size limit since we will manage evictions ourselves
	// based on stored size using `RemoveOldest` method.
	l, err := lru.NewLRU(math.MaxInt64, c.onEvict)
	if err != nil {
		return nil, err
	}
	c.lru = l

	level.Info(logger).Log(
		"msg", "created index cache",
		"maxItemSizeBytes", c.maxItemSizeBytes,
		"maxSizeBytes", c.maxSizeBytes,
		"maxItems", "math.MaxInt64",
	)
	return c, nil
}

func (c *IndexCache) onEvict(key, val interface{}) {
	k := key.(cacheKey).keyType()
	entrySize := sliceHeaderSize + uint64(len(val.([]byte)))

	c.evicted.WithLabelValues(string(k)).Inc()
	c.current.WithLabelValues(string(k)).Dec()
	c.currentSize.WithLabelValues(string(k)).Sub(float64(entrySize))
	c.totalCurrentSize.WithLabelValues(string(k)).Sub(float64(entrySize + key.(cacheKey).size()))

	c.curSize -= entrySize
}

func (c *IndexCache) get(typ string, key cacheKey) ([]byte, bool) {
	c.requests.WithLabelValues(typ).Inc()

	c.mtx.Lock()
	defer c.mtx.Unlock()

	v, ok := c.lru.Get(key)
	if !ok {
		return nil, false
	}
	c.hits.WithLabelValues(typ).Inc()
	return v.([]byte), true
}

func (c *IndexCache) set(typ string, key cacheKey, val []byte) {
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
	c.totalCurrentSize.WithLabelValues(typ).Add(float64(size + key.size()))
	c.current.WithLabelValues(typ).Inc()
	c.curSize += size
}

// ensureFits tries to make sure that the passed slice will fit into the LRU cache.
// Returns true if it will fit.
func (c *IndexCache) ensureFits(size uint64, typ string) bool {
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

func (c *IndexCache) reset() {
	c.lru.Purge()
	c.current.Reset()
	c.currentSize.Reset()
	c.totalCurrentSize.Reset()
	c.curSize = 0
}

// SetPostings sets the postings identfied by the ulid and label to the value v,
// if the postings already exists in the cache it is not mutated.
func (c *IndexCache) SetPostings(b ulid.ULID, l labels.Label, v []byte) {
	c.set(cacheTypePostings, cacheKey{b, cacheKeyPostings(l)}, v)
}

func (c *IndexCache) Postings(b ulid.ULID, l labels.Label) ([]byte, bool) {
	return c.get(cacheTypePostings, cacheKey{b, cacheKeyPostings(l)})
}

// SetSeries sets the series identfied by the ulid and id to the value v,
// if the series already exists in the cache it is not mutated.
func (c *IndexCache) SetSeries(b ulid.ULID, id uint64, v []byte) {
	c.set(cacheTypeSeries, cacheKey{b, cacheKeySeries(id)}, v)
}

func (c *IndexCache) Series(b ulid.ULID, id uint64) ([]byte, bool) {
	return c.get(cacheTypeSeries, cacheKey{b, cacheKeySeries(id)})
}
