// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"golang.org/x/crypto/blake2b"

	"github.com/thanos-io/thanos/pkg/tenancy"
)

const (
	CacheTypePostings         string = "Postings"
	CacheTypeExpandedPostings string = "ExpandedPostings"
	CacheTypeSeries           string = "Series"

	sliceHeaderSize = 16
)

type CacheKeyPostings labels.Label
type CacheKeyExpandedPostings string // We don't use []*labels.Matcher because it is not a hashable type so fail at inmemory cache.
type CacheKeySeries uint64

var (
	ulidSize = uint64(len(ulid.ULID{}))
)

// IndexCache is the interface exported by index cache backends.
// Store operations do not support context.Context, deadlines need to be
// supported by the backends themselves. This is because Set operations are
// run async and it does not make sense to attach same context
// (potentially with a deadline) as in the original user's request.
type IndexCache interface {
	// StorePostings stores postings for a single series.
	StorePostings(blockID ulid.ULID, l labels.Label, v []byte, tenant string, ttl time.Duration)

	// FetchMultiPostings fetches multiple postings - each identified by a label -
	// and returns a map containing cache hits, along with a list of missing keys.
	FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label, tenant string) (hits map[labels.Label][]byte, misses []labels.Label)

	// StoreExpandedPostings stores expanded postings for a set of label matchers.
	StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte, tenant string, ttl time.Duration)

	// FetchExpandedPostings fetches expanded postings and returns cached data and a boolean value representing whether it is a cache hit or not.
	FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, matchers []*labels.Matcher, tenant string) ([]byte, bool)

	// StoreSeries stores a single series.
	StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte, tenant string, ttl time.Duration)

	// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
	// and returns a map containing cache hits, along with a list of missing IDs.
	FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef, tenant string) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef)
}

// CacheTTL returns cache TTL based on the block meta. A Short TTL is useful for
// temporary blocks that will be compacted soon and a long TTL for large blocks
// that won't be compacted.
func CacheTTL(meta *metadata.Meta) time.Duration {
	ttl := time.Duration(meta.MaxTime-meta.MinTime) * time.Millisecond

	// ceil to the next hour
	if ttl%time.Hour != 0 {
		ttl += time.Hour - ttl%time.Hour
	}

	return ttl
}

// Common metrics that should be used by all cache implementations.
type CommonMetrics struct {
	RequestTotal  *prometheus.CounterVec
	HitsTotal     *prometheus.CounterVec
	DataSizeBytes *prometheus.HistogramVec
	FetchLatency  *prometheus.HistogramVec
}

// NewCommonMetrics initializes common metrics for index cache.
func NewCommonMetrics(reg prometheus.Registerer) *CommonMetrics {
	return &CommonMetrics{
		RequestTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_index_cache_requests_total",
			Help: "Total number of items requests to the cache.",
		}, []string{"item_type", tenancy.MetricLabel}),
		HitsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_index_cache_hits_total",
			Help: "Total number of items requests to the cache that were a hit.",
		}, []string{"item_type", tenancy.MetricLabel}),
		DataSizeBytes: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name: "thanos_store_index_cache_stored_data_size_bytes",
			Help: "Histogram to track item data size stored in index cache",
			Buckets: []float64{
				32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024, 128 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024,
			},
		}, []string{"item_type", tenancy.MetricLabel}),
		FetchLatency: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "thanos_store_index_cache_fetch_duration_seconds",
			Help:    "Histogram to track latency to fetch items from index cache",
			Buckets: []float64{0.01, 0.1, 0.3, 0.6, 1, 3, 6, 10, 15, 20, 30, 45, 60, 90, 120},
		}, []string{"item_type", tenancy.MetricLabel}),
	}
}

// CacheKey defines cache key used in index cache.
type CacheKey struct {
	Block string
	Key   any

	Compression string
}

// KeyType returns cache key type.
func (c CacheKey) KeyType() string {
	switch c.Key.(type) {
	case CacheKeyPostings:
		return CacheTypePostings
	case CacheKeySeries:
		return CacheTypeSeries
	case CacheKeyExpandedPostings:
		return CacheTypeExpandedPostings
	}
	return "<unknown>"
}

// Size returns the size bytes of the cache key.
func (c CacheKey) Size() uint64 {
	switch k := c.Key.(type) {
	case CacheKeyPostings:
		// ULID + 2 slice headers + number of chars in value and name.
		return ulidSize + 2*sliceHeaderSize + uint64(len(k.Value)+len(k.Name))
	case CacheKeyExpandedPostings:
		return ulidSize + sliceHeaderSize + uint64(len(k))
	case CacheKeySeries:
		return ulidSize + 8 // ULID + uint64.
	}
	return 0
}

func (c CacheKey) String() string {
	switch c.Key.(type) {
	case CacheKeyPostings:
		// Use cryptographically hash functions to avoid hash collisions
		// which would end up in wrong query results.
		lbl := c.Key.(CacheKeyPostings)
		lblHash := blake2b.Sum256([]byte(lbl.Name + ":" + lbl.Value))
		key := "P:" + c.Block + ":" + base64.RawURLEncoding.EncodeToString(lblHash[0:])
		if len(c.Compression) > 0 {
			key += ":" + c.Compression
		}
		return key
	case CacheKeyExpandedPostings:
		// Use cryptographically hash functions to avoid hash collisions
		// which would end up in wrong query results.
		matchers := c.Key.(CacheKeyExpandedPostings)
		matchersHash := blake2b.Sum256([]byte(matchers))
		key := "EP:" + c.Block + ":" + base64.RawURLEncoding.EncodeToString(matchersHash[0:])
		if len(c.Compression) > 0 {
			key += ":" + c.Compression
		}
		return key
	case CacheKeySeries:
		return "S:" + c.Block + ":" + strconv.FormatUint(uint64(c.Key.(CacheKeySeries)), 10)
	default:
		return ""
	}
}

// LabelMatchersToString converts the given label matchers to string format.
func LabelMatchersToString(matchers []*labels.Matcher) string {
	sb := strings.Builder{}
	for i, lbl := range matchers {
		sb.WriteString(lbl.String())
		if i < len(matchers)-1 {
			sb.WriteRune(';')
		}
	}
	return sb.String()
}
