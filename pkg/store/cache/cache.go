// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"encoding/base64"
	"strconv"
	"strings"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"golang.org/x/crypto/blake2b"
)

const (
	cacheTypePostings         string = "Postings"
	cacheTypeExpandedPostings string = "ExpandedPostings"
	cacheTypeSeries           string = "Series"

	sliceHeaderSize = 16
)

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
	StorePostings(blockID ulid.ULID, l labels.Label, v []byte)

	// FetchMultiPostings fetches multiple postings - each identified by a label -
	// and returns a map containing cache hits, along with a list of missing keys.
	FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label)

	// StoreExpandedPostings stores expanded postings for a set of label matchers.
	StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte)

	// FetchExpandedPostings fetches expanded postings and returns cached data and a boolean value representing whether it is a cache hit or not.
	FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, matchers []*labels.Matcher) ([]byte, bool)

	// StoreSeries stores a single series.
	StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte)

	// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
	// and returns a map containing cache hits, along with a list of missing IDs.
	FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef)
}

// Common metrics that should be used by all cache implementations.
type commonMetrics struct {
	requestTotal  *prometheus.CounterVec
	hitsTotal     *prometheus.CounterVec
	dataSizeBytes *prometheus.HistogramVec
}

func newCommonMetrics(reg prometheus.Registerer) *commonMetrics {
	return &commonMetrics{
		requestTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_index_cache_requests_total",
			Help: "Total number of items requests to the cache.",
		}, []string{"item_type"}),
		hitsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_index_cache_hits_total",
			Help: "Total number of items requests to the cache that were a hit.",
		}, []string{"item_type"}),
		dataSizeBytes: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name: "thanos_store_index_cache_stored_data_size_bytes",
			Help: "Histogram to track item data size stored in index cache",
			Buckets: []float64{
				32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024, 128 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024,
			},
		}, []string{"item_type"}),
	}
}

type cacheKey struct {
	block string
	key   interface{}

	compression string
}

func (c cacheKey) keyType() string {
	switch c.key.(type) {
	case cacheKeyPostings:
		return cacheTypePostings
	case cacheKeySeries:
		return cacheTypeSeries
	case cacheKeyExpandedPostings:
		return cacheTypeExpandedPostings
	}
	return "<unknown>"
}

func (c cacheKey) size() uint64 {
	switch k := c.key.(type) {
	case cacheKeyPostings:
		// ULID + 2 slice headers + number of chars in value and name.
		return ulidSize + 2*sliceHeaderSize + uint64(len(k.Value)+len(k.Name))
	case cacheKeyExpandedPostings:
		return ulidSize + sliceHeaderSize + uint64(len(k))
	case cacheKeySeries:
		return ulidSize + 8 // ULID + uint64.
	}
	return 0
}

func (c cacheKey) string() string {
	switch c.key.(type) {
	case cacheKeyPostings:
		// Use cryptographically hash functions to avoid hash collisions
		// which would end up in wrong query results.
		lbl := c.key.(cacheKeyPostings)
		lblHash := blake2b.Sum256([]byte(lbl.Name + ":" + lbl.Value))
		key := "P:" + c.block + ":" + base64.RawURLEncoding.EncodeToString(lblHash[0:])
		if len(c.compression) > 0 {
			key += ":" + c.compression
		}
		return key
	case cacheKeyExpandedPostings:
		// Use cryptographically hash functions to avoid hash collisions
		// which would end up in wrong query results.
		matchers := c.key.(cacheKeyExpandedPostings)
		matchersHash := blake2b.Sum256([]byte(matchers))
		key := "EP:" + c.block + ":" + base64.RawURLEncoding.EncodeToString(matchersHash[0:])
		if len(c.compression) > 0 {
			key += ":" + c.compression
		}
		return key
	case cacheKeySeries:
		return "S:" + c.block + ":" + strconv.FormatUint(uint64(c.key.(cacheKeySeries)), 10)
	default:
		return ""
	}
}

func labelMatchersToString(matchers []*labels.Matcher) string {
	sb := strings.Builder{}
	for i, lbl := range matchers {
		sb.WriteString(lbl.String())
		if i < len(matchers)-1 {
			sb.WriteRune(';')
		}
	}
	return sb.String()
}

type cacheKeyPostings labels.Label
type cacheKeyExpandedPostings string // We don't use []*labels.Matcher because it is not a hashable type so fail at inmemory cache.
type cacheKeySeries uint64
