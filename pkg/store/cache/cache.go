package storecache

import (
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	cacheTypePostings string = "Postings"
	cacheTypeSeries   string = "Series"

	sliceHeaderSize = 16
)

// IndexCache is the interface exported by index cache backends.
type IndexCache interface {
	// StorePostings stores postings for a single series.
	StorePostings(blockID ulid.ULID, l labels.Label, v []byte)

	// FetchMultiPostings fetches multiple postings - each identified by a label -
	// and returns a map containing cache hits, along with a list of missing keys.
	FetchMultiPostings(blockID ulid.ULID, keys []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label)

	// StoreSeries stores a single series.
	StoreSeries(blockID ulid.ULID, id uint64, v []byte)

	// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
	// and returns a map containing cache hits, along with a list of missing IDs.
	FetchMultiSeries(blockID ulid.ULID, ids []uint64) (hits map[uint64][]byte, misses []uint64)
}

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
