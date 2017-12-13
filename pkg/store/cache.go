package store

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/oklog/ulid"
	"github.com/prometheus/tsdb/labels"
)

type cacheItem struct {
	block ulid.ULID
	key   interface{}
}
type cacheKeyPostings labels.Label
type cacheKeySeries uint64

type indexCache struct {
	lru *lru.TwoQueueCache
}

func newIndexCache(sz int) (*indexCache, error) {
	l, err := lru.New2Q(sz)
	if err != nil {
		return nil, err
	}
	return &indexCache{lru: l}, nil
}

func (c *indexCache) setPostings(b ulid.ULID, l labels.Label, v []byte) {
	// The caller may be passing in a sub-slice of a huge array. Copy the data
	// to ensure we don't waste huge amounts of space for something small.
	cv := make([]byte, len(v))
	copy(cv, v)
	c.lru.Add(cacheItem{b, cacheKeyPostings(l)}, cv)
}

func (c *indexCache) postings(b ulid.ULID, l labels.Label) ([]byte, bool) {
	v, ok := c.lru.Get(cacheItem{b, cacheKeyPostings(l)})
	if !ok {
		return nil, false
	}
	return v.([]byte), true
}

func (c *indexCache) setSeries(b ulid.ULID, id uint64, v []byte) {
	// The caller may be passing in a sub-slice of a huge array. Copy the data
	// to ensure we don't waste huge amounts of space for something small.
	cv := make([]byte, len(v))
	copy(cv, v)
	c.lru.Add(cacheItem{b, cacheKeySeries(id)}, cv)
}

func (c *indexCache) series(b ulid.ULID, id uint64) ([]byte, bool) {
	v, ok := c.lru.Get(cacheItem{b, cacheKeySeries(id)})
	if !ok {
		return nil, false
	}
	return v.([]byte), true
}
