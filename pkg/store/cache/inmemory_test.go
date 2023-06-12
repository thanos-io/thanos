// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Tests out the index cache implementation.
package storecache

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/go-kit/log"
	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/efficientgo/core/testutil"
)

func TestNewInMemoryIndexCache(t *testing.T) {
	// Should return error on invalid YAML config.
	conf := []byte("invalid")
	cache, err := NewInMemoryIndexCache(log.NewNopLogger(), nil, conf)
	testutil.NotOk(t, err)
	testutil.Equals(t, (*InMemoryIndexCache)(nil), cache)

	// Should instance an in-memory index cache with default config
	// on empty YAML config.
	conf = []byte{}
	cache, err = NewInMemoryIndexCache(log.NewNopLogger(), nil, conf)
	testutil.Ok(t, err)
	testutil.Equals(t, uint64(DefaultInMemoryIndexCacheConfig.MaxSize), cache.maxSizeBytes)
	testutil.Equals(t, uint64(DefaultInMemoryIndexCacheConfig.MaxItemSize), cache.maxItemSizeBytes)

	// Should instance an in-memory index cache with specified YAML config.s with units.
	conf = []byte(`
max_size: 1MB
max_item_size: 2KB
`)
	cache, err = NewInMemoryIndexCache(log.NewNopLogger(), nil, conf)
	testutil.Ok(t, err)
	testutil.Equals(t, uint64(1024*1024), cache.maxSizeBytes)
	testutil.Equals(t, uint64(2*1024), cache.maxItemSizeBytes)

	// Should instance an in-memory index cache with specified YAML config.s with units.
	conf = []byte(`
max_size: 2KB
max_item_size: 1MB
`)
	cache, err = NewInMemoryIndexCache(log.NewNopLogger(), nil, conf)
	testutil.NotOk(t, err)
	testutil.Equals(t, (*InMemoryIndexCache)(nil), cache)
	// testutil.Equals(t, uint64(1024*1024), cache.maxSizeBytes)
	// testutil.Equals(t, uint64(2*1024), cache.maxItemSizeBytes)

	// testutil.Equals(t, uint64(1024*1024), cache.maxItemSizeBytes)
	// testutil.Equals(t, uint64(2*1024), cache.maxSizeBytes)
}

func TestInMemoryIndexCache_AvoidsDeadlock(t *testing.T) {
	metrics := prometheus.NewRegistry()
	cache, err := NewInMemoryIndexCacheWithConfig(log.NewNopLogger(), metrics, InMemoryIndexCacheConfig{
		MaxItemSize: sliceHeaderSize + 5,
		MaxSize:     sliceHeaderSize + 5,
	})
	testutil.Ok(t, err)

	l, err := simplelru.NewLRU(math.MaxInt64, func(key, val interface{}) {
		// Hack LRU to simulate broken accounting: evictions do not reduce current size.
		size := cache.curSize
		cache.onEvict(key, val)
		cache.curSize = size
	})
	testutil.Ok(t, err)
	cache.lru = l

	cache.StorePostings(ulid.MustNew(0, nil), labels.Label{Name: "test2", Value: "1"}, []byte{42, 33, 14, 67, 11})

	testutil.Equals(t, uint64(sliceHeaderSize+5), cache.curSize)
	testutil.Equals(t, float64(cache.curSize), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))

	// This triggers deadlock logic.
	cache.StorePostings(ulid.MustNew(0, nil), labels.Label{Name: "test1", Value: "1"}, []byte{42})

	testutil.Equals(t, uint64(sliceHeaderSize+1), cache.curSize)
	testutil.Equals(t, float64(cache.curSize), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
}

func TestInMemoryIndexCache_UpdateItem(t *testing.T) {
	const maxSize = 2 * (sliceHeaderSize + 1)

	var errorLogs []string
	errorLogger := log.LoggerFunc(func(kvs ...interface{}) error {
		var lvl string
		for i := 0; i < len(kvs); i += 2 {
			if kvs[i] == "level" {
				lvl = fmt.Sprint(kvs[i+1])
				break
			}
		}
		if lvl != "error" {
			return nil
		}
		var buf bytes.Buffer
		defer func() { errorLogs = append(errorLogs, buf.String()) }()
		return log.NewLogfmtLogger(&buf).Log(kvs...)
	})

	metrics := prometheus.NewRegistry()
	cache, err := NewInMemoryIndexCacheWithConfig(log.NewSyncLogger(errorLogger), metrics, InMemoryIndexCacheConfig{
		MaxItemSize: maxSize,
		MaxSize:     maxSize,
	})
	testutil.Ok(t, err)

	uid := func(id storage.SeriesRef) ulid.ULID { return ulid.MustNew(uint64(id), nil) }
	lbl := labels.Label{Name: "foo", Value: "bar"}
	matcher := labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")
	ctx := context.Background()

	for _, tt := range []struct {
		typ string
		set func(storage.SeriesRef, []byte)
		get func(storage.SeriesRef) ([]byte, bool)
	}{
		{
			typ: cacheTypePostings,
			set: func(id storage.SeriesRef, b []byte) { cache.StorePostings(uid(id), lbl, b) },
			get: func(id storage.SeriesRef) ([]byte, bool) {
				hits, _ := cache.FetchMultiPostings(ctx, uid(id), []labels.Label{lbl})
				b, ok := hits[lbl]

				return b, ok
			},
		},
		{
			typ: cacheTypeSeries,
			set: func(id storage.SeriesRef, b []byte) { cache.StoreSeries(uid(id), id, b) },
			get: func(id storage.SeriesRef) ([]byte, bool) {
				hits, _ := cache.FetchMultiSeries(ctx, uid(id), []storage.SeriesRef{id})
				b, ok := hits[id]

				return b, ok
			},
		},
		{
			typ: cacheTypeExpandedPostings,
			set: func(id storage.SeriesRef, b []byte) {
				cache.StoreExpandedPostings(uid(id), []*labels.Matcher{matcher}, b)
			},
			get: func(id storage.SeriesRef) ([]byte, bool) {
				return cache.FetchExpandedPostings(ctx, uid(id), []*labels.Matcher{matcher})
			},
		},
	} {
		t.Run(tt.typ, func(t *testing.T) {
			defer func() { errorLogs = nil }()

			// Set value.
			tt.set(0, []byte{0})
			buf, ok := tt.get(0)
			testutil.Equals(t, true, ok)
			testutil.Equals(t, []byte{0}, buf)
			testutil.Equals(t, float64(sliceHeaderSize+1), promtest.ToFloat64(cache.currentSize.WithLabelValues(tt.typ)))
			testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(tt.typ)))
			testutil.Equals(t, []string(nil), errorLogs)

			// Set the same value again.
			// NB: This used to over-count the value.
			tt.set(0, []byte{0})
			buf, ok = tt.get(0)
			testutil.Equals(t, true, ok)
			testutil.Equals(t, []byte{0}, buf)
			testutil.Equals(t, float64(sliceHeaderSize+1), promtest.ToFloat64(cache.currentSize.WithLabelValues(tt.typ)))
			testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(tt.typ)))
			testutil.Equals(t, []string(nil), errorLogs)

			// Set a larger value.
			// NB: This used to deadlock when enough values were over-counted and it
			// couldn't clear enough space -- repeatedly removing oldest after empty.
			tt.set(1, []byte{0, 1})
			buf, ok = tt.get(1)
			testutil.Equals(t, true, ok)
			testutil.Equals(t, []byte{0, 1}, buf)
			testutil.Equals(t, float64(sliceHeaderSize+2), promtest.ToFloat64(cache.currentSize.WithLabelValues(tt.typ)))
			testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(tt.typ)))
			testutil.Equals(t, []string(nil), errorLogs)

			// Mutations to existing values will be ignored.
			tt.set(1, []byte{1, 2})
			buf, ok = tt.get(1)
			testutil.Equals(t, true, ok)
			testutil.Equals(t, []byte{0, 1}, buf)
			testutil.Equals(t, float64(sliceHeaderSize+2), promtest.ToFloat64(cache.currentSize.WithLabelValues(tt.typ)))
			testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(tt.typ)))
			testutil.Equals(t, []string(nil), errorLogs)
		})
	}
}

// This should not happen as we hardcode math.MaxInt, but we still add test to check this out.
func TestInMemoryIndexCache_MaxNumberOfItemsHit(t *testing.T) {
	metrics := prometheus.NewRegistry()
	cache, err := NewInMemoryIndexCacheWithConfig(log.NewNopLogger(), metrics, InMemoryIndexCacheConfig{
		MaxItemSize: 2*sliceHeaderSize + 10,
		MaxSize:     2*sliceHeaderSize + 10,
	})
	testutil.Ok(t, err)

	l, err := simplelru.NewLRU(2, cache.onEvict)
	testutil.Ok(t, err)
	cache.lru = l

	id := ulid.MustNew(0, nil)

	cache.StorePostings(id, labels.Label{Name: "test", Value: "123"}, []byte{42, 33})
	cache.StorePostings(id, labels.Label{Name: "test", Value: "124"}, []byte{42, 33})
	cache.StorePostings(id, labels.Label{Name: "test", Value: "125"}, []byte{42, 33})

	testutil.Equals(t, uint64(2*sliceHeaderSize+4), cache.curSize)
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(3), promtest.ToFloat64(cache.added.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.added.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.requests.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.requests.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.hits.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.hits.WithLabelValues(cacheTypeSeries)))
}

func TestInMemoryIndexCache_Eviction_WithMetrics(t *testing.T) {
	metrics := prometheus.NewRegistry()
	cache, err := NewInMemoryIndexCacheWithConfig(log.NewNopLogger(), metrics, InMemoryIndexCacheConfig{
		MaxItemSize: 2*sliceHeaderSize + 5,
		MaxSize:     2*sliceHeaderSize + 5,
	})
	testutil.Ok(t, err)

	id := ulid.MustNew(0, nil)
	lbls := labels.Label{Name: "test", Value: "123"}
	ctx := context.Background()
	emptyPostingsHits := map[labels.Label][]byte{}
	emptyPostingsMisses := []labels.Label(nil)
	emptySeriesHits := map[storage.SeriesRef][]byte{}
	emptySeriesMisses := []storage.SeriesRef(nil)

	pHits, pMisses := cache.FetchMultiPostings(ctx, id, []labels.Label{lbls})
	testutil.Equals(t, emptyPostingsHits, pHits, "no such key")
	testutil.Equals(t, []labels.Label{lbls}, pMisses)

	// Add sliceHeaderSize + 2 bytes.
	cache.StorePostings(id, lbls, []byte{42, 33})
	testutil.Equals(t, uint64(sliceHeaderSize+2), cache.curSize)
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(sliceHeaderSize+2), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(sliceHeaderSize+2+55), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeries)))

	pHits, pMisses = cache.FetchMultiPostings(ctx, id, []labels.Label{lbls})
	testutil.Equals(t, map[labels.Label][]byte{lbls: {42, 33}}, pHits, "key exists")
	testutil.Equals(t, emptyPostingsMisses, pMisses)

	pHits, pMisses = cache.FetchMultiPostings(ctx, ulid.MustNew(1, nil), []labels.Label{lbls})
	testutil.Equals(t, emptyPostingsHits, pHits, "no such key")
	testutil.Equals(t, []labels.Label{lbls}, pMisses)

	pHits, pMisses = cache.FetchMultiPostings(ctx, id, []labels.Label{{Name: "test", Value: "124"}})
	testutil.Equals(t, emptyPostingsHits, pHits, "no such key")
	testutil.Equals(t, []labels.Label{{Name: "test", Value: "124"}}, pMisses)

	// Add sliceHeaderSize + 3 more bytes.
	cache.StoreSeries(id, 1234, []byte{222, 223, 224})
	testutil.Equals(t, uint64(2*sliceHeaderSize+5), cache.curSize)
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(sliceHeaderSize+2), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(sliceHeaderSize+2+55), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(sliceHeaderSize+3), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(sliceHeaderSize+3+24), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeries)))

	sHits, sMisses := cache.FetchMultiSeries(ctx, id, []storage.SeriesRef{1234})
	testutil.Equals(t, map[storage.SeriesRef][]byte{1234: {222, 223, 224}}, sHits, "key exists")
	testutil.Equals(t, emptySeriesMisses, sMisses)

	lbls2 := labels.Label{Name: "test", Value: "124"}

	// Add sliceHeaderSize + 5 + 16 bytes, should fully evict 2 last items.
	v := []byte{42, 33, 14, 67, 11}
	for i := 0; i < sliceHeaderSize; i++ {
		v = append(v, 3)
	}
	cache.StorePostings(id, lbls2, v)

	testutil.Equals(t, uint64(2*sliceHeaderSize+5), cache.curSize)
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(2*sliceHeaderSize+5), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(2*sliceHeaderSize+5+55), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings))) // Eviction.
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeries)))   // Eviction.

	// Evicted.
	pHits, pMisses = cache.FetchMultiPostings(ctx, id, []labels.Label{lbls})
	testutil.Equals(t, emptyPostingsHits, pHits, "no such key")
	testutil.Equals(t, []labels.Label{lbls}, pMisses)

	sHits, sMisses = cache.FetchMultiSeries(ctx, id, []storage.SeriesRef{1234})
	testutil.Equals(t, emptySeriesHits, sHits, "no such key")
	testutil.Equals(t, []storage.SeriesRef{1234}, sMisses)

	pHits, pMisses = cache.FetchMultiPostings(ctx, id, []labels.Label{lbls2})
	testutil.Equals(t, map[labels.Label][]byte{lbls2: v}, pHits)
	testutil.Equals(t, emptyPostingsMisses, pMisses)

	// Add same item again.
	cache.StorePostings(id, lbls2, v)

	testutil.Equals(t, uint64(2*sliceHeaderSize+5), cache.curSize)
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(2*sliceHeaderSize+5), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(2*sliceHeaderSize+5+55), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeries)))

	pHits, pMisses = cache.FetchMultiPostings(ctx, id, []labels.Label{lbls2})
	testutil.Equals(t, map[labels.Label][]byte{lbls2: v}, pHits)
	testutil.Equals(t, emptyPostingsMisses, pMisses)

	// Add too big item.
	cache.StorePostings(id, labels.Label{Name: "test", Value: "toobig"}, append(v, 5))
	testutil.Equals(t, uint64(2*sliceHeaderSize+5), cache.curSize)
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(2*sliceHeaderSize+5), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(2*sliceHeaderSize+5+55), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings))) // Overflow.
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeries)))

	_, _, ok := cache.lru.RemoveOldest()
	testutil.Assert(t, ok, "something to remove")

	testutil.Equals(t, uint64(0), cache.curSize)
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeries)))

	_, _, ok = cache.lru.RemoveOldest()
	testutil.Assert(t, !ok, "nothing to remove")

	lbls3 := labels.Label{Name: "test", Value: "124"}

	cache.StorePostings(id, lbls3, []byte{})

	testutil.Equals(t, uint64(sliceHeaderSize), cache.curSize)
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(sliceHeaderSize), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(sliceHeaderSize+55), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeries)))

	pHits, pMisses = cache.FetchMultiPostings(ctx, id, []labels.Label{lbls3})
	testutil.Equals(t, map[labels.Label][]byte{lbls3: {}}, pHits, "key exists")
	testutil.Equals(t, emptyPostingsMisses, pMisses)

	// nil works and still allocates empty slice.
	lbls4 := labels.Label{Name: "test", Value: "125"}
	cache.StorePostings(id, lbls4, []byte(nil))

	testutil.Equals(t, 2*uint64(sliceHeaderSize), cache.curSize)
	testutil.Equals(t, float64(2), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, 2*float64(sliceHeaderSize), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, 2*float64(sliceHeaderSize+55), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeries)))

	pHits, pMisses = cache.FetchMultiPostings(ctx, id, []labels.Label{lbls4})
	testutil.Equals(t, map[labels.Label][]byte{lbls4: {}}, pHits, "key exists")
	testutil.Equals(t, emptyPostingsMisses, pMisses)

	// Other metrics.
	testutil.Equals(t, float64(4), promtest.ToFloat64(cache.added.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.added.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(9), promtest.ToFloat64(cache.requests.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(cache.requests.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(5), promtest.ToFloat64(cache.hits.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.hits.WithLabelValues(cacheTypeSeries)))
}
