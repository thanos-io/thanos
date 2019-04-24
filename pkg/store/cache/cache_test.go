// Tests out the index cache implementation.
package storecache

import (
	"math"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/tsdb/labels"
)

func TestIndexCache_AvoidsDeadlock(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	metrics := prometheus.NewRegistry()
	cache, err := NewIndexCache(log.NewNopLogger(), metrics, Opts{
		MaxItemSizeBytes: sliceHeaderSize + 5,
		MaxSizeBytes:     sliceHeaderSize + 5,
	})
	testutil.Ok(t, err)

	l, err := simplelru.NewLRU(math.MaxInt64, func(key, val interface{}) {
		cache.onEvict(key, val)

		// We hack LRU to add back entry on eviction to simulate broken evictions.
		cache.lru.Add(key, val)
		cache.curSize += sliceHeaderSize + uint64(len(val.([]byte))) // Slice header + bytes.
	})
	testutil.Ok(t, err)
	cache.lru = l

	cache.SetPostings(ulid.MustNew(0, nil), labels.Label{Name: "test2", Value: "1"}, []byte{42, 33, 14, 67, 11})

	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))

	// This triggers deadlock logic.
	cache.SetPostings(ulid.MustNew(0, nil), labels.Label{Name: "test1", Value: "1"}, []byte{42})

	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))
}

// This should not happen as we hardcode math.MaxInt, but we still add test to check this out.
func TestIndexCache_MaxNumberOfItemsHit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	metrics := prometheus.NewRegistry()
	cache, err := NewIndexCache(log.NewNopLogger(), metrics, Opts{
		MaxItemSizeBytes: 2*sliceHeaderSize + 10,
		MaxSizeBytes:     2*sliceHeaderSize + 10,
	})
	testutil.Ok(t, err)

	l, err := simplelru.NewLRU(2, cache.onEvict)
	testutil.Ok(t, err)
	cache.lru = l

	id := ulid.MustNew(0, nil)

	cache.SetPostings(id, labels.Label{Name: "test", Value: "123"}, []byte{42, 33})
	cache.SetPostings(id, labels.Label{Name: "test", Value: "124"}, []byte{42, 33})
	cache.SetPostings(id, labels.Label{Name: "test", Value: "125"}, []byte{42, 33})

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

func TestIndexCache_Eviction_WithMetrics(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	metrics := prometheus.NewRegistry()
	cache, err := NewIndexCache(log.NewNopLogger(), metrics, Opts{
		MaxItemSizeBytes: 2*sliceHeaderSize + 5,
		MaxSizeBytes:     2*sliceHeaderSize + 5,
	})
	testutil.Ok(t, err)

	id := ulid.MustNew(0, nil)
	lbls := labels.Label{Name: "test", Value: "123"}

	_, ok := cache.Postings(id, lbls)
	testutil.Assert(t, !ok, "no such key")

	// Add sliceHeaderSize + 2 bytes.
	cache.SetPostings(id, lbls, []byte{42, 33})
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

	p, ok := cache.Postings(id, lbls)
	testutil.Assert(t, ok, "key exists")
	testutil.Equals(t, []byte{42, 33}, p)

	_, ok = cache.Postings(ulid.MustNew(1, nil), lbls)
	testutil.Assert(t, !ok, "no such key")
	_, ok = cache.Postings(id, labels.Label{Name: "test", Value: "124"})
	testutil.Assert(t, !ok, "no such key")

	// Add sliceHeaderSize + 3 more bytes.
	cache.SetSeries(id, 1234, []byte{222, 223, 224})
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

	p, ok = cache.Series(id, 1234)
	testutil.Assert(t, ok, "key exists")
	testutil.Equals(t, []byte{222, 223, 224}, p)

	lbls2 := labels.Label{Name: "test", Value: "124"}

	// Add sliceHeaderSize + 5 + 16 bytes, should fully evict 2 last items.
	v := []byte{42, 33, 14, 67, 11}
	for i := 0; i < sliceHeaderSize; i++ {
		v = append(v, 3)
	}
	cache.SetPostings(id, lbls2, v)

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
	_, ok = cache.Postings(id, lbls)
	testutil.Assert(t, !ok, "no such key")
	_, ok = cache.Series(id, 1234)
	testutil.Assert(t, !ok, "no such key")

	p, ok = cache.Postings(id, lbls2)
	testutil.Assert(t, ok, "key exists")
	testutil.Equals(t, v, p)

	// Add same item again.
	// NOTE: In our caller code, we always check first hit, then we claim miss and set posting so this should not happen.
	// That's why this case is not optimized and we evict + re add the item.
	cache.SetPostings(id, lbls2, v)

	testutil.Equals(t, uint64(2*sliceHeaderSize+5), cache.curSize)
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(2*sliceHeaderSize+5), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(2*sliceHeaderSize+5+55), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings))) // Eviction.
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeries)))

	p, ok = cache.Postings(id, lbls2)
	testutil.Assert(t, ok, "key exists")
	testutil.Equals(t, v, p)

	// Add too big item.
	cache.SetPostings(id, labels.Label{Name: "test", Value: "toobig"}, append(v, 5))
	testutil.Equals(t, uint64(2*sliceHeaderSize+5), cache.curSize)
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(2*sliceHeaderSize+5), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(2*sliceHeaderSize+5+55), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings))) // Overflow.
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeries)))

	_, _, ok = cache.lru.RemoveOldest()
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
	testutil.Equals(t, float64(3), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeries)))

	_, _, ok = cache.lru.RemoveOldest()
	testutil.Assert(t, !ok, "nothing to remove")

	lbls3 := labels.Label{Name: "test", Value: "124"}

	cache.SetPostings(id, lbls3, []byte{})

	testutil.Equals(t, uint64(sliceHeaderSize), cache.curSize)
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(sliceHeaderSize), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(sliceHeaderSize+55), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(3), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeries)))

	p, ok = cache.Postings(id, lbls3)
	testutil.Assert(t, ok, "key exists")
	testutil.Equals(t, []byte{}, p)

	// nil works and still allocates empty slice.
	lbls4 := labels.Label{Name: "test", Value: "125"}
	cache.SetPostings(id, lbls4, []byte(nil))

	testutil.Equals(t, 2*uint64(sliceHeaderSize), cache.curSize)
	testutil.Equals(t, float64(2), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, 2*float64(sliceHeaderSize), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, 2*float64(sliceHeaderSize+55), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(3), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeries)))

	p, ok = cache.Postings(id, lbls4)
	testutil.Assert(t, ok, "key exists")
	testutil.Equals(t, []byte{}, p)

	// Other metrics.
	testutil.Equals(t, float64(5), promtest.ToFloat64(cache.added.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.added.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(9), promtest.ToFloat64(cache.requests.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(cache.requests.WithLabelValues(cacheTypeSeries)))
	testutil.Equals(t, float64(5), promtest.ToFloat64(cache.hits.WithLabelValues(cacheTypePostings)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(cache.hits.WithLabelValues(cacheTypeSeries)))
}
