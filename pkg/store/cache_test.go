// Tests out the index cache implementation.
package store

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/labels"
)

// TestIndexCacheEdge tests the index cache edge cases.
func TestIndexCacheEdge(t *testing.T) {
	metrics := prometheus.NewRegistry()
	cache, err := newIndexCache(metrics, 1)
	testutil.Ok(t, err)

	fits := cache.ensureFits([]byte{42, 24})
	testutil.Equals(t, fits, false)

	fits = cache.ensureFits([]byte{42})
	testutil.Equals(t, fits, true)
}

// TestIndexCacheSmoke runs the smoke tests for the index cache.
func TestIndexCacheSmoke(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	metrics := prometheus.NewRegistry()
	cache, err := newIndexCache(metrics, 20)
	testutil.Ok(t, err)

	blid := ulid.ULID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	labels := labels.Label{Name: "test", Value: "123"}

	cache.setPostings(blid, labels, []byte{42})

	p, ok := cache.postings(blid, labels)
	testutil.Equals(t, ok, true)
	testutil.Equals(t, p, []byte{42})
	testutil.Equals(t, cache.curSize, uint64(1))

	cache.setSeries(blid, 1234, []byte{42, 42})

	s, ok := cache.series(blid, 1234)
	testutil.Equals(t, ok, true)
	testutil.Equals(t, s, []byte{42, 42})
	testutil.Equals(t, cache.curSize, uint64(3))

	cache.lru.RemoveOldest()
	testutil.Equals(t, cache.curSize, uint64(2))

	cache.lru.RemoveOldest()
	testutil.Equals(t, cache.curSize, uint64(0))

	cache.lru.RemoveOldest()
	testutil.Equals(t, cache.curSize, uint64(0))
}
