// Tests out the index cache implementation.
package store

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/client_golang/prometheus"
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
