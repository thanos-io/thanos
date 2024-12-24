// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Original code by Alan Protasio (https://github.com/alanprot) in the Cortex project.
//
//nolint:unparam
package expandedpostingscache

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func Test_ShouldFetchPromiseOnlyOnce(t *testing.T) {
	m := NewPostingCacheMetrics(prometheus.NewPedanticRegistry())
	cache := newFifoCache[int]("test", m, time.Now, 10<<20)
	calls := atomic.Int64{}
	concurrency := 100
	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	fetchFunc := func() (int, int64, error) {
		calls.Inc()
		time.Sleep(100 * time.Millisecond)
		return 0, 0, nil //nolint:unparam
	}

	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			cache.getPromiseForKey("key1", fetchFunc)
		}()
	}

	wg.Wait()
	require.Equal(t, int64(1), calls.Load())

}

func TestFifoCacheExpire(t *testing.T) {

	keySize := 20
	numberOfKeys := 100

	tc := map[string]struct {
		ttl                time.Duration
		maxBytes           uint64
		expectedFinalItems int
		ttlExpire          bool
	}{
		"MaxBytes": {
			expectedFinalItems: 10,
			ttl:                time.Hour,
			maxBytes:           uint64(10 * (8 + keySize)),
		},
		"TTL": {
			expectedFinalItems: numberOfKeys,
			ttlExpire:          true,
			ttl:                time.Hour,
			maxBytes:           10 << 20,
		},
	}

	for name, c := range tc {
		t.Run(name, func(t *testing.T) {
			r := prometheus.NewPedanticRegistry()
			m := NewPostingCacheMetrics(r)
			timeNow := time.Now
			cache := newFifoCache[int]("test", m, timeNow, c.maxBytes)

			for i := 0; i < numberOfKeys; i++ {
				key := repeatStringIfNeeded(fmt.Sprintf("key%d", i), keySize)
				p, loaded := cache.getPromiseForKey(key, func() (int, int64, error) {
					return 1, 8, nil
				})
				require.False(t, loaded)
				require.Equal(t, 1, p.v)
				require.True(t, cache.contains(key))
				p, loaded = cache.getPromiseForKey(key, func() (int, int64, error) {
					return 1, 0, nil
				})
				require.True(t, loaded)
				require.Equal(t, 1, p.v)
			}

			totalCacheSize := 0

			for i := 0; i < numberOfKeys; i++ {
				key := repeatStringIfNeeded(fmt.Sprintf("key%d", i), keySize)
				if cache.contains(key) {
					totalCacheSize++
				}
			}

			require.Equal(t, c.expectedFinalItems, totalCacheSize)

			if c.expectedFinalItems != numberOfKeys {
				err := promtest.GatherAndCompare(r, bytes.NewBufferString(fmt.Sprintf(`
		# HELP expanded_postings_cache_evicts_total Total number of evictions in the cache, excluding items that got evicted.
		# TYPE expanded_postings_cache_evicts_total counter
        expanded_postings_cache_evicts_total{cache="test",reason="full"} %v
`, numberOfKeys-c.expectedFinalItems)), "expanded_postings_cache_evicts_total")
				require.NoError(t, err)

			}

			if c.ttlExpire {
				cache.timeNow = func() time.Time {
					return timeNow().Add(2 * c.ttl)
				}

				for i := 0; i < numberOfKeys; i++ {
					key := repeatStringIfNeeded(fmt.Sprintf("key%d", i), keySize)
					originalSize := cache.cachedBytes
					p, loaded := cache.getPromiseForKey(key, func() (int, int64, error) {
						return 2, 18, nil
					})
					require.False(t, loaded)
					// New value
					require.Equal(t, 2, p.v)
					// Total Size Updated
					require.Equal(t, originalSize+10, cache.cachedBytes)
				}

				err := promtest.GatherAndCompare(r, bytes.NewBufferString(fmt.Sprintf(`
		# HELP expanded_postings_cache_evicts_total Total number of evictions in the cache, excluding items that got evicted.
		# TYPE expanded_postings_cache_evicts_total counter
        expanded_postings_cache_evicts_total{cache="test",reason="expired"} %v
`, numberOfKeys)), "expanded_postings_cache_evicts_total")
				require.NoError(t, err)

				cache.timeNow = func() time.Time {
					return timeNow().Add(5 * c.ttl)
				}

				cache.getPromiseForKey("newKwy", func() (int, int64, error) {
					return 2, 18, nil
				})

				// Should expire all keys again as ttl is expired
				err = promtest.GatherAndCompare(r, bytes.NewBufferString(fmt.Sprintf(`
		# HELP expanded_postings_cache_evicts_total Total number of evictions in the cache, excluding items that got evicted.
		# TYPE expanded_postings_cache_evicts_total counter
        expanded_postings_cache_evicts_total{cache="test",reason="expired"} %v
`, numberOfKeys*2)), "expanded_postings_cache_evicts_total")
				require.NoError(t, err)
			}
		})
	}
}

func repeatStringIfNeeded(seed string, length int) string {
	if len(seed) > length {
		return seed
	}

	return strings.Repeat(seed, 1+length/len(seed))[:max(length, len(seed))]
}

func TestLockRaceExpireSeries(t *testing.T) {
	for j := 0; j < 10; j++ {
		wg := &sync.WaitGroup{}

		c := NewBlocksPostingsForMatchersCache(ExpandedPostingsCacheMetrics{}, 1<<7, 1<<7, 3)
		for i := 0; i < 1000; i++ {
			wg.Add(2)

			go func() {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					c.ExpireSeries(
						labels.FromMap(map[string]string{"__name__": randSeq(10)}),
					)
				}
			}()

			go func() {
				defer wg.Done()

				for i := 0; i < 10; i++ {
					c.getSeedForMetricName(randSeq(10))
				}
			}()
		}
		wg.Wait()
	}
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return string(b)
}
