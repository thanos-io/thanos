// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package cache_test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/internal/cortex/chunk/cache"
)

func fillCache(t *testing.T, cache cache.Cache) ([]string, [][]byte) {
	keys := []string{}
	bufs := [][]byte{}
	for i := 0; i < 111; i++ {
		keys = append(keys, fmt.Sprintf("test%d", i))
		bufs = append(bufs, []byte(fmt.Sprintf("buf%d", i)))
	}

	cache.Store(context.Background(), keys, bufs)
	return keys, bufs
}

func testCacheSingle(t *testing.T, cache cache.Cache, keys []string, data [][]byte) {
	for i := 0; i < 100; i++ {
		index := rand.Intn(len(keys))
		key := keys[index]

		found, bufs, missingKeys := cache.Fetch(context.Background(), []string{key})
		require.Len(t, found, 1)
		require.Len(t, bufs, 1)
		require.Len(t, missingKeys, 0)
		require.Equal(t, data[index], bufs[0])
	}
}

func testCacheMultiple(t *testing.T, cache cache.Cache, keys []string, data [][]byte) {
	// test getting them all
	found, bufs, missingKeys := cache.Fetch(context.Background(), keys)
	require.Len(t, found, len(keys))
	require.Len(t, bufs, len(keys))
	require.Len(t, missingKeys, 0)

	result := [][]byte{}
	for i := range found {
		result = append(result, bufs[i])
	}
	require.Equal(t, data, result)
}

func testCacheMiss(t *testing.T, cache cache.Cache) {
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(rand.Int()) // arbitrary key which should fail: no chunk key is a single integer
		found, bufs, missing := cache.Fetch(context.Background(), []string{key})
		require.Empty(t, found)
		require.Empty(t, bufs)
		require.Len(t, missing, 1)
	}
}

func testCache(t *testing.T, cache cache.Cache) {
	keys, bufs := fillCache(t, cache)
	t.Run("Single", func(t *testing.T) {
		testCacheSingle(t, cache, keys, bufs)
	})
	t.Run("Multiple", func(t *testing.T) {
		testCacheMultiple(t, cache, keys, bufs)
	})
	t.Run("Miss", func(t *testing.T) {
		testCacheMiss(t, cache)
	})
}

func TestMemcache(t *testing.T) {
	t.Run("Unbatched", func(t *testing.T) {
		cache := cache.NewMemcached(cache.MemcachedConfig{}, newMockMemcache(),
			"test", nil, log.NewNopLogger())
		testCache(t, cache)
	})

	t.Run("Batched", func(t *testing.T) {
		cache := cache.NewMemcached(cache.MemcachedConfig{
			BatchSize:   10,
			Parallelism: 3,
		}, newMockMemcache(), "test", nil, log.NewNopLogger())
		testCache(t, cache)
	})
}

func TestFifoCache(t *testing.T) {
	cache := cache.NewFifoCache("test", cache.FifoCacheConfig{MaxSizeItems: 1e3, Validity: 1 * time.Hour},
		nil, log.NewNopLogger())
	testCache(t, cache)
}

func TestSnappyCache(t *testing.T) {
	cache := cache.NewSnappy(cache.NewMockCache(), log.NewNopLogger())
	testCache(t, cache)
}
