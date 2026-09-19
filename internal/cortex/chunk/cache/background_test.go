// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package cache_test

import (
	"testing"

	"github.com/thanos-io/thanos/internal/cortex/chunk/cache"
)

func TestBackground(t *testing.T) {
	c := cache.NewBackground("mock", cache.BackgroundConfig{
		WriteBackGoroutines: 1,
		WriteBackBuffer:     100,
	}, cache.NewMockCache(), nil)

	keys, bufs := fillCache(t, c)
	cache.Flush(c)

	testCacheSingle(t, c, keys, bufs)
	testCacheMultiple(t, c, keys, bufs)
	testCacheMiss(t, c)
}
