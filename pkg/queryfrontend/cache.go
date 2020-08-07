// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"time"

	cortexcache "github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
)

// NewFifoCacheConfig creates Cortex fifo cache config.
func NewFifoCacheConfig(maxSizeBytes string, maxSizeItems int, ttl time.Duration) queryrange.ResultsCacheConfig {
	return queryrange.ResultsCacheConfig{
		CacheConfig: cortexcache.Config{
			EnableFifoCache: true,
			Fifocache: cortexcache.FifoCacheConfig{
				MaxSizeBytes: maxSizeBytes,
				MaxSizeItems: maxSizeItems,
				Validity:     ttl,
			},
		},
	}
}
