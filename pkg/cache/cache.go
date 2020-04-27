// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"time"
)

// Generic cache
type Cache interface {
	// Store data into the cache.
	//
	// Note that individual byte buffers may be retained by the cache!
	Store(ctx context.Context, data map[string][]byte, ttl time.Duration)

	// Fetch multiple keys from cache.
	Fetch(ctx context.Context, keys []string) (found map[string][]byte, missing []string)
}
