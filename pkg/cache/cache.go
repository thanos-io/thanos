package cache

import (
	"context"
	"time"
)

// Generic cache
type Cache interface {
	// Store data into the cache. If data for given key is nil, data is removed from cache instead.
	// Only positive ttl values are used.
	Store(ctx context.Context, data map[string][]byte, ttl time.Duration)

	// Fetch multiple keys from cache.
	Fetch(ctx context.Context, keys []string) (found map[string][]byte, missing []string)
}
