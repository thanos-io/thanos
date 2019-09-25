package storecache

import (
	"math"

	lru "github.com/hashicorp/golang-lru/simplelru"
)

// SimpleLRU is a wrapper around a simple LRU data structure.
type SimpleLRU struct {
	l *lru.LRU

	// keyData is true if the cache retains the information about the key types.
	keyData bool
}

// Add adds the key with the specified value.
func (s *SimpleLRU) Add(key, val interface{}) {
	s.l.Add(key, val)
}

// Get gets the key's value.
func (s *SimpleLRU) Get(key interface{}) (interface{}, bool) {
	return s.l.Get(key)
}

// RemoveOldest removes the oldest key.
func (s *SimpleLRU) RemoveOldest() (interface{}, interface{}, bool) {
	return s.l.RemoveOldest()
}

// Purge purges the LRU.
func (s *SimpleLRU) Purge() {
	s.l.Purge()
}

// KeyData returns if the cache retains key data.
func (s *SimpleLRU) KeyData() bool {
	return true
}

// NewSimpleLRU returns a new simple LRU based cache storage which
// calls the given onEvict on eviction.
func NewSimpleLRU(onEvict func(key, val interface{})) (StorageCache, error) {
	// Initialize LRU cache with a high size limit since we will manage evictions ourselves
	// based on stored size using `RemoveOldest` method.
	l, err := lru.NewLRU(math.MaxInt64, onEvict)
	if err != nil {
		return nil, err
	}
	return StorageCache(&SimpleLRU{l: l}), nil
}
