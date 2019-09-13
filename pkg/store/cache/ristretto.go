package storecache

import (
	"github.com/dgraph-io/ristretto"
)

// TinyLFU is a wrapper around Ristretto (TinyLFU).
type TinyLFU struct {
	l *ristretto.Cache
}

// Add adds the key with the specified value.
func (t *TinyLFU) Add(key, val interface{}) {
	v := val.([]byte)
	t.l.Set(key, val, int64(len(v)))
}

// Get gets the key's value.
func (t *TinyLFU) Get(key interface{}) (interface{}, bool) {
	return t.l.Get(key)
}

// RemoveOldest removes the oldest key.
func (t *TinyLFU) RemoveOldest() (interface{}, interface{}, bool) {
	// NOOP
	return nil, nil, false
}

// Purge purges the LRU.
func (t *TinyLFU) Purge() {
	// Recreate the whole cache.
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1 * 1024 * 1024 * 100 * 10,
		MaxCost:     1 * 1024 * 1024 * 100,
		BufferItems: 64,
	})
	// TODO: handle properly.
	if err != nil {
		panic(err)
	}
	t.l = cache
}

// NewTinyLFU returns a new TinyLFU based cache storage which
// calls the given onEvict on eviction.
func NewTinyLFU(onEvict func(key, val interface{})) (StorageCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1 * 1024 * 1024 * 100 * 10,
		MaxCost:     1 * 1024 * 1024 * 100,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	return StorageCache(&TinyLFU{l: cache}), nil
}
