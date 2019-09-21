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
	// NOOP since TinyLFU is size restricted itself.
	return nil, nil, false
}

// Purge purges the LRU.
func (t *TinyLFU) Purge() {
	// NOOP since TinyLFU is size restricted itself.
}

// NewTinyLFU returns a new TinyLFU based cache storage which
// calls the given onEvict on eviction.
func NewTinyLFU(onEvict func(key uint64, val interface{}, cost int64), maxSize int64) (StorageCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: maxSize * 10,
		MaxCost:     maxSize,
		BufferItems: 64,
		OnEvict:     onEvict,
	})
	if err != nil {
		return nil, err
	}
	return StorageCache(&TinyLFU{l: cache}), nil
}
