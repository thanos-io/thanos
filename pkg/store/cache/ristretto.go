package storecache

import (
	"github.com/dgraph-io/ristretto"
	"github.com/dgraph-io/ristretto/z"
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
	t.l.Clear()
}

// KeyData returns if the cache retains key data.
func (t *TinyLFU) KeyData() bool {
	return false
}

// NewTinyLFU returns a new TinyLFU based cache storage which
// calls the given onEvict on eviction.
func NewTinyLFU(onEvict func(key uint64, conflict uint64, val interface{}, cost int64), maxSize int64) (StorageCache, error) {
	ctrs := maxSize / 1000 // Seems like a good value, ad-hoc calculation of cache size divided by avg. cache item's size.
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: ctrs,
		MaxCost:     maxSize,
		BufferItems: 64, // Value that should give good enough performance.
		OnEvict:     onEvict,
		KeyToHash: func(key interface{}) (uint64, uint64) {
			k := key.(cacheKey)
			b := [16]byte(k.block)

			var d uint64

			keyType := k.keyType()
			switch keyType {
			case cacheTypePostings:
				datum := k.key.(cacheKeyPostings)
				d, _ = z.KeyToHash(datum.Name + datum.Value)
			case cacheTypeSeries:
				datum := k.key.(cacheKeySeries)
				d, _ = z.KeyToHash(uint64(datum))
			}
			hashedBlock, _ := z.KeyToHash(b[:])
			return z.KeyToHash(hashedBlock + d)
		},
	})
	if err != nil {
		return nil, err
	}
	return StorageCache(&TinyLFU{l: cache}), nil
}
