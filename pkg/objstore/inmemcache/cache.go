package inmemcache

import (
	"context"
	"io"

	"bytes"
	"io/ioutil"

	"sync"
	"time"

	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Bucket implements the store.Bucket interfaces that wraps given store.Bucket and cache its results in memory.
type Bucket struct {
	mtx     sync.RWMutex
	objects map[string][]byte
	// TODO(Bplotko): Make that a linked list someday.
	timestamps      map[string]time.Time
	wrapped         store.Bucket
	memoryUsedBytes uint64
	memCapBytes     uint64

	timeNow func() time.Time
}

// NewWholeObjectsBucket returns a new in memory cache Bucket that caches naively whole objects, even when only short range was requested.
// That's why this struct is only useful for caching smaller files like index.
// TODO(bplotka): Add proper support for object ranges to nicely cache chunks.
func NewWholeObjectsBucket(reg *prometheus.Registry, wrapped store.Bucket, memCapBytes uint64) *Bucket {
	b := &Bucket{
		objects:     map[string][]byte{},
		timestamps:  map[string]time.Time{},
		wrapped:     wrapped,
		memCapBytes: memCapBytes,

		timeNow: time.Now,
	}

	memUsed := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_wocache_bucket_store_memory_used_bytes",
		Help: "Number of currently used memory in bytes.",
	}, func() float64 {
		b.mtx.RLock()
		defer b.mtx.RUnlock()
		return float64(b.memoryUsedBytes)
	})
	objectsCached := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_wocache_bucket_store_objects_cached",
		Help: "Number of object currently cached.",
	}, func() float64 {
		b.mtx.RLock()
		defer b.mtx.RUnlock()
		return float64(len(b.objects))
	})

	if reg != nil {
		reg.MustRegister(memUsed, objectsCached)
	}

	return b
}

// Iter directly calls wrapped bucket.
// NOTE(bplotka): There is no need to cache/reuse cached objects for now. Not much impact on GCS operations and mem allocations. Add if needed.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	return b.wrapped.Iter(ctx, dir, f)
}

func (b *Bucket) getObject(name string) ([]byte, bool) {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	c, ok := b.objects[name]
	if ok {
		b.timestamps[name] = b.timeNow()
	}
	return c, ok
}

func (b *Bucket) objectFromWrapped(ctx context.Context, name string) ([]byte, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	// Get whole object from wrapped bucket.
	r, err := b.wrapped.Get(ctx, name)
	if err != nil {
		return nil, errors.Wrap(err, "get from wrapped bucket")
	}
	defer r.Close()

	// For now no client of store.Bucket is reading truly asynchronously, so we are ok to fetch synchronously here.
	// TODO(bplotka): Create ReadCloser wrapper if needed to keep async logic for this method.
	c, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "read from wrapped bucket")
	}

	b.objects[name] = c
	b.timestamps[name] = b.timeNow()
	b.memoryUsedBytes += uint64(len(c))
	if b.memoryUsedBytes >= b.memCapBytes {
		// TODO(bplotka): Make that async someday + add time expiry as well (?).
		b.evictOldestObjects()
	}

	return c, nil
}

func (b *Bucket) evictOldestObjects() {
	// TODO(bplotka): Add some extra buffer of memory?
	for b.memCapBytes >= b.memoryUsedBytes {
		var oldestName string
		oldest := b.timeNow()
		for n, t := range b.timestamps {
			if t.Before(oldest) {
				oldest = t
				oldestName = n
			}
		}

		if oldestName == "" {
			return
		}

		delete(b.timestamps, oldestName)
		b.memoryUsedBytes -= uint64(len(b.objects[oldestName]))
		delete(b.objects, oldestName)
	}
}

// Get returns cached reader for the given object name.
// If whole object is not spotted in cache it is retrieved from wrapped bucket given to the client.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	c, ok := b.getObject(name)
	if !ok {
		// Miss or actual not found.
		var err error
		c, err = b.objectFromWrapped(ctx, name)
		if err != nil {
			return nil, err
		}
	}
	return ioutil.NopCloser(bytes.NewReader(c)), nil
}

// GetRange returns a new range reader for the given object name and range.
// If whole object is not spotted in cache it is retrieved from wrapped bucket and range is given to the client.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	c, ok := b.getObject(name)
	if !ok {
		// Miss or actual not found.
		var err error
		c, err = b.objectFromWrapped(ctx, name)
		if err != nil {
			return nil, err
		}
	}

	if int64(len(c)) < off {
		return nil, errors.Errorf("offset larger than content length. Len %d. Offset: %v", len(c), off)
	}

	if int64(len(c)) <= off+length {
		// Just return maximum of what we have.
		length = int64(len(c)) - off
	}

	return ioutil.NopCloser(bytes.NewReader(c[off : off+length])), nil
}
