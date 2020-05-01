// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestCachingBucket(t *testing.T) {
	length := int64(1024 * 1024)
	blockSize := int64(16000) // All tests are based on this value.

	data := make([]byte, length)
	for ix := 0; ix < len(data); ix++ {
		data[ix] = byte(ix)
	}

	name := "/test/chunks/000001"

	inmem := objstore.NewInMemBucket()
	testutil.Ok(t, inmem.Upload(context.Background(), name, bytes.NewReader(data)))

	cache := &mockCache{cache: make(map[string][]byte)}

	cachingBucket, err := NewCachingBucket(inmem, cache, DefaultCachingBucketConfig(), nil, nil)
	testutil.Ok(t, err)
	cachingBucket.config.ChunkBlockSize = blockSize

	// Warning, these tests must be run in order, they depend cache state from previous test.
	for _, tc := range []struct {
		name                 string
		init                 func()
		offset               int64
		length               int64
		maxGetRangeRequests  int
		expectedLength       int64
		expectedFetchedBytes int64
		expectedCachedBytes  int64
	}{
		{
			name:                 "basic test",
			offset:               555555,
			length:               55555,
			expectedLength:       55555,
			expectedFetchedBytes: 5 * blockSize,
		},

		{
			name:                "same request will hit all blocks in the cache",
			offset:              555555,
			length:              55555,
			expectedLength:      55555,
			expectedCachedBytes: 5 * blockSize,
		},

		{
			name:                 "request data close to the end of object",
			offset:               length - 10,
			length:               3000,
			expectedLength:       10,
			expectedFetchedBytes: 8576, // Last (incomplete) block is fetched.
		},

		{
			name:                "another request data close to the end of object, cached by previous test",
			offset:              1040100,
			length:              blockSize,
			expectedLength:      8476,
			expectedCachedBytes: 8576,
		},

		{
			name:                 "entire object, combination of cached and uncached blocks",
			offset:               0,
			length:               length,
			expectedLength:       length,
			expectedCachedBytes:  5*blockSize + 8576, // 5 block cached from first test, plus last incomplete block.
			expectedFetchedBytes: 60 * blockSize,
		},

		{
			name:                "entire object again, everything is cached",
			offset:              0,
			length:              length,
			expectedLength:      length,
			expectedCachedBytes: length, // Entire file is now cached.
		},

		{
			name:                 "entire object again, nothing is cached",
			offset:               0,
			length:               length,
			expectedLength:       length,
			expectedFetchedBytes: length,
			expectedCachedBytes:  0, // Cache is flushed.
			init: func() {
				cache.cache = map[string][]byte{} // Flush cache.
			},
		},

		{
			name:                 "missing first blocks",
			offset:               0,
			length:               10 * blockSize,
			expectedLength:       10 * blockSize,
			expectedFetchedBytes: 3 * blockSize,
			expectedCachedBytes:  7 * blockSize,
			init: func() {
				// Delete first 3 blocks.
				delete(cache.cache, cachingKeyObjectBlock(name, 0*blockSize, 1*blockSize))
				delete(cache.cache, cachingKeyObjectBlock(name, 1*blockSize, 2*blockSize))
				delete(cache.cache, cachingKeyObjectBlock(name, 2*blockSize, 3*blockSize))
			},
		},

		{
			name:                 "missing last blocks",
			offset:               0,
			length:               10 * blockSize,
			expectedLength:       10 * blockSize,
			expectedFetchedBytes: 3 * blockSize,
			expectedCachedBytes:  7 * blockSize,
			init: func() {
				// Delete last 3 blocks.
				delete(cache.cache, cachingKeyObjectBlock(name, 7*blockSize, 8*blockSize))
				delete(cache.cache, cachingKeyObjectBlock(name, 8*blockSize, 9*blockSize))
				delete(cache.cache, cachingKeyObjectBlock(name, 9*blockSize, 10*blockSize))
			},
		},

		{
			name:                 "missing middle blocks",
			offset:               0,
			length:               10 * blockSize,
			expectedLength:       10 * blockSize,
			expectedFetchedBytes: 3 * blockSize,
			expectedCachedBytes:  7 * blockSize,
			init: func() {
				// Delete 3 blocks in the middle.
				delete(cache.cache, cachingKeyObjectBlock(name, 3*blockSize, 4*blockSize))
				delete(cache.cache, cachingKeyObjectBlock(name, 4*blockSize, 5*blockSize))
				delete(cache.cache, cachingKeyObjectBlock(name, 5*blockSize, 6*blockSize))
			},
		},

		{
			name:                 "missing everything except middle blocks",
			offset:               0,
			length:               10 * blockSize,
			expectedLength:       10 * blockSize,
			expectedFetchedBytes: 7 * blockSize,
			expectedCachedBytes:  3 * blockSize,
			init: func() {
				// Delete all but 3 blocks in the middle, and keep unlimited number of ranged subrequests.
				for i := int64(0); i < 10; i++ {
					if i > 0 && i%3 == 0 {
						continue
					}
					delete(cache.cache, cachingKeyObjectBlock(name, i*blockSize, (i+1)*blockSize))
				}
			},
		},

		{
			name:                 "missing everything except middle blocks, one subrequest only",
			offset:               0,
			length:               10 * blockSize,
			expectedLength:       10 * blockSize,
			expectedFetchedBytes: 10 * blockSize, // We need to fetch beginning and end in single request.
			expectedCachedBytes:  3 * blockSize,
			maxGetRangeRequests:  1,
			init: func() {
				// Delete all but 3 blocks in the middle, but only allow 1 subrequest.
				for i := int64(0); i < 10; i++ {
					if i == 3 || i == 5 || i == 7 {
						continue
					}
					delete(cache.cache, cachingKeyObjectBlock(name, i*blockSize, (i+1)*blockSize))
				}
			},
		},

		{
			name:                 "missing everything except middle blocks, two subrequests",
			offset:               0,
			length:               10 * blockSize,
			expectedLength:       10 * blockSize,
			expectedFetchedBytes: 7 * blockSize,
			expectedCachedBytes:  3 * blockSize,
			maxGetRangeRequests:  2,
			init: func() {
				// Delete all but one blocks in the middle, and allow 2 subrequests. They will be: 0-80000, 128000-160000.
				for i := int64(0); i < 10; i++ {
					if i == 5 || i == 6 || i == 7 {
						continue
					}
					delete(cache.cache, cachingKeyObjectBlock(name, i*blockSize, (i+1)*blockSize))
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.init != nil {
				tc.init()
			}
			cachedBytes, fetchedBytes := &counter{}, &counter{}
			cachingBucket.cachedChunkBytes = cachedBytes
			cachingBucket.fetchedChunkBytes = fetchedBytes
			cachingBucket.config.MaxChunksGetRangeRequests = tc.maxGetRangeRequests

			verifyGetRange(t, cachingBucket, name, tc.offset, tc.length, tc.expectedLength)
			testutil.Equals(t, tc.expectedCachedBytes, int64(cachedBytes.Get()))
			testutil.Equals(t, tc.expectedFetchedBytes, int64(fetchedBytes.Get()))
		})
	}
}

func verifyGetRange(t *testing.T, cachingBucket *CachingBucket, name string, offset, length int64, expectedLength int64) {
	t.Helper()

	r, err := cachingBucket.GetRange(context.Background(), name, offset, length)
	testutil.Ok(t, err)

	read, err := ioutil.ReadAll(r)
	testutil.Ok(t, err)
	testutil.Equals(t, expectedLength, int64(len(read)))

	for ix := 0; ix < len(read); ix++ {
		if byte(ix)+byte(offset) != read[ix] {
			t.Fatalf("bytes differ at position %d", ix)
		}
	}
}

type mockCache struct {
	mu    sync.Mutex
	cache map[string][]byte
}

func (m *mockCache) Store(_ context.Context, data map[string][]byte, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, val := range data {
		m.cache[key] = val
	}
}

func (m *mockCache) Fetch(_ context.Context, keys []string) (found map[string][]byte, missing []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	found = make(map[string][]byte)

	for _, k := range keys {
		v, ok := m.cache[k]
		if ok {
			found[k] = v
		} else {
			missing = append(missing, k)
		}
	}

	return
}

func TestMergeRanges(t *testing.T) {
	for ix, tc := range []struct {
		input    []rng
		limit    int64
		expected []rng
	}{
		{
			input:    nil,
			limit:    0,
			expected: nil,
		},

		{
			input:    []rng{{start: 0, end: 100}, {start: 100, end: 200}},
			limit:    0,
			expected: []rng{{start: 0, end: 200}},
		},

		{
			input:    []rng{{start: 0, end: 100}, {start: 500, end: 1000}},
			limit:    300,
			expected: []rng{{start: 0, end: 100}, {start: 500, end: 1000}},
		},
		{
			input:    []rng{{start: 0, end: 100}, {start: 500, end: 1000}},
			limit:    400,
			expected: []rng{{start: 0, end: 1000}},
		},
	} {
		t.Run(fmt.Sprintf("%d", ix), func(t *testing.T) {
			testutil.Equals(t, tc.expected, mergeRanges(tc.input, tc.limit))
		})
	}
}

type counter struct {
	mu    sync.Mutex
	count float64
}

func (c *counter) Desc() *prometheus.Desc             { return nil }
func (c *counter) Write(_ *dto.Metric) error          { return nil }
func (c *counter) Describe(_ chan<- *prometheus.Desc) {}
func (c *counter) Collect(_ chan<- prometheus.Metric) {}
func (c *counter) Inc() {
	c.mu.Lock()
	c.count++
	c.mu.Unlock()
}
func (c *counter) Add(f float64) {
	c.mu.Lock()
	c.count += f
	c.mu.Unlock()
}
func (c *counter) Get() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}
