// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestCachingBucket(t *testing.T) {
	length := int64(1024 * 1024)
	subrangeSize := int64(16000) // All tests are based on this value.

	data := make([]byte, length)
	for ix := 0; ix < len(data); ix++ {
		data[ix] = byte(ix)
	}

	name := "/test/chunks/000001"

	inmem := objstore.NewInMemBucket()
	testutil.Ok(t, inmem.Upload(context.Background(), name, bytes.NewReader(data)))

	// We reuse cache between tests (!)
	cache := &mockCache{cache: make(map[string][]byte)}

	// Warning, these tests must be run in order, they depend cache state from previous test.
	for _, tc := range []struct {
		name                   string
		init                   func()
		offset                 int64
		length                 int64
		maxGetRangeRequests    int
		expectedLength         int64
		expectedFetchedBytes   int64
		expectedCachedBytes    int64
		expectedRefetchedBytes int64
	}{
		{
			name:                 "basic test",
			offset:               555555,
			length:               55555,
			expectedLength:       55555,
			expectedFetchedBytes: 5 * subrangeSize,
		},

		{
			name:                "same request will hit all subranges in the cache",
			offset:              555555,
			length:              55555,
			expectedLength:      55555,
			expectedCachedBytes: 5 * subrangeSize,
		},

		{
			name:                 "request data close to the end of object",
			offset:               length - 10,
			length:               3000,
			expectedLength:       10,
			expectedFetchedBytes: 8576, // Last (incomplete) subrange is fetched.
		},

		{
			name:                "another request data close to the end of object, cached by previous test",
			offset:              1040100,
			length:              subrangeSize,
			expectedLength:      8476,
			expectedCachedBytes: 8576,
		},

		{
			name:                 "entire object, combination of cached and uncached subranges",
			offset:               0,
			length:               length,
			expectedLength:       length,
			expectedCachedBytes:  5*subrangeSize + 8576, // 5 subrange cached from first test, plus last incomplete subrange.
			expectedFetchedBytes: 60 * subrangeSize,
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
			name:                 "missing first subranges",
			offset:               0,
			length:               10 * subrangeSize,
			expectedLength:       10 * subrangeSize,
			expectedFetchedBytes: 3 * subrangeSize,
			expectedCachedBytes:  7 * subrangeSize,
			init: func() {
				// Delete first 3 subranges.
				delete(cache.cache, cachingKeyObjectSubrange(name, 0*subrangeSize, 1*subrangeSize))
				delete(cache.cache, cachingKeyObjectSubrange(name, 1*subrangeSize, 2*subrangeSize))
				delete(cache.cache, cachingKeyObjectSubrange(name, 2*subrangeSize, 3*subrangeSize))
			},
		},

		{
			name:                 "missing last subranges",
			offset:               0,
			length:               10 * subrangeSize,
			expectedLength:       10 * subrangeSize,
			expectedFetchedBytes: 3 * subrangeSize,
			expectedCachedBytes:  7 * subrangeSize,
			init: func() {
				// Delete last 3 subranges.
				delete(cache.cache, cachingKeyObjectSubrange(name, 7*subrangeSize, 8*subrangeSize))
				delete(cache.cache, cachingKeyObjectSubrange(name, 8*subrangeSize, 9*subrangeSize))
				delete(cache.cache, cachingKeyObjectSubrange(name, 9*subrangeSize, 10*subrangeSize))
			},
		},

		{
			name:                 "missing middle subranges",
			offset:               0,
			length:               10 * subrangeSize,
			expectedLength:       10 * subrangeSize,
			expectedFetchedBytes: 3 * subrangeSize,
			expectedCachedBytes:  7 * subrangeSize,
			init: func() {
				// Delete 3 subranges in the middle.
				delete(cache.cache, cachingKeyObjectSubrange(name, 3*subrangeSize, 4*subrangeSize))
				delete(cache.cache, cachingKeyObjectSubrange(name, 4*subrangeSize, 5*subrangeSize))
				delete(cache.cache, cachingKeyObjectSubrange(name, 5*subrangeSize, 6*subrangeSize))
			},
		},

		{
			name:                 "missing everything except middle subranges",
			offset:               0,
			length:               10 * subrangeSize,
			expectedLength:       10 * subrangeSize,
			expectedFetchedBytes: 7 * subrangeSize,
			expectedCachedBytes:  3 * subrangeSize,
			init: func() {
				// Delete all but 3 subranges in the middle, and keep unlimited number of ranged subrequests.
				for i := int64(0); i < 10; i++ {
					if i > 0 && i%3 == 0 {
						continue
					}
					delete(cache.cache, cachingKeyObjectSubrange(name, i*subrangeSize, (i+1)*subrangeSize))
				}
			},
		},

		{
			name:                   "missing everything except middle subranges, one subrequest only",
			offset:                 0,
			length:                 10 * subrangeSize,
			expectedLength:         10 * subrangeSize,
			expectedFetchedBytes:   7 * subrangeSize,
			expectedCachedBytes:    3 * subrangeSize,
			expectedRefetchedBytes: 3 * subrangeSize, // Entire object fetched, 3 subranges are "refetched".
			maxGetRangeRequests:    1,
			init: func() {
				// Delete all but 3 subranges in the middle, but only allow 1 subrequest.
				for i := int64(0); i < 10; i++ {
					if i == 3 || i == 5 || i == 7 {
						continue
					}
					delete(cache.cache, cachingKeyObjectSubrange(name, i*subrangeSize, (i+1)*subrangeSize))
				}
			},
		},

		{
			name:                 "missing everything except middle subranges, two subrequests",
			offset:               0,
			length:               10 * subrangeSize,
			expectedLength:       10 * subrangeSize,
			expectedFetchedBytes: 7 * subrangeSize,
			expectedCachedBytes:  3 * subrangeSize,
			maxGetRangeRequests:  2,
			init: func() {
				// Delete all but one subranges in the middle, and allow 2 subrequests. They will be: 0-80000, 128000-160000.
				for i := int64(0); i < 10; i++ {
					if i == 5 || i == 6 || i == 7 {
						continue
					}
					delete(cache.cache, cachingKeyObjectSubrange(name, i*subrangeSize, (i+1)*subrangeSize))
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.init != nil {
				tc.init()
			}

			cfg := DefaultCachingBucketConfig()
			cfg.ChunkSubrangeSize = subrangeSize
			cfg.MaxChunksGetRangeRequests = tc.maxGetRangeRequests

			cachingBucket, err := NewCachingBucket(inmem, cache, cfg, nil, nil)
			testutil.Ok(t, err)

			verifyGetRange(t, cachingBucket, name, tc.offset, tc.length, tc.expectedLength)
			testutil.Equals(t, tc.expectedCachedBytes, int64(promtest.ToFloat64(cachingBucket.fetchedChunkBytes.WithLabelValues(originCache))))
			testutil.Equals(t, tc.expectedFetchedBytes, int64(promtest.ToFloat64(cachingBucket.fetchedChunkBytes.WithLabelValues(originBucket))))
			testutil.Equals(t, tc.expectedRefetchedBytes, int64(promtest.ToFloat64(cachingBucket.refetchedChunkBytes.WithLabelValues(originCache))))
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

func (m *mockCache) Fetch(_ context.Context, keys []string) map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	found := make(map[string][]byte, len(keys))

	for _, k := range keys {
		v, ok := m.cache[k]
		if ok {
			found[k] = v
		}
	}

	return found
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
			input:    []rng{{start: 0, end: 100}, {start: 100, end: 200}, {start: 500, end: 1000}},
			limit:    0,
			expected: []rng{{start: 0, end: 200}, {start: 500, end: 1000}},
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

func TestInvalidOffsetAndLength(t *testing.T) {
	b := &testBucket{objstore.NewInMemBucket()}
	c, err := NewCachingBucket(b, &mockCache{cache: make(map[string][]byte)}, DefaultCachingBucketConfig(), nil, nil)
	testutil.Ok(t, err)

	r, err := c.GetRange(context.Background(), "test", -1, 1000)
	testutil.Equals(t, nil, r)
	testutil.NotOk(t, err)

	r, err = c.GetRange(context.Background(), "test", 100, -1)
	testutil.Equals(t, nil, r)
	testutil.NotOk(t, err)
}

type testBucket struct {
	*objstore.InMemBucket
}

func (b *testBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if off < 0 {
		return nil, errors.Errorf("invalid offset: %d", off)
	}

	if length <= 0 {
		return nil, errors.Errorf("invalid length: %d", length)
	}

	return b.InMemBucket.GetRange(ctx, name, off, length)
}
