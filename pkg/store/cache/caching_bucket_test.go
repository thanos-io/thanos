// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestChunksCaching(t *testing.T) {
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
	cache := newMockCache()

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
				cache.flush()
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
			cfg.SubrangeSize = subrangeSize
			cfg.MaxGetRangeRequests = tc.maxGetRangeRequests

			cachingBucket, err := NewCachingBucket(inmem, nil, nil)
			testutil.Ok(t, err)
			cachingBucket.CacheGetRange("chunks", cache, isTSDBChunkFile, cfg)

			verifyGetRange(t, cachingBucket, name, tc.offset, tc.length, tc.expectedLength)
			testutil.Equals(t, tc.expectedCachedBytes, int64(promtest.ToFloat64(cachingBucket.fetchedGetRangeBytes.WithLabelValues(originCache, "chunks"))))
			testutil.Equals(t, tc.expectedFetchedBytes, int64(promtest.ToFloat64(cachingBucket.fetchedGetRangeBytes.WithLabelValues(originBucket, "chunks"))))
			testutil.Equals(t, tc.expectedRefetchedBytes, int64(promtest.ToFloat64(cachingBucket.refetchedGetRangeBytes.WithLabelValues(originCache, "chunks"))))
		})
	}
}

func verifyGetRange(t *testing.T, cachingBucket *CachingBucket, name string, offset, length int64, expectedLength int64) {
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

type cacheItem struct {
	data []byte
	exp  time.Time
}

type mockCache struct {
	mu    sync.Mutex
	cache map[string]cacheItem
}

func newMockCache() *mockCache {
	c := &mockCache{}
	c.flush()
	return c
}

func (m *mockCache) Store(_ context.Context, data map[string][]byte, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	exp := time.Now().Add(ttl)
	for key, val := range data {
		m.cache[key] = cacheItem{data: val, exp: exp}
	}
}

func (m *mockCache) Fetch(_ context.Context, keys []string) map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	found := make(map[string][]byte, len(keys))

	now := time.Now()
	for _, k := range keys {
		v, ok := m.cache[k]
		if ok && now.Before(v.exp) {
			found[k] = v.data
		}
	}

	return found
}

func (m *mockCache) flush() {
	m.cache = map[string]cacheItem{}
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
	c, err := NewCachingBucket(b, nil, nil)
	testutil.Ok(t, err)
	c.CacheGetRange("chunks", newMockCache(), func(string) bool { return true }, DefaultCachingBucketConfig())

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

func TestCachedIter(t *testing.T) {
	inmem := objstore.NewInMemBucket()
	testutil.Ok(t, inmem.Upload(context.Background(), "/file-1", strings.NewReader("hej")))
	testutil.Ok(t, inmem.Upload(context.Background(), "/file-2", strings.NewReader("ahoj")))
	testutil.Ok(t, inmem.Upload(context.Background(), "/file-3", strings.NewReader("hello")))
	testutil.Ok(t, inmem.Upload(context.Background(), "/file-4", strings.NewReader("ciao")))

	allFiles := []string{"/file-1", "/file-2", "/file-3", "/file-4"}

	// We reuse cache between tests (!)
	cache := newMockCache()

	config := DefaultCachingBucketConfig()

	cb, err := NewCachingBucket(inmem, nil, nil)
	testutil.Ok(t, err)
	cb.CacheIter("dirs", cache, func(string) bool { return true }, config)

	verifyIter(t, cb, allFiles, false)

	testutil.Ok(t, inmem.Upload(context.Background(), "/file-5", strings.NewReader("nazdar")))
	verifyIter(t, cb, allFiles, true) // Iter returns old response.

	cache.flush()
	allFiles = append(allFiles, "/file-5")
	verifyIter(t, cb, allFiles, false)

	cache.flush()

	e := errors.Errorf("test error")

	// This iteration returns false. Result will not be cached.
	testutil.Equals(t, e, cb.Iter(context.Background(), "/", func(_ string) error {
		return e
	}))

	// Nothing cached now.
	verifyIter(t, cb, allFiles, false)
}

func verifyIter(t *testing.T, cb *CachingBucket, expectedFiles []string, expectedCache bool) {
	hitsBefore := int(promtest.ToFloat64(cb.iterCacheHits))

	col := iterCollector{}
	testutil.Ok(t, cb.Iter(context.Background(), "/", col.collect))

	hitsAfter := int(promtest.ToFloat64(cb.iterCacheHits))

	sort.Strings(col.items)
	testutil.Equals(t, expectedFiles, col.items)

	expectedHitsDiff := 0
	if expectedCache {
		expectedHitsDiff = 1
	}

	testutil.Equals(t, expectedHitsDiff, hitsAfter-hitsBefore)
}

type iterCollector struct {
	items []string
}

func (it *iterCollector) collect(s string) error {
	it.items = append(it.items, s)
	return nil
}

func TestExists(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := newMockCache()

	config := DefaultCachingBucketConfig()

	cb, err := NewCachingBucket(inmem, nil, nil)
	testutil.Ok(t, err)
	cb.CacheExists("test", cache, matchAll, config)

	file := "/block123" + metaFilenameSuffix
	verifyExists(t, cb, file, false, false, "test")

	testutil.Ok(t, inmem.Upload(context.Background(), file, strings.NewReader("hej")))
	verifyExists(t, cb, file, false, true, "test") // Reused cache result.
	cache.flush()
	verifyExists(t, cb, file, true, false, "test")

	testutil.Ok(t, inmem.Delete(context.Background(), file))
	verifyExists(t, cb, file, true, true, "test") // Reused cache result.
	cache.flush()
	verifyExists(t, cb, file, false, false, "test")
}

func TestExistsCachingDisabled(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := newMockCache()

	cb, err := NewCachingBucket(inmem, nil, nil)
	testutil.Ok(t, err)
	cb.CacheExists("test", cache, func(string) bool { return false }, DefaultCachingBucketConfig())

	file := "/block123" + metaFilenameSuffix
	verifyExists(t, cb, file, false, false, "test")

	testutil.Ok(t, inmem.Upload(context.Background(), file, strings.NewReader("hej")))
	verifyExists(t, cb, file, true, false, "test")

	testutil.Ok(t, inmem.Delete(context.Background(), file))
	verifyExists(t, cb, file, false, false, "test")
}

func verifyExists(t *testing.T, cb *CachingBucket, file string, exists bool, fromCache bool, label string) {
	t.Helper()
	hitsBefore := int(promtest.ToFloat64(cb.existsCacheHits.WithLabelValues(label)))
	ok, err := cb.Exists(context.Background(), file)
	testutil.Ok(t, err)
	testutil.Equals(t, exists, ok)
	hitsAfter := int(promtest.ToFloat64(cb.existsCacheHits.WithLabelValues(label)))

	if fromCache {
		testutil.Equals(t, 1, hitsAfter-hitsBefore)
	} else {
		testutil.Equals(t, 0, hitsAfter-hitsBefore)
	}
}

func TestGetMetafile(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := newMockCache()

	cb, err := NewCachingBucket(inmem, nil, nil)
	testutil.Ok(t, err)
	cb.CacheGet("metafile", cache, matchAll, DefaultCachingBucketConfig())
	cb.CacheExists("metafile", cache, matchAll, DefaultCachingBucketConfig())

	file := "/block123" + metaFilenameSuffix
	verifyGet(t, cb, file, nil, false, "test")

	data := []byte("hello world")
	testutil.Ok(t, inmem.Upload(context.Background(), file, bytes.NewBuffer(data)))

	// Even if file is now uploaded, old data is served from cache.
	verifyGet(t, cb, file, nil, true, "metafile")
	verifyExists(t, cb, file, false, true, "metafile")

	cache.flush()

	verifyGet(t, cb, file, data, false, "metafile")
	verifyGet(t, cb, file, data, true, "metafile")
	verifyExists(t, cb, file, true, true, "metafile")
}

func verifyGet(t *testing.T, cb *CachingBucket, file string, expectedData []byte, cacheUsed bool, label string) {
	hitsBefore := int(promtest.ToFloat64(cb.getCacheHits.WithLabelValues(label)))

	r, err := cb.Get(context.Background(), file)
	if expectedData == nil {
		testutil.Assert(t, cb.IsObjNotFoundErr(err))

		hitsAfter := int(promtest.ToFloat64(cb.getCacheHits.WithLabelValues(label)))
		if cacheUsed {
			testutil.Equals(t, 1, hitsAfter-hitsBefore)
		} else {
			testutil.Equals(t, 0, hitsAfter-hitsBefore)
		}
	} else {
		testutil.Ok(t, err)
		runutil.CloseWithLogOnErr(nil, r, "verifyGet")
		data, err := ioutil.ReadAll(r)
		testutil.Ok(t, err)
		testutil.Equals(t, expectedData, data)

		hitsAfter := int(promtest.ToFloat64(cb.getCacheHits.WithLabelValues(label)))
		if cacheUsed {
			testutil.Equals(t, 1, hitsAfter-hitsBefore)
		} else {
			testutil.Equals(t, 0, hitsAfter-hitsBefore)
		}
	}
}

func matchAll(string) bool { return true }
