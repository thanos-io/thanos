// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

//nolint:unparam
package storecache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/thanos-io/objstore"

	"github.com/efficientgo/core/testutil"
	thanoscache "github.com/thanos-io/thanos/pkg/cache"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/cache/cachekey"
)

const testFilename = "/random_object"

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
				objectSubrange := cachekey.BucketCacheKey{Verb: cachekey.SubrangeVerb, Name: name, Start: 0 * subrangeSize, End: 1 * subrangeSize}
				delete(cache.cache, objectSubrange.String())
				objectSubrange = cachekey.BucketCacheKey{Verb: cachekey.SubrangeVerb, Name: name, Start: 1 * subrangeSize, End: 2 * subrangeSize}
				delete(cache.cache, objectSubrange.String())
				objectSubrange = cachekey.BucketCacheKey{Verb: cachekey.SubrangeVerb, Name: name, Start: 2 * subrangeSize, End: 3 * subrangeSize}
				delete(cache.cache, objectSubrange.String())
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
				objectSubrange := cachekey.BucketCacheKey{Verb: cachekey.SubrangeVerb, Name: name, Start: 7 * subrangeSize, End: 8 * subrangeSize}
				delete(cache.cache, objectSubrange.String())
				objectSubrange = cachekey.BucketCacheKey{Verb: cachekey.SubrangeVerb, Name: name, Start: 8 * subrangeSize, End: 9 * subrangeSize}
				delete(cache.cache, objectSubrange.String())
				objectSubrange = cachekey.BucketCacheKey{Verb: cachekey.SubrangeVerb, Name: name, Start: 9 * subrangeSize, End: 10 * subrangeSize}
				delete(cache.cache, objectSubrange.String())
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
				objectSubrange := cachekey.BucketCacheKey{Verb: cachekey.SubrangeVerb, Name: name, Start: 3 * subrangeSize, End: 4 * subrangeSize}
				delete(cache.cache, objectSubrange.String())
				objectSubrange = cachekey.BucketCacheKey{Verb: cachekey.SubrangeVerb, Name: name, Start: 4 * subrangeSize, End: 5 * subrangeSize}
				delete(cache.cache, objectSubrange.String())
				objectSubrange = cachekey.BucketCacheKey{Verb: cachekey.SubrangeVerb, Name: name, Start: 5 * subrangeSize, End: 6 * subrangeSize}
				delete(cache.cache, objectSubrange.String())
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
					objectSubrange := cachekey.BucketCacheKey{Verb: cachekey.SubrangeVerb, Name: name, Start: i * subrangeSize, End: (i + 1) * subrangeSize}
					delete(cache.cache, objectSubrange.String())
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
					objectSubrange := cachekey.BucketCacheKey{Verb: cachekey.SubrangeVerb, Name: name, Start: i * subrangeSize, End: (i + 1) * subrangeSize}
					delete(cache.cache, objectSubrange.String())
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
					objectSubrange := cachekey.BucketCacheKey{Verb: cachekey.SubrangeVerb, Name: name, Start: i * subrangeSize, End: (i + 1) * subrangeSize}
					delete(cache.cache, objectSubrange.String())
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.init != nil {
				tc.init()
			}

			cfg := thanoscache.NewCachingBucketConfig()
			cfg.CacheGetRange("chunks", cache, isTSDBChunkFile, subrangeSize, time.Hour, time.Hour, tc.maxGetRangeRequests)

			cachingBucket, err := NewCachingBucket(inmem, cfg, nil, nil)
			testutil.Ok(t, err)

			verifyGetRange(t, cachingBucket, name, tc.offset, tc.length, tc.expectedLength)
			testutil.Equals(t, tc.expectedCachedBytes, int64(promtest.ToFloat64(cachingBucket.fetchedGetRangeBytes.WithLabelValues(originCache, "chunks"))))
			testutil.Equals(t, tc.expectedFetchedBytes, int64(promtest.ToFloat64(cachingBucket.fetchedGetRangeBytes.WithLabelValues(originBucket, "chunks"))))
			testutil.Equals(t, tc.expectedRefetchedBytes, int64(promtest.ToFloat64(cachingBucket.refetchedGetRangeBytes.WithLabelValues(originCache, "chunks"))))
		})
	}
}

func verifyGetRange(t *testing.T, cachingBucket *CachingBucket, name string, offset, length, expectedLength int64) {
	r, err := cachingBucket.GetRange(context.Background(), name, offset, length)
	testutil.Ok(t, err)

	read, err := io.ReadAll(r)
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

func (m *mockCache) Store(data map[string][]byte, ttl time.Duration) {
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

func (m *mockCache) Name() string {
	return "mockCache"
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

	cfg := thanoscache.NewCachingBucketConfig()
	cfg.CacheGetRange("chunks", newMockCache(), func(string) bool { return true }, 10000, time.Hour, time.Hour, 3)

	c, err := NewCachingBucket(b, cfg, nil, nil)
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

func TestCachedIter(t *testing.T) {
	inmem := objstore.NewInMemBucket()
	testutil.Ok(t, inmem.Upload(context.Background(), "/file-1", strings.NewReader("hej")))
	testutil.Ok(t, inmem.Upload(context.Background(), "/file-2", strings.NewReader("ahoj")))
	testutil.Ok(t, inmem.Upload(context.Background(), "/file-3", strings.NewReader("hello")))
	testutil.Ok(t, inmem.Upload(context.Background(), "/file-4", strings.NewReader("ciao")))

	allFiles := []string{"/file-1", "/file-2", "/file-3", "/file-4"}

	// We reuse cache between tests (!)
	cache := newMockCache()

	const cfgName = "dirs"
	cfg := thanoscache.NewCachingBucketConfig()
	cfg.CacheIter(cfgName, cache, func(string) bool { return true }, 5*time.Minute, JSONIterCodec{})

	cb, err := NewCachingBucket(inmem, cfg, nil, nil)
	testutil.Ok(t, err)

	verifyIter(t, cb, allFiles, false, cfgName)

	testutil.Ok(t, inmem.Upload(context.Background(), "/file-5", strings.NewReader("nazdar")))
	verifyIter(t, cb, allFiles, true, cfgName) // Iter returns old response.

	cache.flush()
	allFiles = append(allFiles, "/file-5")
	verifyIter(t, cb, allFiles, false, cfgName)

	cache.flush()

	e := errors.Errorf("test error")

	// This iteration returns false. Result will not be cached.
	testutil.Equals(t, e, cb.Iter(context.Background(), "/", func(_ string) error {
		return e
	}))

	// Nothing cached now.
	verifyIter(t, cb, allFiles, false, cfgName)
}

func verifyIter(t *testing.T, cb *CachingBucket, expectedFiles []string, expectedCache bool, cfgName string) {
	hitsBefore := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpIter, cfgName)))

	col := iterCollector{}
	testutil.Ok(t, cb.Iter(context.Background(), "/", col.collect))

	hitsAfter := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpIter, cfgName)))

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

	cfg := thanoscache.NewCachingBucketConfig()
	const cfgName = "test"
	cfg.CacheExists(cfgName, cache, matchAll, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket(inmem, cfg, nil, nil)
	testutil.Ok(t, err)

	verifyExists(t, cb, testFilename, false, false, cfgName)

	testutil.Ok(t, inmem.Upload(context.Background(), testFilename, strings.NewReader("hej")))
	verifyExists(t, cb, testFilename, false, true, cfgName) // Reused cache result.
	cache.flush()
	verifyExists(t, cb, testFilename, true, false, cfgName)

	testutil.Ok(t, inmem.Delete(context.Background(), testFilename))
	verifyExists(t, cb, testFilename, true, true, cfgName) // Reused cache result.
	cache.flush()
	verifyExists(t, cb, testFilename, false, false, cfgName)
}

func TestExistsCachingDisabled(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := newMockCache()

	cfg := thanoscache.NewCachingBucketConfig()
	const cfgName = "test"
	cfg.CacheExists(cfgName, cache, func(string) bool { return false }, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket(inmem, cfg, nil, nil)
	testutil.Ok(t, err)

	verifyExists(t, cb, testFilename, false, false, cfgName)

	testutil.Ok(t, inmem.Upload(context.Background(), testFilename, strings.NewReader("hej")))
	verifyExists(t, cb, testFilename, true, false, cfgName)

	testutil.Ok(t, inmem.Delete(context.Background(), testFilename))
	verifyExists(t, cb, testFilename, false, false, cfgName)
}

func verifyExists(t *testing.T, cb *CachingBucket, file string, exists, fromCache bool, cfgName string) {
	t.Helper()
	hitsBefore := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpExists, cfgName)))
	ok, err := cb.Exists(context.Background(), file)
	testutil.Ok(t, err)
	testutil.Equals(t, exists, ok)
	hitsAfter := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpExists, cfgName)))

	if fromCache {
		testutil.Equals(t, 1, hitsAfter-hitsBefore)
	} else {
		testutil.Equals(t, 0, hitsAfter-hitsBefore)
	}
}

func TestGet(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := newMockCache()

	cfg := thanoscache.NewCachingBucketConfig()
	const cfgName = "metafile"
	cfg.CacheGet(cfgName, cache, matchAll, 1024, 10*time.Minute, 10*time.Minute, 2*time.Minute)
	cfg.CacheExists(cfgName, cache, matchAll, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket(inmem, cfg, nil, nil)
	testutil.Ok(t, err)

	verifyGet(t, cb, testFilename, nil, false, cfgName)
	verifyExists(t, cb, testFilename, false, true, cfgName)

	data := []byte("hello world")
	testutil.Ok(t, inmem.Upload(context.Background(), testFilename, bytes.NewBuffer(data)))

	// Even if file is now uploaded, old data is served from cache.
	verifyGet(t, cb, testFilename, nil, true, cfgName)
	verifyExists(t, cb, testFilename, false, true, cfgName)

	cache.flush()

	verifyGet(t, cb, testFilename, data, false, cfgName)
	verifyGet(t, cb, testFilename, data, true, cfgName)
	verifyExists(t, cb, testFilename, true, true, cfgName)
}

func TestGetTooBigObject(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := newMockCache()

	cfg := thanoscache.NewCachingBucketConfig()
	const cfgName = "metafile"
	// Only allow 5 bytes to be cached.
	cfg.CacheGet(cfgName, cache, matchAll, 5, 10*time.Minute, 10*time.Minute, 2*time.Minute)
	cfg.CacheExists(cfgName, cache, matchAll, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket(inmem, cfg, nil, nil)
	testutil.Ok(t, err)

	data := []byte("hello world")
	testutil.Ok(t, inmem.Upload(context.Background(), testFilename, bytes.NewBuffer(data)))

	// Object is too big, so it will not be stored to cache on first read.
	verifyGet(t, cb, testFilename, data, false, cfgName)
	verifyGet(t, cb, testFilename, data, false, cfgName)
	verifyExists(t, cb, testFilename, true, true, cfgName)
}

func TestGetPartialRead(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	cache := newMockCache()

	cfg := thanoscache.NewCachingBucketConfig()
	const cfgName = "metafile"
	cfg.CacheGet(cfgName, cache, matchAll, 1024, 10*time.Minute, 10*time.Minute, 2*time.Minute)
	cfg.CacheExists(cfgName, cache, matchAll, 10*time.Minute, 2*time.Minute)

	cb, err := NewCachingBucket(inmem, cfg, nil, nil)
	testutil.Ok(t, err)

	data := []byte("hello world")
	testutil.Ok(t, inmem.Upload(context.Background(), testFilename, bytes.NewBuffer(data)))

	// Read only few bytes from data.
	r, err := cb.Get(context.Background(), testFilename)
	testutil.Ok(t, err)
	_, err = r.Read(make([]byte, 1))
	testutil.Ok(t, err)
	testutil.Ok(t, r.Close())

	// Object wasn't cached as it wasn't fully read.
	verifyGet(t, cb, testFilename, data, false, cfgName)
	// VerifyGet read object, so now it's cached.
	verifyGet(t, cb, testFilename, data, true, cfgName)
}

func verifyGet(t *testing.T, cb *CachingBucket, file string, expectedData []byte, cacheUsed bool, cfgName string) {
	hitsBefore := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpGet, cfgName)))

	r, err := cb.Get(context.Background(), file)
	if expectedData == nil {
		testutil.Assert(t, cb.IsObjNotFoundErr(err))

		hitsAfter := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpGet, cfgName)))
		if cacheUsed {
			testutil.Equals(t, 1, hitsAfter-hitsBefore)
		} else {
			testutil.Equals(t, 0, hitsAfter-hitsBefore)
		}
	} else {
		testutil.Ok(t, err)
		defer runutil.CloseWithLogOnErr(nil, r, "verifyGet")
		data, err := io.ReadAll(r)
		testutil.Ok(t, err)
		testutil.Equals(t, expectedData, data)

		hitsAfter := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpGet, cfgName)))
		if cacheUsed {
			testutil.Equals(t, 1, hitsAfter-hitsBefore)
		} else {
			testutil.Equals(t, 0, hitsAfter-hitsBefore)
		}
	}
}

func TestAttributes(t *testing.T) {
	inmem := objstore.NewInMemBucket()

	// We reuse cache between tests (!)
	cache := newMockCache()

	cfg := thanoscache.NewCachingBucketConfig()
	const cfgName = "test"
	cfg.CacheAttributes(cfgName, cache, matchAll, time.Minute)

	cb, err := NewCachingBucket(inmem, cfg, nil, nil)
	testutil.Ok(t, err)

	verifyObjectAttrs(t, cb, testFilename, -1, false, cfgName)
	verifyObjectAttrs(t, cb, testFilename, -1, false, cfgName) // Attributes doesn't cache non-existent files.

	data := []byte("hello world")
	testutil.Ok(t, inmem.Upload(context.Background(), testFilename, bytes.NewBuffer(data)))

	verifyObjectAttrs(t, cb, testFilename, len(data), false, cfgName)
	verifyObjectAttrs(t, cb, testFilename, len(data), true, cfgName)
}

func verifyObjectAttrs(t *testing.T, cb *CachingBucket, file string, expectedLength int, cacheUsed bool, cfgName string) {
	t.Helper()
	hitsBefore := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpAttributes, cfgName)))

	attrs, err := cb.Attributes(context.Background(), file)
	if expectedLength < 0 {
		testutil.Assert(t, cb.IsObjNotFoundErr(err))
	} else {
		testutil.Ok(t, err)
		testutil.Equals(t, int64(expectedLength), attrs.Size)

		hitsAfter := int(promtest.ToFloat64(cb.operationHits.WithLabelValues(objstore.OpAttributes, cfgName)))
		if cacheUsed {
			testutil.Equals(t, 1, hitsAfter-hitsBefore)
		} else {
			testutil.Equals(t, 0, hitsAfter-hitsBefore)
		}
	}
}

func matchAll(string) bool { return true }
