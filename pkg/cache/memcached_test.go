// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/efficientgo/core/testutil"
)

func TestMemcachedCache(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	key1 := "key1"
	key2 := "key2"
	key3 := "key3"
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := map[string]struct {
		setup        map[string][]byte
		mockedErr    error
		fetchKeys    []string
		expectedHits map[string][]byte
	}{
		"should return no hits on empty cache": {
			setup:        nil,
			fetchKeys:    []string{key1, key2},
			expectedHits: map[string][]byte{},
		},
		"should return no misses on 100% hit ratio": {
			setup: map[string][]byte{
				key1: value1,
				key2: value2,
				key3: value3,
			},
			fetchKeys: []string{key1},
			expectedHits: map[string][]byte{
				key1: value1,
			},
		},
		"should return hits and misses on partial hits": {
			setup: map[string][]byte{
				key1: value1,
				key2: value2,
			},
			fetchKeys:    []string{key1, key3},
			expectedHits: map[string][]byte{key1: value1},
		},
		"should return no hits on memcached error": {
			setup: map[string][]byte{
				key1: value1,
				key2: value2,
				key3: value3,
			},
			mockedErr:    errors.New("mocked error"),
			fetchKeys:    []string{key1},
			expectedHits: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			memcached := newMockedMemcachedClient(testData.mockedErr)
			c := NewMemcachedCache("test", log.NewNopLogger(), memcached, nil)

			// Store the postings expected before running the test.
			ctx := context.Background()
			c.Store(testData.setup, time.Hour)

			// Fetch postings from cached and assert on it.
			hits := c.Fetch(ctx, testData.fetchKeys)
			testutil.Equals(t, testData.expectedHits, hits)

			// Assert on metrics.
			testutil.Equals(t, float64(len(testData.fetchKeys)), prom_testutil.ToFloat64(c.requests))
			testutil.Equals(t, float64(len(testData.expectedHits)), prom_testutil.ToFloat64(c.hits))
		})
	}
}

// mockedMemcachedClient is a mocked memcached client for testing.
type mockedMemcachedClient struct {
	cache       map[string][]byte
	getMultiErr error
}

// GrabKeys implements cacheutil.ReadThroughRemoteCache.
func (c *mockedMemcachedClient) GrabKeys(ctx context.Context, keys []string) map[string][]byte {
	result := make(map[string][]byte)

	for _, key := range keys {
		if value, ok := c.GetMulti(ctx, []string{key})[key]; ok {
			result[key] = value
		}
	}

	return result
}

// newMockedMemcachedClient returns a mocked memcached client.
func newMockedMemcachedClient(getMultiErr error) *mockedMemcachedClient {
	return &mockedMemcachedClient{
		cache:       map[string][]byte{},
		getMultiErr: getMultiErr,
	}
}

func (c *mockedMemcachedClient) GetMulti(_ context.Context, keys []string) map[string][]byte {
	if c.getMultiErr != nil {
		return nil
	}

	hits := map[string][]byte{}

	for _, key := range keys {
		if value, ok := c.cache[key]; ok {
			hits[key] = value
		}
	}

	return hits
}

func (c *mockedMemcachedClient) SetAsync(key string, value []byte, _ time.Duration) error {
	c.cache[key] = value
	return nil
}

func (c *mockedMemcachedClient) Stop() {
	// Nothing to do.
}
