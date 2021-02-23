// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"

	"github.com/go-kit/kit/log"

	"testing"
	"time"

	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestInmemoryCache(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later on.
	key1 := "key1"
	key2 := "key2"
	key3 := "key3"
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := map[string]struct {
		setup        map[string][]byte
		ttl          time.Duration
		testTTL      bool
		mockedErr    error
		fetchKeys    []string
		expectedHits map[string][]byte
	}{
		"should return no hits on empty cache": {
			setup:        nil,
			ttl:          time.Duration(time.Hour),
			fetchKeys:    []string{key1, key2},
			expectedHits: map[string][]byte{},
		},
		"should return no misses on 100% hit ratio": {
			setup: map[string][]byte{
				key1: value1,
				key2: value2,
				key3: value3,
			},
			ttl:       time.Duration(time.Hour),
			fetchKeys: []string{key1},
			expectedHits: map[string][]byte{
				key1: value1,
			},
		},
		"should return a miss due to TTL": {
			setup: map[string][]byte{
				key1: value1,
				key2: value2,
				key3: value3,
			},
			// setting ttl for the values as 250 ms
			ttl:          time.Duration(250 * time.Millisecond),
			testTTL:      true,
			fetchKeys:    []string{key1},
			expectedHits: map[string][]byte{},
		},
		"should return hits and misses on partial hits": {
			setup: map[string][]byte{
				key1: value1,
				key2: value2,
			},
			ttl:          time.Duration(time.Hour),
			fetchKeys:    []string{key1, key3},
			expectedHits: map[string][]byte{key1: value1},
		},
	}
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			conf := []byte(`
max_size: 1MB
max_item_size: 2KB
`)
			c, _ := NewInMemoryCache("test", log.NewNopLogger(), nil, conf)

			// Store the postings expected before running the test.
			ctx := context.Background()
			c.Store(ctx, testData.setup, testData.ttl)

			// Add delay to test expiry functionality.
			if testData.testTTL {
				// The delay will be based on the TTL itself.
				time.Sleep(testData.ttl)
			}

			// Fetch postings from cached and assert on it.
			hits := c.Fetch(ctx, testData.fetchKeys)
			testutil.Equals(t, testData.expectedHits, hits)

			// Assert on metrics.
			testutil.Equals(t, float64(len(testData.fetchKeys)), prom_testutil.ToFloat64(c.requests))
			testutil.Equals(t, float64(len(testData.expectedHits)), prom_testutil.ToFloat64(c.hits))
		})
	}
}

func TestNewInMemoryCache(t *testing.T) {
	// Should return error when the max size of the cache is smaller than the max size of
	conf := []byte(`
max_size: 2KB
max_item_size: 1MB
`)
	cache, err := NewInMemoryCache("test", log.NewNopLogger(), nil, conf)
	testutil.NotOk(t, err)
	testutil.Equals(t, (*InMemoryCache)(nil), cache)
}
