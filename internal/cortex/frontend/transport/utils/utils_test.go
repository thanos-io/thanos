// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

// Package utils Monitoring platform team helper resources for frontend
package utils

import (
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func verifyMetricCount(t *testing.T, reg *prometheus.Registry, expectedCount int) {
	var (
		mChan = make(chan prometheus.Metric)
	)

	go func() {
		reg.Collect(mChan)
		close(mChan)
	}()
	cnt := 0
	for range mChan {
		cnt++
	}
	if cnt != expectedCount {
		t.Fatalf("Expected %d metrics, got %d", expectedCount, cnt)
	}
}

func TestNewFailedQueryCache(t *testing.T) {
	reg := prometheus.NewRegistry()
	cache, err := NewFailedQueryCache(2, reg)
	if cache == nil {
		t.Fatalf("Expected cache to be created, but got nil")
	}
	if err != nil {
		t.Fatalf("Expected no error message, but got: %s", err.Error())
	}
	verifyMetricCount(t, reg, 2)
}

func TestUpdateFailedQueryCache(t *testing.T) {
	reg := prometheus.NewRegistry()
	cache, _ := NewFailedQueryCache(3, reg)

	tests := []struct {
		name              string
		err               error
		query             url.Values
		expectedResult    bool
		expectedMessage   string
		expectedCacheSize int
	}{
		{
			name: "No error code in error message",
			err:  errors.New("no error code here"),
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"test_query"},
			},
			expectedResult:  false,
			expectedMessage: "String to regex conversion error, cached_query: test_query, query_range_seconds: 100, cached_error: no error code here",
		},
		{
			name: "Non-cacheable error code",
			err:  errors.New("serads;ajkvsd( Code(500) code)asd"),
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"test_query"},
			},
			expectedResult:  false,
			expectedMessage: "Query not cached due to non-cacheable error code, cached_query: test_query, query_range_seconds: 100, cached_error: serads;ajkvsd( Code(500) code)asd",
		},
		{
			name: "Cacheable error code",
			err:  errors.New("This is a random error Code(408). It is random."),
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"test_query"},
			},
			expectedResult:    true,
			expectedMessage:   "Cached a failed query, cached_query: test_query, query_range_seconds: 100, cached_error: This is a random error Code(408). It is random.",
			expectedCacheSize: 1,
		},

		{
			name: "Adding query with whitespace and ensuring it is normalized",
			err:  errors.New("Adding error with query that has whitespace and tabs Code(408). Let's see what happens."),
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"\n \t tes \t  t query  \n"},
			},
			expectedResult:    true,
			expectedMessage:   "Cached a failed query, cached_query:  tes t query , query_range_seconds: 100, cached_error: Adding error with query that has whitespace and tabs Code(408). Let's see what happens.",
			expectedCacheSize: 2,
		},

		{
			name: "Cacheable error code with range of 0, ensuring regex parsing is correct, and updating range length",
			err:  errors.New("error code( Code(408) error.)"),
			query: url.Values{
				"start": {"100"},
				"end":   {"180"},
				"query": {"test_query"},
			},
			expectedResult:    true,
			expectedMessage:   "Cached a failed query, cached_query: test_query, query_range_seconds: 80, cached_error: error code( Code(408) error.)",
			expectedCacheSize: 2,
		},

		{
			name: "Successful update to range length",
			err:  errors.New("error code( Code(408) error.)"),
			query: url.Values{
				"start": {"100"},
				"end":   {"100"},
				"query": {"test_query"},
			},
			expectedResult:    true,
			expectedMessage:   "Cached a failed query, cached_query: test_query, query_range_seconds: 0, cached_error: error code( Code(408) error.)",
			expectedCacheSize: 2,
		},
		{
			name: "Successful update to range length",
			err:  errors.New("error code( Code(408) error.)"),
			query: url.Values{
				"start": {"100"},
				"end":   {"100"},
				"query": {"test_query"},
			},
			expectedResult:    true,
			expectedMessage:   "Cached a failed query, cached_query: test_query, query_range_seconds: 0, cached_error: error code( Code(408) error.)",
			expectedCacheSize: 2,
		},
		{
			name: "Successful update to range length",
			err:  errors.New("rpc error: code = Code(400) desc = {\"status"),
			query: url.Values{
				"start": {"100"},
				"end":   {"100"},
				"query": {"test_query"},
			},
			expectedResult:    true,
			expectedMessage:   "Cached a failed query, cached_query: test_query, query_range_seconds: 0, cached_error: rpc error: code = Code(400) desc = {\"status",
			expectedCacheSize: 2,
		},
		{
			name: "Emtpy query",
			err:  errors.New("error code( Code(400) error.)"),
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {""},
			},
			expectedResult:    false,
			expectedMessage:   "Query parameter is empty",
			expectedCacheSize: 2,
		},
		{
			name: "Successful update to range length",
			err:  errors.New("rpc error: code = Code(400) desc = {\"status"),
			query: url.Values{
				"start": {"100"},
				"end":   {"100"},
				"query": {"test_query1"},
			},
			expectedResult:    true,
			expectedMessage:   "Cached a failed query, cached_query: test_query1, query_range_seconds: 0, cached_error: rpc error: code = Code(400) desc = {\"status",
			expectedCacheSize: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, message := cache.UpdateFailedQueryCache(tt.err, tt.query, time.Second*60)
			if result != tt.expectedResult {
				t.Errorf("expected result %v, got %v", tt.expectedResult, result)
			}
			if message != tt.expectedMessage {
				t.Errorf("expected message to contain %s, got %s", tt.expectedMessage, message)
			}
			expectedCacheSize := tt.expectedCacheSize
			cacheSize := cache.lruCache.Len()
			if cacheSize != expectedCacheSize {
				t.Errorf("expected cache size to be %d, got %d", expectedCacheSize, cacheSize)
			}

			cacheSizeGauge := int(testutil.ToFloat64(cache.cachedQueries))
			if cacheSizeGauge != expectedCacheSize {
				t.Errorf("expected cache size gauge to be %d, got %d", expectedCacheSize, cacheSizeGauge)
			}
		})
	}
	verifyMetricCount(t, reg, 2)
}

// TestQueryHitCache tests the QueryHitCache method
func TestQueryHitCache(t *testing.T) {
	reg := prometheus.NewRegistry()
	cache, _ := NewFailedQueryCache(2, reg)
	lruCache := cache.lruCache

	lruCache.Add("test_query", 100)
	lruCache.Add(" tes t query ", 100)

	tests := []struct {
		name            string
		query           url.Values
		expectedResult  bool
		expectedMessage string
	}{
		{
			name: "Cache hit",
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"test_query"},
			},
			expectedResult:  true,
			expectedMessage: "Blocked a query from failed query cache, cached_query: test_query, cached_range_seconds: 100, query_range_seconds: 100",
		},
		{
			name: "Cache miss",
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"miss"},
			},
			expectedResult:  false,
			expectedMessage: "",
		},

		{
			name: "Cache miss due to shorter range length",
			query: url.Values{
				"start": {"100"},
				"end":   {"150"},
				"query": {"test_query"},
			},
			expectedResult:  false,
			expectedMessage: "",
		},

		{
			name: "Cache hit whitespace",
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {" \n\ttes \tt \n   query \t\n  "},
			},
			expectedResult:  true,
			expectedMessage: "Blocked a query from failed query cache, cached_query:  tes t query , cached_range_seconds: 100, query_range_seconds: 100",
		},

		{
			name: "Cache miss whitespace",
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {" \n\tte s \tt \n   query \t\n  "},
			},
			expectedResult:  false,
			expectedMessage: "",
		},
	}

	verifyMetricCount(t, reg, 2)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, message := cache.QueryHitCache(tt.query)
			if result != tt.expectedResult {
				t.Errorf("expected result %v, got %v", tt.expectedResult, result)
			}
			if message != tt.expectedMessage {
				t.Errorf("expected message to contain %s, got %s", tt.expectedMessage, message)
			}
		})
	}
}

func TestCacheCounterVec(t *testing.T) {
	reg := prometheus.NewRegistry()
	cache, _ := NewFailedQueryCache(2, reg)
	lruCache := cache.lruCache

	lruCache.Add("test_query", 100)
	lruCache.Add(" tes t query ", 100)

	tests := []struct {
		name            string
		query           url.Values
		expectedCounter int
	}{
		{
			name: "Cache miss",
			query: url.Values{
				"start": {"100"},
				"end":   {"2000"},
				"query": {"miss_query_counter_test"},
			},
			expectedCounter: 0,
		}, {
			name: "Cache hit",
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"test_query"},
			},
			expectedCounter: 1,
		},
		{
			name: "Cache miss",
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"miss"},
			},
			expectedCounter: 1,
		},

		{
			name: "Cache miss due to shorter range length",
			query: url.Values{
				"start": {"100"},
				"end":   {"150"},
				"query": {"test_query"},
			},
			expectedCounter: 1,
		},

		{
			name: "Cache hit whitespace",
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {" \n\ttes \tt \n   query \t\n  "},
			},
			expectedCounter: 2,
		},

		{
			name: "Cache miss whitespace",
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {" \n\tte s \tt \n   query \t\n  "},
			},
			expectedCounter: 2,
		},
	}
	verifyMetricCount(t, reg, 2)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache.QueryHitCache(tt.query)
			result := int(testutil.ToFloat64(cache.cachedHits))
			if result != tt.expectedCounter {
				t.Errorf("expected counter value to be %v, got %v", tt.expectedCounter, result)
			}
		})
	}
}

func TestCacheLongRunningFailedQuery(t *testing.T) {
	reg := prometheus.NewRegistry()
	cache, _ := NewFailedQueryCache(3, reg)

	tests := []struct {
		name              string
		err               error
		query             url.Values
	}{
		{
			name: "No error code in error message",
			err:  errors.New("no error code here"),
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"test_query"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Short running failed query without an error code
			cached, _ := cache.UpdateFailedQueryCache(tt.err, tt.query, time.Second*60)
			if cached {
				t.Errorf("Shouldn't cache short running failed query without an error code")
			}
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Long running failed query without an error code
			cached, _ := cache.UpdateFailedQueryCache(tt.err, tt.query, time.Second*(5 * 60 - 1))
			if !cached {
				t.Errorf("Should cache short running failed query without an error code")
			}
		})
	}
	verifyMetricCount(t, reg, 2)
}
