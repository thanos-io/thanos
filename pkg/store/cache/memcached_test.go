// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/efficientgo/core/testutil"
)

func TestMemcachedIndexCache_FetchMultiPostings(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	label1 := labels.Label{Name: "instance", Value: "a"}
	label2 := labels.Label{Name: "instance", Value: "b"}
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := map[string]struct {
		setup          []mockedPostings
		mockedErr      error
		fetchBlockID   ulid.ULID
		fetchLabels    []labels.Label
		expectedHits   map[labels.Label][]byte
		expectedMisses []labels.Label
	}{
		"should return no hits on empty cache": {
			setup:          []mockedPostings{},
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label1, label2},
			expectedHits:   nil,
			expectedMisses: []labels.Label{label1, label2},
		},
		"should return no misses on 100% hit ratio": {
			setup: []mockedPostings{
				{block: block1, label: label1, value: value1},
				{block: block1, label: label2, value: value2},
				{block: block2, label: label1, value: value3},
			},
			fetchBlockID: block1,
			fetchLabels:  []labels.Label{label1, label2},
			expectedHits: map[labels.Label][]byte{
				label1: value1,
				label2: value2,
			},
			expectedMisses: nil,
		},
		"should return hits and misses on partial hits": {
			setup: []mockedPostings{
				{block: block1, label: label1, value: value1},
				{block: block2, label: label1, value: value3},
			},
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label1, label2},
			expectedHits:   map[labels.Label][]byte{label1: value1},
			expectedMisses: []labels.Label{label2},
		},
		"should return no hits on memcached error": {
			setup: []mockedPostings{
				{block: block1, label: label1, value: value1},
				{block: block1, label: label2, value: value2},
				{block: block2, label: label1, value: value3},
			},
			mockedErr:      errors.New("mocked error"),
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label1, label2},
			expectedHits:   nil,
			expectedMisses: []labels.Label{label1, label2},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			memcached := newMockedMemcachedClient(testData.mockedErr)
			c, err := NewRemoteIndexCache(log.NewNopLogger(), memcached, nil)
			testutil.Ok(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StorePostings(p.block, p.label, p.value)
			}

			// Fetch postings from cached and assert on it.
			hits, misses := c.FetchMultiPostings(ctx, testData.fetchBlockID, testData.fetchLabels)
			testutil.Equals(t, testData.expectedHits, hits)
			testutil.Equals(t, testData.expectedMisses, misses)

			// Assert on metrics.
			testutil.Equals(t, float64(len(testData.fetchLabels)), prom_testutil.ToFloat64(c.postingRequests))
			testutil.Equals(t, float64(len(testData.expectedHits)), prom_testutil.ToFloat64(c.postingHits))
			testutil.Equals(t, 0.0, prom_testutil.ToFloat64(c.seriesRequests))
			testutil.Equals(t, 0.0, prom_testutil.ToFloat64(c.seriesHits))
		})
	}
}

func TestMemcachedIndexCache_FetchExpandedPostings(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	matcher1 := labels.MustNewMatcher(labels.MatchEqual, "cluster", "us")
	matcher2 := labels.MustNewMatcher(labels.MatchEqual, "job", "thanos")
	matcher3 := labels.MustNewMatcher(labels.MatchRegexp, "__name__", "up")
	value1 := []byte{1}
	value2 := []byte{2}

	tests := map[string]struct {
		setup         []mockedExpandedPostings
		mockedErr     error
		fetchBlockID  ulid.ULID
		fetchMatchers []*labels.Matcher
		expectedHit   bool
		expectedValue []byte
	}{
		"should return no hits on empty cache": {
			setup:         []mockedExpandedPostings{},
			fetchBlockID:  block1,
			fetchMatchers: []*labels.Matcher{matcher1, matcher2},
			expectedHit:   false,
		},
		"should return no misses on 100% hit ratio": {
			setup: []mockedExpandedPostings{
				{block: block1, matchers: []*labels.Matcher{matcher1}, value: value1},
			},
			fetchBlockID:  block1,
			fetchMatchers: []*labels.Matcher{matcher1},
			expectedHit:   true,
			expectedValue: value1,
		},
		"Cache miss when matchers key doesn't match": {
			setup: []mockedExpandedPostings{
				{block: block1, matchers: []*labels.Matcher{matcher1}, value: value1},
				{block: block2, matchers: []*labels.Matcher{matcher2}, value: value2},
			},
			fetchBlockID:  block1,
			fetchMatchers: []*labels.Matcher{matcher1, matcher2},
			expectedHit:   false,
		},
		"should return no hits on memcached error": {
			setup: []mockedExpandedPostings{
				{block: block1, matchers: []*labels.Matcher{matcher3}, value: value1},
			},
			mockedErr:     errors.New("mocked error"),
			fetchBlockID:  block1,
			fetchMatchers: []*labels.Matcher{matcher3},
			expectedHit:   false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			memcached := newMockedMemcachedClient(testData.mockedErr)
			c, err := NewRemoteIndexCache(log.NewNopLogger(), memcached, nil)
			testutil.Ok(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreExpandedPostings(p.block, p.matchers, p.value)
			}

			// Fetch postings from cached and assert on it.
			val, hit := c.FetchExpandedPostings(ctx, testData.fetchBlockID, testData.fetchMatchers)
			testutil.Equals(t, testData.expectedHit, hit)
			if hit {
				testutil.Equals(t, testData.expectedValue, val)
			}

			// Assert on metrics.
			testutil.Equals(t, 1.0, prom_testutil.ToFloat64(c.expandedPostingRequests))
			if testData.expectedHit {
				testutil.Equals(t, 1.0, prom_testutil.ToFloat64(c.expandedPostingHits))
			}
			testutil.Equals(t, 0.0, prom_testutil.ToFloat64(c.postingRequests))
			testutil.Equals(t, 0.0, prom_testutil.ToFloat64(c.postingHits))
			testutil.Equals(t, 0.0, prom_testutil.ToFloat64(c.seriesRequests))
			testutil.Equals(t, 0.0, prom_testutil.ToFloat64(c.seriesHits))
		})
	}
}

func TestMemcachedIndexCache_FetchMultiSeries(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := map[string]struct {
		setup          []mockedSeries
		mockedErr      error
		fetchBlockID   ulid.ULID
		fetchIds       []storage.SeriesRef
		expectedHits   map[storage.SeriesRef][]byte
		expectedMisses []storage.SeriesRef
	}{
		"should return no hits on empty cache": {
			setup:          []mockedSeries{},
			fetchBlockID:   block1,
			fetchIds:       []storage.SeriesRef{1, 2},
			expectedHits:   nil,
			expectedMisses: []storage.SeriesRef{1, 2},
		},
		"should return no misses on 100% hit ratio": {
			setup: []mockedSeries{
				{block: block1, id: 1, value: value1},
				{block: block1, id: 2, value: value2},
				{block: block2, id: 1, value: value3},
			},
			fetchBlockID: block1,
			fetchIds:     []storage.SeriesRef{1, 2},
			expectedHits: map[storage.SeriesRef][]byte{
				1: value1,
				2: value2,
			},
			expectedMisses: nil,
		},
		"should return hits and misses on partial hits": {
			setup: []mockedSeries{
				{block: block1, id: 1, value: value1},
				{block: block2, id: 1, value: value3},
			},
			fetchBlockID:   block1,
			fetchIds:       []storage.SeriesRef{1, 2},
			expectedHits:   map[storage.SeriesRef][]byte{1: value1},
			expectedMisses: []storage.SeriesRef{2},
		},
		"should return no hits on memcached error": {
			setup: []mockedSeries{
				{block: block1, id: 1, value: value1},
				{block: block1, id: 2, value: value2},
				{block: block2, id: 1, value: value3},
			},
			mockedErr:      errors.New("mocked error"),
			fetchBlockID:   block1,
			fetchIds:       []storage.SeriesRef{1, 2},
			expectedHits:   nil,
			expectedMisses: []storage.SeriesRef{1, 2},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			memcached := newMockedMemcachedClient(testData.mockedErr)
			c, err := NewRemoteIndexCache(log.NewNopLogger(), memcached, nil)
			testutil.Ok(t, err)

			// Store the series expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreSeries(p.block, p.id, p.value)
			}

			// Fetch series from cached and assert on it.
			hits, misses := c.FetchMultiSeries(ctx, testData.fetchBlockID, testData.fetchIds)
			testutil.Equals(t, testData.expectedHits, hits)
			testutil.Equals(t, testData.expectedMisses, misses)

			// Assert on metrics.
			testutil.Equals(t, float64(len(testData.fetchIds)), prom_testutil.ToFloat64(c.seriesRequests))
			testutil.Equals(t, float64(len(testData.expectedHits)), prom_testutil.ToFloat64(c.seriesHits))
			testutil.Equals(t, 0.0, prom_testutil.ToFloat64(c.postingRequests))
			testutil.Equals(t, 0.0, prom_testutil.ToFloat64(c.postingHits))
		})
	}
}

type mockedPostings struct {
	block ulid.ULID
	label labels.Label
	value []byte
}

type mockedExpandedPostings struct {
	block    ulid.ULID
	matchers []*labels.Matcher
	value    []byte
}

type mockedSeries struct {
	block ulid.ULID
	id    storage.SeriesRef
	value []byte
}

type mockedMemcachedClient struct {
	cache             map[string][]byte
	mockedGetMultiErr error
}

func newMockedMemcachedClient(mockedGetMultiErr error) *mockedMemcachedClient {
	return &mockedMemcachedClient{
		cache:             map[string][]byte{},
		mockedGetMultiErr: mockedGetMultiErr,
	}
}

func (c *mockedMemcachedClient) GetMulti(ctx context.Context, keys []string) map[string][]byte {
	if c.mockedGetMultiErr != nil {
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

func (c *mockedMemcachedClient) SetAsync(key string, value []byte, ttl time.Duration) error {
	c.cache[key] = value

	return nil
}

func (c *mockedMemcachedClient) Stop() {
	// Nothing to do.
}
