// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

func TestFilterCache(t *testing.T) {
	blockID := ulid.MustNew(ulid.Now(), nil)
	postingKeys := []labels.Label{
		{Name: "foo", Value: "bar"},
	}
	expandedPostingsMatchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
	}
	testPostingData := []byte("postings")
	testExpandedPostingsData := []byte("expandedPostings")
	testSeriesData := []byte("series")
	ctx := context.TODO()
	for _, tc := range []struct {
		name         string
		enabledItems []string
		verifyFunc   func(t *testing.T, c IndexCache)
	}{
		{
			name: "empty enabled items",
			verifyFunc: func(t *testing.T, c IndexCache) {
				c.StorePostings(blockID, postingKeys[0], testPostingData)
				c.StoreExpandedPostings(blockID, expandedPostingsMatchers, testExpandedPostingsData)
				c.StoreSeries(blockID, 1, testSeriesData)

				hits, missed := c.FetchMultiPostings(ctx, blockID, postingKeys)
				testutil.Equals(t, 0, len(missed))
				testutil.Equals(t, testPostingData, hits[postingKeys[0]])

				ep, hit := c.FetchExpandedPostings(ctx, blockID, expandedPostingsMatchers)
				testutil.Equals(t, true, hit)
				testutil.Equals(t, testExpandedPostingsData, ep)

				seriesHit, misses := c.FetchMultiSeries(ctx, blockID, []storage.SeriesRef{1})
				testutil.Equals(t, 0, len(misses))
				testutil.Equals(t, testSeriesData, seriesHit[1])
			},
		},
		{
			name:         "all enabled items",
			enabledItems: []string{cacheTypeSeries, cacheTypePostings, cacheTypeExpandedPostings},
			verifyFunc: func(t *testing.T, c IndexCache) {
				c.StorePostings(blockID, postingKeys[0], testPostingData)
				c.StoreExpandedPostings(blockID, expandedPostingsMatchers, testExpandedPostingsData)
				c.StoreSeries(blockID, 1, testSeriesData)

				hits, missed := c.FetchMultiPostings(ctx, blockID, postingKeys)
				testutil.Equals(t, 0, len(missed))
				testutil.Equals(t, testPostingData, hits[postingKeys[0]])

				ep, hit := c.FetchExpandedPostings(ctx, blockID, expandedPostingsMatchers)
				testutil.Assert(t, true, hit)
				testutil.Equals(t, testExpandedPostingsData, ep)

				seriesHit, misses := c.FetchMultiSeries(ctx, blockID, []storage.SeriesRef{1})
				testutil.Equals(t, 0, len(misses))
				testutil.Equals(t, testSeriesData, seriesHit[1])
			},
		},
		{
			name:         "only enable postings",
			enabledItems: []string{cacheTypePostings},
			verifyFunc: func(t *testing.T, c IndexCache) {
				c.StorePostings(blockID, postingKeys[0], testPostingData)
				c.StoreExpandedPostings(blockID, expandedPostingsMatchers, testExpandedPostingsData)
				c.StoreSeries(blockID, 1, testSeriesData)

				hits, missed := c.FetchMultiPostings(ctx, blockID, postingKeys)
				testutil.Equals(t, 0, len(missed))
				testutil.Equals(t, testPostingData, hits[postingKeys[0]])

				_, hit := c.FetchExpandedPostings(ctx, blockID, expandedPostingsMatchers)
				testutil.Equals(t, false, hit)

				seriesHit, misses := c.FetchMultiSeries(ctx, blockID, []storage.SeriesRef{1})
				testutil.Equals(t, 1, len(misses))
				testutil.Equals(t, 0, len(seriesHit))
			},
		},
		{
			name:         "only enable expanded postings",
			enabledItems: []string{cacheTypeExpandedPostings},
			verifyFunc: func(t *testing.T, c IndexCache) {
				c.StorePostings(blockID, postingKeys[0], testPostingData)
				c.StoreExpandedPostings(blockID, expandedPostingsMatchers, testExpandedPostingsData)
				c.StoreSeries(blockID, 1, testSeriesData)

				hits, missed := c.FetchMultiPostings(ctx, blockID, postingKeys)
				testutil.Equals(t, 1, len(missed))
				testutil.Equals(t, 0, len(hits))

				ep, hit := c.FetchExpandedPostings(ctx, blockID, expandedPostingsMatchers)
				testutil.Equals(t, true, hit)
				testutil.Equals(t, testExpandedPostingsData, ep)

				seriesHit, misses := c.FetchMultiSeries(ctx, blockID, []storage.SeriesRef{1})
				testutil.Equals(t, 1, len(misses))
				testutil.Equals(t, 0, len(seriesHit))
			},
		},
		{
			name:         "only enable series",
			enabledItems: []string{cacheTypeSeries},
			verifyFunc: func(t *testing.T, c IndexCache) {
				c.StorePostings(blockID, postingKeys[0], testPostingData)
				c.StoreExpandedPostings(blockID, expandedPostingsMatchers, testExpandedPostingsData)
				c.StoreSeries(blockID, 1, testSeriesData)

				hits, missed := c.FetchMultiPostings(ctx, blockID, postingKeys)
				testutil.Equals(t, 1, len(missed))
				testutil.Equals(t, 0, len(hits))

				_, hit := c.FetchExpandedPostings(ctx, blockID, expandedPostingsMatchers)
				testutil.Equals(t, false, hit)

				seriesHit, misses := c.FetchMultiSeries(ctx, blockID, []storage.SeriesRef{1})
				testutil.Equals(t, 0, len(misses))
				testutil.Equals(t, testSeriesData, seriesHit[1])
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inMemoryCache, err := NewInMemoryIndexCacheWithConfig(log.NewNopLogger(), nil, prometheus.NewRegistry(), DefaultInMemoryIndexCacheConfig)
			testutil.Ok(t, err)
			c := NewFilteredIndexCache(inMemoryCache, tc.enabledItems)
			tc.verifyFunc(t, c)
		})
	}
}
