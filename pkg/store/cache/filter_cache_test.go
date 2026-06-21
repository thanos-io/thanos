// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/thanos/pkg/tenancy"
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
		name          string
		enabledItems  []string
		expectedError string
		verifyFunc    func(t *testing.T, c IndexCache)
	}{
		{
			name:          "invalid item type",
			expectedError: "unsupported item type foo",
			enabledItems:  []string{"foo"},
		},
		{
			name:          "invalid item type with 1 valid cache type",
			expectedError: "unsupported item type foo",
			enabledItems:  []string{CacheTypeExpandedPostings, "foo"},
		},
		{
			name: "empty enabled items",
			verifyFunc: func(t *testing.T, c IndexCache) {
				c.StorePostings(blockID, postingKeys[0], testPostingData, tenancy.DefaultTenant, 0)
				c.StoreExpandedPostings(blockID, expandedPostingsMatchers, testExpandedPostingsData, tenancy.DefaultTenant, 0)
				c.StoreSeries(blockID, 1, testSeriesData, tenancy.DefaultTenant, 0)

				hits, missed := c.FetchMultiPostings(ctx, blockID, postingKeys, tenancy.DefaultTenant)
				testutil.Equals(t, 0, len(missed))
				testutil.Equals(t, testPostingData, hits[postingKeys[0]])

				ep, hit := c.FetchExpandedPostings(ctx, blockID, expandedPostingsMatchers, tenancy.DefaultTenant)
				testutil.Equals(t, true, hit)
				testutil.Equals(t, testExpandedPostingsData, ep)

				seriesHit, misses := c.FetchMultiSeries(ctx, blockID, []storage.SeriesRef{1}, tenancy.DefaultTenant)
				testutil.Equals(t, 0, len(misses))
				testutil.Equals(t, testSeriesData, seriesHit[1])
			},
		},
		{
			name:         "all enabled items",
			enabledItems: []string{CacheTypeSeries, CacheTypePostings, CacheTypeExpandedPostings},
			verifyFunc: func(t *testing.T, c IndexCache) {
				c.StorePostings(blockID, postingKeys[0], testPostingData, tenancy.DefaultTenant, 0)
				c.StoreExpandedPostings(blockID, expandedPostingsMatchers, testExpandedPostingsData, tenancy.DefaultTenant, 0)
				c.StoreSeries(blockID, 1, testSeriesData, tenancy.DefaultTenant, 0)

				hits, missed := c.FetchMultiPostings(ctx, blockID, postingKeys, tenancy.DefaultTenant)
				testutil.Equals(t, 0, len(missed))
				testutil.Equals(t, testPostingData, hits[postingKeys[0]])

				ep, hit := c.FetchExpandedPostings(ctx, blockID, expandedPostingsMatchers, tenancy.DefaultTenant)
				testutil.Assert(t, true, hit)
				testutil.Equals(t, testExpandedPostingsData, ep)

				seriesHit, misses := c.FetchMultiSeries(ctx, blockID, []storage.SeriesRef{1}, tenancy.DefaultTenant)
				testutil.Equals(t, 0, len(misses))
				testutil.Equals(t, testSeriesData, seriesHit[1])
			},
		},
		{
			name:         "only enable postings",
			enabledItems: []string{CacheTypePostings},
			verifyFunc: func(t *testing.T, c IndexCache) {
				c.StorePostings(blockID, postingKeys[0], testPostingData, tenancy.DefaultTenant, 0)
				c.StoreExpandedPostings(blockID, expandedPostingsMatchers, testExpandedPostingsData, tenancy.DefaultTenant, 0)
				c.StoreSeries(blockID, 1, testSeriesData, tenancy.DefaultTenant, 0)

				hits, missed := c.FetchMultiPostings(ctx, blockID, postingKeys, tenancy.DefaultTenant)
				testutil.Equals(t, 0, len(missed))
				testutil.Equals(t, testPostingData, hits[postingKeys[0]])

				_, hit := c.FetchExpandedPostings(ctx, blockID, expandedPostingsMatchers, tenancy.DefaultTenant)
				testutil.Equals(t, false, hit)

				seriesHit, misses := c.FetchMultiSeries(ctx, blockID, []storage.SeriesRef{1}, tenancy.DefaultTenant)
				testutil.Equals(t, 1, len(misses))
				testutil.Equals(t, 0, len(seriesHit))
			},
		},
		{
			name:         "only enable expanded postings",
			enabledItems: []string{CacheTypeExpandedPostings},
			verifyFunc: func(t *testing.T, c IndexCache) {
				c.StorePostings(blockID, postingKeys[0], testPostingData, tenancy.DefaultTenant, 0)
				c.StoreExpandedPostings(blockID, expandedPostingsMatchers, testExpandedPostingsData, tenancy.DefaultTenant, 0)
				c.StoreSeries(blockID, 1, testSeriesData, tenancy.DefaultTenant, 0)

				hits, missed := c.FetchMultiPostings(ctx, blockID, postingKeys, tenancy.DefaultTenant)
				testutil.Equals(t, 1, len(missed))
				testutil.Equals(t, 0, len(hits))

				ep, hit := c.FetchExpandedPostings(ctx, blockID, expandedPostingsMatchers, tenancy.DefaultTenant)
				testutil.Equals(t, true, hit)
				testutil.Equals(t, testExpandedPostingsData, ep)

				seriesHit, misses := c.FetchMultiSeries(ctx, blockID, []storage.SeriesRef{1}, tenancy.DefaultTenant)
				testutil.Equals(t, 1, len(misses))
				testutil.Equals(t, 0, len(seriesHit))
			},
		},
		{
			name:         "only enable series",
			enabledItems: []string{CacheTypeSeries},
			verifyFunc: func(t *testing.T, c IndexCache) {
				c.StorePostings(blockID, postingKeys[0], testPostingData, tenancy.DefaultTenant, 0)
				c.StoreExpandedPostings(blockID, expandedPostingsMatchers, testExpandedPostingsData, tenancy.DefaultTenant, 0)
				c.StoreSeries(blockID, 1, testSeriesData, tenancy.DefaultTenant, 0)

				hits, missed := c.FetchMultiPostings(ctx, blockID, postingKeys, tenancy.DefaultTenant)
				testutil.Equals(t, 1, len(missed))
				testutil.Equals(t, 0, len(hits))

				_, hit := c.FetchExpandedPostings(ctx, blockID, expandedPostingsMatchers, tenancy.DefaultTenant)
				testutil.Equals(t, false, hit)

				seriesHit, misses := c.FetchMultiSeries(ctx, blockID, []storage.SeriesRef{1}, tenancy.DefaultTenant)
				testutil.Equals(t, 0, len(misses))
				testutil.Equals(t, testSeriesData, seriesHit[1])
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inMemoryCache, err := NewInMemoryIndexCacheWithConfig(log.NewNopLogger(), nil, prometheus.NewRegistry(), DefaultInMemoryIndexCacheConfig)
			testutil.Ok(t, err)
			err = ValidateEnabledItems(tc.enabledItems)
			if tc.expectedError != "" {
				testutil.Equals(t, tc.expectedError, err.Error())
			} else {
				testutil.Ok(t, err)
				c := NewFilteredIndexCache(inMemoryCache, tc.enabledItems)
				tc.verifyFunc(t, c)
			}
		})
	}
}
