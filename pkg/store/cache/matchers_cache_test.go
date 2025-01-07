// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestMatchersCache(t *testing.T) {
	testCases := map[string]struct {
		isCacheable func(matcher ConversionLabelMatcher) bool
	}{
		"default": {
			isCacheable: defaultIsCacheableFunc,
		},
		"cache all items": {
			isCacheable: func(matcher ConversionLabelMatcher) bool {
				return true
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cache, err := NewMatchersCache(
				WithSize(2),
				WithIsCacheableFunc(tc.isCacheable),
			)
			testutil.Ok(t, err)

			matcher := &storepb.LabelMatcher{
				Type:  storepb.LabelMatcher_EQ,
				Name:  "key",
				Value: "val",
			}

			matcher2 := &storepb.LabelMatcher{
				Type:  storepb.LabelMatcher_RE,
				Name:  "key2",
				Value: "val2|val3",
			}

			matcher3 := &storepb.LabelMatcher{
				Type:  storepb.LabelMatcher_EQ,
				Name:  "key3",
				Value: "val3",
			}

			var cacheHit bool
			newItem := func(matcher *storepb.LabelMatcher) func() (*labels.Matcher, error) {
				return func() (*labels.Matcher, error) {
					cacheHit = false
					return storepb.MatcherToPromMatcher(*matcher)
				}
			}
			expected := labels.MustNewMatcher(labels.MatchEqual, "key", "val")
			expected2 := labels.MustNewMatcher(labels.MatchRegexp, "key2", "val2|val3")
			expected3 := labels.MustNewMatcher(labels.MatchEqual, "key3", "val3")

			item, err := cache.GetOrSet(matcher, newItem(matcher))
			testutil.Ok(t, err)
			testutil.Equals(t, false, cacheHit)
			testutil.Equals(t, expected.String(), item.String())

			cacheHit = true
			item, err = cache.GetOrSet(matcher, newItem(matcher))
			testutil.Ok(t, err)
			testutil.Equals(t, tc.isCacheable(matcher), cacheHit)
			testutil.Equals(t, expected.String(), item.String())

			cacheHit = true
			item, err = cache.GetOrSet(matcher2, newItem(matcher2))
			testutil.Ok(t, err)
			testutil.Equals(t, false, cacheHit)
			testutil.Equals(t, expected2.String(), item.String())

			cacheHit = true
			item, err = cache.GetOrSet(matcher2, newItem(matcher2))
			testutil.Ok(t, err)
			testutil.Equals(t, tc.isCacheable(matcher2), cacheHit)
			testutil.Equals(t, expected2.String(), item.String())

			cacheHit = true
			item, err = cache.GetOrSet(matcher, newItem(matcher))
			testutil.Ok(t, err)
			testutil.Equals(t, tc.isCacheable(matcher), cacheHit)
			testutil.Equals(t, expected, item)

			cacheHit = true
			item, err = cache.GetOrSet(matcher3, newItem(matcher3))
			testutil.Ok(t, err)
			testutil.Equals(t, false, cacheHit)
			testutil.Equals(t, expected3, item)

			cacheHit = true
			item, err = cache.GetOrSet(matcher2, newItem(matcher2))
			testutil.Ok(t, err)
			testutil.Equals(t, tc.isCacheable(matcher2) && cache.cache.Len() < 2, cacheHit)
			testutil.Equals(t, expected2.String(), item.String())
		})
	}
}

func BenchmarkMatchersCache(b *testing.B) {
	cache, err := NewMatchersCache(WithSize(100))
	if err != nil {
		b.Fatalf("failed to create cache: %v", err)
	}

	matchers := []*storepb.LabelMatcher{
		{Type: storepb.LabelMatcher_EQ, Name: "key1", Value: "val1"},
		{Type: storepb.LabelMatcher_EQ, Name: "key2", Value: "val2"},
		{Type: storepb.LabelMatcher_EQ, Name: "key3", Value: "val3"},
		{Type: storepb.LabelMatcher_EQ, Name: "key4", Value: "val4"},
		{Type: storepb.LabelMatcher_RE, Name: "key5", Value: "^(val5|val6|val7|val8|val9).*$"},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		matcher := matchers[i%len(matchers)]
		_, err := cache.GetOrSet(matcher, func() (*labels.Matcher, error) {
			return storepb.MatcherToPromMatcher(*matcher)
		})
		if err != nil {
			b.Fatalf("failed to get or set cache item: %v", err)
		}
	}
}
