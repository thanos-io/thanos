// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache_test

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"

	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestMatchersCache(t *testing.T) {
	cache, err := storecache.NewMatchersCache(storecache.WithSize(2))
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
	newItem := func(matcher storecache.ConversionLabelMatcher) (*labels.Matcher, error) {
		cacheHit = false
		return storecache.MatcherToPromMatcher(matcher)
	}
	expected := labels.MustNewMatcher(labels.MatchEqual, "key", "val")
	expected2 := labels.MustNewMatcher(labels.MatchRegexp, "key2", "val2|val3")
	expected3 := labels.MustNewMatcher(labels.MatchEqual, "key3", "val3")

	item, err := cache.GetOrSet(cloneMatcher(matcher), newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, false, cacheHit)
	testutil.Equals(t, expected.String(), item.String())

	cacheHit = true
	item, err = cache.GetOrSet(cloneMatcher(matcher), newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, true, cacheHit)
	testutil.Equals(t, expected.String(), item.String())

	cacheHit = true
	item, err = cache.GetOrSet(cloneMatcher(matcher2), newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, false, cacheHit)
	testutil.Equals(t, expected2.String(), item.String())

	cacheHit = true
	item, err = cache.GetOrSet(cloneMatcher(matcher2), newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, true, cacheHit)
	testutil.Equals(t, expected2.String(), item.String())

	cacheHit = true
	item, err = cache.GetOrSet(cloneMatcher(matcher), newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, true, cacheHit)
	testutil.Equals(t, expected, item)

	cacheHit = true
	item, err = cache.GetOrSet(cloneMatcher(matcher3), newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, false, cacheHit)
	testutil.Equals(t, expected3, item)

	cacheHit = true
	item, err = cache.GetOrSet(cloneMatcher(matcher2), newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, false, cacheHit)
	testutil.Equals(t, expected2.String(), item.String())
}

func BenchmarkMatchersCache(b *testing.B) {
	cache, err := storecache.NewMatchersCache(storecache.WithSize(100))
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
		_, err := cache.GetOrSet(matcher, storecache.MatcherToPromMatcher)
		if err != nil {
			b.Fatalf("failed to get or set cache item: %v", err)
		}
	}
}

func cloneMatcher(m *storepb.LabelMatcher) *storepb.LabelMatcher {
	return &storepb.LabelMatcher{
		Type:  m.Type,
		Name:  m.Name,
		Value: m.Value,
	}
}
