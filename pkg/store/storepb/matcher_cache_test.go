// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb_test

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestMatchersCache(t *testing.T) {
	cache, err := storepb.NewMatchersCache(storepb.WithSize(2))
	testutil.Ok(t, err)

	matcher := storepb.LabelMatcher{
		Type:  storepb.LabelMatcher_EQ,
		Name:  "key",
		Value: "val",
	}

	matcher2 := storepb.LabelMatcher{
		Type:  storepb.LabelMatcher_RE,
		Name:  "key2",
		Value: "val2|val3",
	}

	matcher3 := storepb.LabelMatcher{
		Type:  storepb.LabelMatcher_EQ,
		Name:  "key3",
		Value: "val3",
	}

	var cacheHit bool
	newItem := func(matcher storepb.LabelMatcher) (*labels.Matcher, error) {
		cacheHit = false
		return storepb.MatcherToPromMatcher(matcher)
	}
	expected := labels.MustNewMatcher(labels.MatchEqual, "key", "val")
	expected2 := labels.MustNewMatcher(labels.MatchRegexp, "key2", "val2|val3")
	expected3 := labels.MustNewMatcher(labels.MatchEqual, "key3", "val3")

	item, err := cache.GetOrSet(matcher, newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, false, cacheHit)
	testutil.Equals(t, expected.String(), item.String())

	cacheHit = true
	item, err = cache.GetOrSet(matcher, newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, true, cacheHit)
	testutil.Equals(t, expected.String(), item.String())

	cacheHit = true
	item, err = cache.GetOrSet(matcher2, newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, false, cacheHit)
	testutil.Equals(t, expected2.String(), item.String())

	cacheHit = true
	item, err = cache.GetOrSet(matcher2, newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, true, cacheHit)
	testutil.Equals(t, expected2.String(), item.String())

	cacheHit = true
	item, err = cache.GetOrSet(matcher, newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, true, cacheHit)
	testutil.Equals(t, expected, item)

	cacheHit = true
	item, err = cache.GetOrSet(matcher3, newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, false, cacheHit)
	testutil.Equals(t, expected3, item)

	cacheHit = true
	item, err = cache.GetOrSet(matcher2, newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, false, cacheHit)
	testutil.Equals(t, expected2.String(), item.String())
}
