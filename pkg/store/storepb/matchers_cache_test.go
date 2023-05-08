// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb_test

import (
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestMatchersCache(t *testing.T) {
	now := time.Now()
	nowFunc := func() time.Time {
		return now
	}
	cache := storepb.NewMatchersCache(storepb.WithNowFunc(nowFunc))

	matcher := storepb.LabelMatcher{
		Type:  storepb.LabelMatcher_EQ,
		Name:  "key",
		Value: "val",
	}

	var cacheHit bool
	newItem := func(matcher storepb.LabelMatcher) (*labels.Matcher, error) {
		cacheHit = false
		return storepb.MatcherToPromMatcher(matcher)

	}
	expected := labels.MustNewMatcher(labels.MatchEqual, "key", "val")

	item, err := cache.GetOrSet(matcher, newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, false, cacheHit)
	testutil.Equals(t, expected, item)

	cacheHit = true
	item, err = cache.GetOrSet(matcher, newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, true, cacheHit)
	testutil.Equals(t, expected, item)

	cacheHit = true
	now = now.Add(storepb.CachedMatcherTTL + time.Second)
	cache.RemoveExpired()
	item, err = cache.GetOrSet(matcher, newItem)
	testutil.Ok(t, err)
	testutil.Equals(t, false, cacheHit)
	testutil.Equals(t, expected, item)
}
