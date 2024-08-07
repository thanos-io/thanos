// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"

	"github.com/efficientgo/core/testutil"
)

func TestGenerateCacheKey(t *testing.T) {
	intervalFn := func(r queryrange.Request) time.Duration { return hour }
	splitter := newThanosCacheKeyGenerator(intervalFn)

	for _, tc := range []struct {
		name     string
		req      queryrange.Request
		expected string
	}{
		{
			name: "non thanos req",
			req: &queryrange.PrometheusRequest{
				Query: "up",
				Start: 0,
				Step:  60 * seconds,
			},
			expected: "fe::up:60000:0",
		},
		{
			name: "non downsampling resolution specified",
			req: &ThanosQueryRangeRequest{
				Query: "up",
				Start: 0,
				Step:  60 * seconds,
			},
			expected: "fe::up:60000:0:2:-:0:",
		},
		{
			name: "10s step",
			req: &ThanosQueryRangeRequest{
				Query: "up",
				Start: 0,
				Step:  10 * seconds,
			},
			expected: "fe::up:10000:0:2:-:0:",
		},
		{
			name: "1m downsampling resolution",
			req: &ThanosQueryRangeRequest{
				Query:               "up",
				Start:               0,
				Step:                10 * seconds,
				MaxSourceResolution: 60 * seconds,
			},
			expected: "fe::up:10000:0:2:-:0:",
		},
		{
			name: "5m downsampling resolution, different cache key",
			req: &ThanosQueryRangeRequest{
				Query:               "up",
				Start:               0,
				Step:                10 * seconds,
				MaxSourceResolution: 300 * seconds,
			},
			expected: "fe::up:10000:0:1:-:0:",
		},
		{
			name: "1h downsampling resolution, different cache key",
			req: &ThanosQueryRangeRequest{
				Query:               "up",
				Start:               0,
				Step:                10 * seconds,
				MaxSourceResolution: hour,
			},
			expected: "fe::up:10000:0:0:-:0:",
		},
		{
			name: "1h downsampling resolution with lookback delta",
			req: &ThanosQueryRangeRequest{
				Query:               "up",
				Start:               0,
				Step:                10 * seconds,
				MaxSourceResolution: hour,
				LookbackDelta:       1000,
			},
			expected: "fe::up:10000:0:0:-:1000:",
		},
		{
			name: "label names, no matcher",
			req: &ThanosLabelsRequest{
				Start: 0,
			},
			expected: "fe:::[]:0",
		},
		{
			name: "label names, single matcher",
			req: &ThanosLabelsRequest{
				Start:    0,
				Matchers: [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}},
			},
			expected: `fe:::[[foo="bar"]]:0`,
		},
		{
			name: "label names, multiple matchers",
			req: &ThanosLabelsRequest{
				Start: 0,
				Matchers: [][]*labels.Matcher{
					{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")},
					{labels.MustNewMatcher(labels.MatchEqual, "baz", "qux")},
				},
			},
			expected: `fe:::[[foo="bar"] [baz="qux"]]:0`,
		},
		{
			name: "label values, no matcher",
			req: &ThanosLabelsRequest{
				Start: 0,
				Label: "up",
			},
			expected: "fe::up:[]:0",
		},
		{
			name: "label values, single matcher",
			req: &ThanosLabelsRequest{
				Start:    0,
				Label:    "up",
				Matchers: [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}},
			},
			expected: `fe::up:[[foo="bar"]]:0`,
		},
		{
			name: "label values, multiple matchers",
			req: &ThanosLabelsRequest{
				Start: 0,
				Label: "up",
				Matchers: [][]*labels.Matcher{
					{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")},
					{labels.MustNewMatcher(labels.MatchEqual, "baz", "qux")},
				},
			},
			expected: `fe::up:[[foo="bar"] [baz="qux"]]:0`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := splitter.GenerateCacheKey("", tc.req)
			testutil.Equals(t, tc.expected, key)
		})
	}
}
