// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestGenerateCacheKey(t *testing.T) {
	splitter := newThanosCacheKeyGenerator(hour)

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
			expected: "up:60000:0",
		},
		{
			name: "non downsampling resolution specified",
			req: &ThanosQueryRangeRequest{
				Query: "up",
				Start: 0,
				Step:  60 * seconds,
			},
			expected: "up:60000:0:2",
		},
		{
			name: "10s step",
			req: &ThanosQueryRangeRequest{
				Query: "up",
				Start: 0,
				Step:  10 * seconds,
			},
			expected: "up:10000:0:2",
		},
		{
			name: "1m downsampling resolution",
			req: &ThanosQueryRangeRequest{
				Query:               "up",
				Start:               0,
				Step:                10 * seconds,
				MaxSourceResolution: 60 * seconds,
			},
			expected: "up:10000:0:2",
		},
		{
			name: "5m downsampling resolution, different cache key",
			req: &ThanosQueryRangeRequest{
				Query:               "up",
				Start:               0,
				Step:                10 * seconds,
				MaxSourceResolution: 300 * seconds,
			},
			expected: "up:10000:0:1",
		},
		{
			name: "1h downsampling resolution, different cache key",
			req: &ThanosQueryRangeRequest{
				Query:               "up",
				Start:               0,
				Step:                10 * seconds,
				MaxSourceResolution: hour,
			},
			expected: "up:10000:0:0",
		},
	} {
		key := splitter.GenerateCacheKey("", tc.req)
		testutil.Equals(t, tc.expected, key)
	}
}
