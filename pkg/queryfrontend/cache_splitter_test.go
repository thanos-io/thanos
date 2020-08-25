// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestGenerateCacheKey(t *testing.T) {
	splitter := newThanosCacheSplitter(hour)

	for _, tc := range []struct {
		name             string
		req              queryrange.Request
		expectedCacheKey string
	}{
		{
			name: "non thanos req",
			req: &queryrange.PrometheusRequest{
				Query: "up",
				Start: 0,
				Step:  60 * seconds,
			},
			expectedCacheKey: "up:60000:0",
		},
		{
			name: "non downsampling resolution specified",
			req: &ThanosRequest{
				Query: "up",
				Start: 0,
				Step:  60 * seconds,
			},
			expectedCacheKey: "up:60000:0:2",
		},
		{
			name: "10s step",
			req: &ThanosRequest{
				Query: "up",
				Start: 0,
				Step:  10 * seconds,
			},
			expectedCacheKey: "up:10000:0:2",
		},
		{
			name: "1m downsampling resolution",
			req: &ThanosRequest{
				Query:               "up",
				Start:               0,
				Step:                10 * seconds,
				MaxSourceResolution: 60 * seconds,
			},
			expectedCacheKey: "up:10000:0:2",
		},
		{
			name: "5m downsampling resolution, different cache key",
			req: &ThanosRequest{
				Query:               "up",
				Start:               0,
				Step:                10 * seconds,
				MaxSourceResolution: 300 * seconds,
			},
			expectedCacheKey: "up:10000:0:1",
		},
		{
			name: "1h downsampling resolution, different cache key",
			req: &ThanosRequest{
				Query:               "up",
				Start:               0,
				Step:                10 * seconds,
				MaxSourceResolution: hour,
			},
			expectedCacheKey: "up:10000:0:0",
		},
	} {
		key := splitter.GenerateCacheKey("", tc.req)
		testutil.Equals(t, tc.expectedCacheKey, key)
	}
}
