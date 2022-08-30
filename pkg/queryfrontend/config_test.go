// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"fmt"
	"github.com/thanos-io/thanos/internal/cortex/chunk/cache"
	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestConfig_Validate(t *testing.T) {

	type testCase struct {
		name   string
		config Config
		err    string
	}

	testCases := []testCase{
		{
			name: "invalid query range options",
			config: Config{
				QueryRangeConfig: QueryRangeConfig{
					SplitQueriesByInterval:      10 * time.Hour,
					MinHorizontalShards:         10,
					QuerySplitThresholdInterval: 1 * time.Hour,
				},
			},
			err: "split queries interval and dynamic query split interval cannot be set at the same time",
		},
		{
			name: "invalid parameters for dynamic query range split",
			config: Config{
				QueryRangeConfig: QueryRangeConfig{
					SplitQueriesByInterval:      0,
					MinHorizontalShards:         0,
					QuerySplitThresholdInterval: 1 * time.Hour,
				},
			},
			err: "min horizontal shards should be greater than 0 when query split threshold is enabled",
		},
		{
			name: "invalid parameters for dynamic query range split - 2",
			config: Config{
				QueryRangeConfig: QueryRangeConfig{
					SplitQueriesByInterval:      0,
					MinHorizontalShards:         10,
					MaxQuerySplitInterval:       0,
					QuerySplitThresholdInterval: 1 * time.Hour,
				},
			},
			err: "max query split interval should be greater than 0 when query split threshold is enabled",
		},
		{
			name: "valid config with caching",
			config: Config{
				DownstreamURL: "localhost:8080",
				QueryRangeConfig: QueryRangeConfig{
					SplitQueriesByInterval:      10 * time.Hour,
					MinHorizontalShards:         0,
					MaxQuerySplitInterval:       0,
					QuerySplitThresholdInterval: 0,
					ResultsCacheConfig: &queryrange.ResultsCacheConfig{
						CacheConfig:                cache.Config{},
						Compression:                "",
						CacheQueryableSamplesStats: false,
					},
				},
				LabelsConfig: LabelsConfig{
					DefaultTimeRange: day,
				},
			},
			err: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.Validate()
			if tc.err != "" {
				testutil.NotOk(t, err)
				testutil.Equals(t, tc.err, err.Error())
			} else {
				testutil.Ok(t, err)
				fmt.Println(err)
			}
		})
	}
}
