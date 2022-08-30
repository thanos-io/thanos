// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"github.com/thanos-io/thanos/pkg/testutil"
	"testing"
	"time"
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
					SplitQueriesByInterval: 10 * time.Hour,
					MaxHorizontalShards:    10,
					MinQuerySplitInterval:  1 * time.Hour,
				},
			},
			err: "split queries interval and dynamic query split interval cannot be set at the same time",
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
			}
		})
	}

}
