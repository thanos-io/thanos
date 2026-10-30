// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
)

func TestConfigureGoAutoMemLimitValidation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		config  goMemLimitConfig
		wantErr bool
	}{
		{
			name:    "disabled with defaults",
			config:  goMemLimitConfig{enableAutoGoMemlimit: false, memlimitRatio: 0.9},
			wantErr: false,
		},
		{
			name:    "ratio zero",
			config:  goMemLimitConfig{memlimitRatio: 0.0},
			wantErr: true,
		},
		{
			name:    "ratio negative",
			config:  goMemLimitConfig{memlimitRatio: -0.5},
			wantErr: true,
		},
		{
			name:    "ratio greater than one",
			config:  goMemLimitConfig{memlimitRatio: 1.1},
			wantErr: true,
		},
		{
			name:    "refresh interval negative",
			config:  goMemLimitConfig{memlimitRatio: 0.9, memlimitRefresh: -time.Second},
			wantErr: true,
		},
		{
			name:    "disabled with refresh interval set",
			config:  goMemLimitConfig{enableAutoGoMemlimit: false, memlimitRatio: 0.9, memlimitRefresh: time.Minute},
			wantErr: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := configureGoAutoMemLimit(tc.config)
			if tc.wantErr {
				testutil.NotOk(t, err)
			} else {
				testutil.Ok(t, err)
			}
		})
	}
}
