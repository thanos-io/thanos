// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestLookbackDeltaFactory(t *testing.T) {
	type testCase struct {
		stepMillis int64
		expect     time.Duration
	}
	var (
		minute = time.Minute.Milliseconds()
		hour   = time.Hour.Milliseconds()
		tData  = []struct {
			lookbackDelta        time.Duration
			dynamicLookbackDelta bool
			tcs                  []testCase
		}{
			{
				// Non-dynamic lookbackDelta should always return the same engine.
				lookbackDelta:        0,
				dynamicLookbackDelta: false,
				tcs: []testCase{
					{0, time.Duration(0)},
					{5 * minute, time.Duration(0)},
					{1 * hour, time.Duration(0)},
				},
			},
			{

				lookbackDelta:        3 * time.Minute,
				dynamicLookbackDelta: true,
				tcs: []testCase{
					{2 * minute, time.Duration(3) * time.Minute},
					{3 * minute, time.Duration(3) * time.Minute},
					{4 * minute, time.Duration(5) * time.Minute},
					{5 * minute, time.Duration(5) * time.Minute},
					{6 * minute, time.Duration(1) * time.Hour},
					{1 * hour, time.Duration(1) * time.Hour},
					{2 * hour, time.Duration(1) * time.Hour},
				},
			},
			{
				lookbackDelta:        5 * time.Minute,
				dynamicLookbackDelta: true,
				tcs: []testCase{
					{0, time.Duration(5) * time.Minute},
					{5 * minute, time.Duration(5) * time.Minute},
					{6 * minute, time.Duration(1) * time.Hour},
					{59 * minute, time.Duration(1) * time.Hour},
					{1 * hour, time.Duration(1) * time.Hour},
					{2 * hour, time.Duration(1) * time.Hour},
				},
			},
			{
				lookbackDelta:        30 * time.Minute,
				dynamicLookbackDelta: true,
				tcs: []testCase{
					{0, time.Duration(30) * time.Minute},
					{5 * minute, time.Duration(30) * time.Minute},
					{30 * minute, time.Duration(30) * time.Minute},
					{31 * minute, time.Duration(1) * time.Hour},
					{59 * minute, time.Duration(1) * time.Hour},
					{1 * hour, time.Duration(1) * time.Hour},
					{2 * hour, time.Duration(1) * time.Hour},
				},
			},
			{
				lookbackDelta:        1 * time.Hour,
				dynamicLookbackDelta: true,
				tcs: []testCase{
					{0, time.Duration(1) * time.Hour},
					{5 * minute, time.Duration(1) * time.Hour},
					{1 * hour, time.Duration(1) * time.Hour},
					{2 * hour, time.Duration(1) * time.Hour},
				},
			},
		}
	)
	for _, td := range tData {
		lookbackCreate := LookbackDeltaFactory(promql.EngineOpts{LookbackDelta: td.lookbackDelta}, td.dynamicLookbackDelta)
		for _, tc := range td.tcs {
			got := lookbackCreate(tc.stepMillis)
			testutil.Equals(t, tc.expect, got)
		}
	}
}
