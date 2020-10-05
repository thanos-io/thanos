// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestEngineFunc(t *testing.T) {
	var (
		engineDefault = promql.NewEngine(promql.EngineOpts{LookbackDelta: 123})
		engine5m      = promql.NewEngine(promql.EngineOpts{LookbackDelta: 5 * time.Minute})
		engine1h      = promql.NewEngine(promql.EngineOpts{LookbackDelta: 1 * time.Hour})
	)
	fakeNewEngine := func(lookback time.Duration) *promql.Engine {
		switch lookback {
		case 1 * time.Hour:
			return engine1h
		case 5 * time.Minute:
			return engine5m
		default:
			return engineDefault
		}
	}
	type testCase struct {
		stepMillis int64
		expect     *promql.Engine
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
					{0, engineDefault},
					{5 * minute, engineDefault},
					{1 * hour, engineDefault},
				},
			},
			{
				lookbackDelta:        3 * time.Minute,
				dynamicLookbackDelta: true,
				tcs: []testCase{
					{2 * minute, engineDefault},
					{3 * minute, engineDefault},
					{4 * minute, engine5m},
					{5 * minute, engine5m},
					{6 * minute, engine1h},
					{1 * hour, engine1h},
					{2 * hour, engine1h},
				},
			},
			{
				lookbackDelta:        5 * time.Minute,
				dynamicLookbackDelta: true,
				tcs: []testCase{
					{0, engine5m},
					{5 * minute, engine5m},
					{6 * minute, engine1h},
					{59 * minute, engine1h},
					{1 * hour, engine1h},
					{2 * hour, engine1h},
				},
			},
			{
				lookbackDelta:        30 * time.Minute,
				dynamicLookbackDelta: true,
				tcs: []testCase{
					{0, engineDefault},
					{5 * minute, engineDefault},
					{30 * minute, engineDefault},
					{31 * minute, engine1h},
					{59 * minute, engine1h},
					{1 * hour, engine1h},
					{2 * hour, engine1h},
				},
			},
			{
				lookbackDelta:        1 * time.Hour,
				dynamicLookbackDelta: true,
				tcs: []testCase{
					{0, engine1h},
					{5 * minute, engine1h},
					{1 * hour, engine1h},
					{2 * hour, engine1h},
				},
			},
		}
	)
	for _, td := range tData {
		e := engineFunc(fakeNewEngine, td.lookbackDelta, td.dynamicLookbackDelta)
		for _, tc := range td.tcs {
			got := e(tc.stepMillis)
			testutil.Equals(t, tc.expect, got)
		}
	}
}
