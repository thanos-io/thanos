// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestEngineFactory(t *testing.T) {
	var (
		engineRaw = promql.NewEngine(promql.EngineOpts{})
		engine5m  = promql.NewEngine(promql.EngineOpts{LookbackDelta: 5 * time.Minute})
		engine1h  = promql.NewEngine(promql.EngineOpts{LookbackDelta: 1 * time.Hour})
	)
	mockNewEngine := func(opts promql.EngineOpts) *promql.Engine {
		switch opts.LookbackDelta {
		case 1 * time.Hour:
			return engine1h
		case 5 * time.Minute:
			return engine5m
		default:
			return engineRaw
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
					{0, engineRaw},
					{5 * minute, engineRaw},
					{1 * hour, engineRaw},
				},
			},
			{

				lookbackDelta:        3 * time.Minute,
				dynamicLookbackDelta: true,
				tcs: []testCase{
					{2 * minute, engineRaw},
					{3 * minute, engineRaw},
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
					{0, engineRaw},
					{5 * minute, engineRaw},
					{30 * minute, engineRaw},
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
		e := engineFactory(mockNewEngine, promql.EngineOpts{LookbackDelta: td.lookbackDelta}, td.dynamicLookbackDelta)
		for _, tc := range td.tcs {
			got := e(tc.stepMillis)
			testutil.Equals(t, tc.expect, got)
		}
	}
}
