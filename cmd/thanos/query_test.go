// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
)

func TestHasFeature(t *testing.T) {
	for _, tc := range []struct {
		name     string
		features []string
		target   string
		want     bool
	}{
		{name: "empty", target: promqlExperimentalFunctions, want: false},
		{name: "single exact match", features: []string{promqlExperimentalFunctions}, target: promqlExperimentalFunctions, want: true},
		{name: "comma separated with spaces", features: []string{" promql-at-modifier , promql-experimental-functions "}, target: promqlExperimentalFunctions, want: true},
		{name: "repeated flags", features: []string{promqlAtModifier, "type-and-unit-labels"}, target: "type-and-unit-labels", want: true},
		{name: "partial prefix does not match", features: []string{"type-and-unit-labels-v2"}, target: "type-and-unit-labels", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testutil.Equals(t, tc.want, hasFeature(tc.features, tc.target))
		})
	}
}

func TestNewPromQLParserExperimentalFunctions(t *testing.T) {
	for _, tc := range []struct {
		name             string
		features         []string
		enableXFunctions bool
		expr             string
		wantErr          bool
	}{
		{name: "disabled", expr: "double_exponential_smoothing(up[1h], 0.5, 0.5)", wantErr: true},
		{name: "unrelated feature", features: []string{promqlAtModifier}, expr: "double_exponential_smoothing(up[1h], 0.5, 0.5)", wantErr: true},
		{name: "enabled", features: []string{promqlExperimentalFunctions}, expr: "double_exponential_smoothing(up[1h], 0.5, 0.5)", wantErr: false},
		{name: "comma separated enabled", features: []string{promqlAtModifier + "," + promqlExperimentalFunctions}, expr: "double_exponential_smoothing(up[1h], 0.5, 0.5)", wantErr: false},
		{name: "xfunctions disabled", expr: "xrate(up[5m])", wantErr: true},
		{name: "xfunctions enabled", enableXFunctions: true, expr: "xrate(up[5m])", wantErr: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newPromQLParser(tc.features, tc.enableXFunctions).ParseExpr(tc.expr)
			testutil.Equals(t, tc.wantErr, err != nil)
		})
	}
}

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
		lookbackCreate := LookbackDeltaFactory(td.lookbackDelta, td.dynamicLookbackDelta)
		for _, tc := range td.tcs {
			got := lookbackCreate(tc.stepMillis)
			testutil.Equals(t, tc.expect, got)
		}
	}
}
