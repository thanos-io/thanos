// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package strutil

import (
	"fmt"
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestMergeSlices(t *testing.T) {
	testCases := map[string]struct {
		slices   [][]string
		limit    int
		expected []string
	}{
		"empty slice": {
			slices: [][]string{
				{},
			},
			expected: []string{},
		},
		"single slice with limit": {
			slices: [][]string{
				{"a", "b", "c", "d"},
			},
			limit:    2,
			expected: []string{"a", "b"},
		},
		"multiple slices with limit": {
			slices: [][]string{
				{"a", "b", "d", "f"},
				{"c", "e", "g"},
				{"r", "s", "t"},
			},
			limit:    4,
			expected: []string{"a", "b", "c", "d"},
		},
		"multiple slices without limit": {
			slices: [][]string{
				{"a", "b", "d", "f"},
				{"c", "e", "g"},
				{"r", "s"},
			},
			expected: []string{"a", "b", "c", "d", "e", "f", "g", "r", "s"},
		},
	}

	for tcName, tc := range testCases {
		t.Run(tcName, func(t *testing.T) {
			res := MergeSlices(tc.limit, tc.slices...)
			testutil.Equals(t, tc.expected, res)
		})
	}
}

func TestMergeUnsortedSlices(t *testing.T) {
	testCases := map[string]struct {
		slices   [][]string
		limit    int
		expected []string
	}{
		"empty slice": {
			slices: [][]string{
				{},
			},
			expected: []string{},
		},
		"multiple slices without limit": {
			slices: [][]string{
				{"d", "c", "b", "a"},
				{"f", "g", "c"},
				{"s", "r", "e"},
			},
			expected: []string{"a", "b", "c", "d", "e", "f", "g", "r", "s"},
		},
		"multiple slices with limit": {
			slices: [][]string{
				{"d", "c", "b", "a"},
				{"f", "g", "c"},
				{"s", "r", "e"},
			},
			limit:    5,
			expected: []string{"a", "b", "c", "d", "e"},
		},
	}

	for tcName, tc := range testCases {
		t.Run(tcName, func(t *testing.T) {
			res := MergeUnsortedSlices(tc.limit, tc.slices...)
			testutil.Equals(t, tc.expected, res)
		})
	}
}

func BenchmarkMergeSlices(b *testing.B) {
	var slices [][]string
	for i := 0; i < 10; i++ {
		var slice []string
		for j := 0; j < 10000; j++ {
			slice = append(slice, fmt.Sprintf("str_%d_%d", i, j))
		}
		slices = append(slices, slice)
	}

	b.Run("benchmark", func(b *testing.B) {
		MergeSlices(1000, slices...)
	})
}
