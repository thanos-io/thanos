// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package strutil

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestParseFlagLabels(t *testing.T) {
	testCases := map[string]struct {
		flags    []string
		expected []string
	}{
		"single flag with commas": {
			flags: []string{
				"a,b,c",
			},
			expected: []string{"a", "b", "c"},
		},
		"multiple flags with commas": {
			flags: []string{
				"a", "b", "c,d",
			},
			expected: []string{"a", "b", "c", "d"},
		},
		"multiple flags empty strings": {
			flags: []string{
				"a", "b", "",
			},
			expected: []string{"a", "b"},
		},
	}

	for tcName, tc := range testCases {
		t.Run(tcName, func(t *testing.T) {
			res := ParseFlagLabels(tc.flags)
			testutil.Equals(t, tc.expected, res)
		})
	}
}
