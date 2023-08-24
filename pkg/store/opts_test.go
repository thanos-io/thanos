// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

// Refer to https://github.com/prometheus/prometheus/issues/2651.
func TestFindSetMatches(t *testing.T) {
	cases := []struct {
		pattern string
		exp     []string
	}{
		// Simple sets.
		{
			pattern: "foo|bar|baz",
			exp: []string{
				"foo",
				"bar",
				"baz",
			},
		},
		// Simple sets with group wrapper.
		{
			pattern: "(foo|bar|baz)",
			exp: []string{
				"foo",
				"bar",
				"baz",
			},
		},
		// Simple sets containing escaped characters.
		{
			pattern: "fo\\.o|bar\\?|\\^baz",
			exp: []string{
				"fo.o",
				"bar?",
				"^baz",
			},
		},
		// Simple sets containing special characters without escaping.
		{
			pattern: "fo.o|bar?|^baz",
			exp:     nil,
		},
		{
			pattern: "(fool|bar)|(baz)",
			exp:     nil,
		},
		{
			pattern: "foo\\|bar\\|baz",
			exp: []string{
				"foo|bar|baz",
			},
		},
		// empty pattern
		{
			pattern: "",
			exp:     nil,
		},
	}

	for _, c := range cases {
		matches := findSetMatches(c.pattern)
		testutil.Equals(t, c.exp, matches)
	}
}
