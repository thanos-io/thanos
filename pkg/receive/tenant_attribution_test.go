// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestNewFilterFromFilterValueInvalidPattern(t *testing.T) {
	inputs := []string{"ab]c[sdf", "abc[z-a]", "*con[tT]ains*"}
	for _, input := range inputs {
		_, err := NewFilterFromFilterValue(FilterValue{Pattern: input})
		require.Error(t, err)
	}
}

func TestNewFilterFromFilterValueWithNegation(t *testing.T) {
	inputs := []struct {
		pattern string
		negate  bool
		data    []mockFilterData
	}{
		{
			pattern: "foo",
			negate:  false,
			data: []mockFilterData{
				{val: "foo", match: true},
				{val: "boo", match: false},
			},
		},
		{
			pattern: "foo*bar",
			negate:  false,
			data: []mockFilterData{
				{val: "foobar", match: true},
				{val: "foozapbar", match: true},
				{val: "bazbar", match: false},
			},
		},
		{
			pattern: "ba?[0-9][!a-z]9",
			negate:  false,
			data: []mockFilterData{
				{val: "bar959", match: true},
				{val: "bat449", match: true},
				{val: "bar9", match: false},
			},
		},
		{
			pattern: "{ba,fx,car}*",
			negate:  false,
			data: []mockFilterData{
				{val: "ba", match: true},
				{val: "fxy", match: true},
				{val: "car", match: true},
				{val: "ca", match: false},
			},
		},
		{
			pattern: "foo",
			negate:  true,
			data: []mockFilterData{
				{val: "foo", match: false},
				{val: "bar", match: true},
			},
		},
		{
			pattern: "foo*bar",
			negate:  true,
			data: []mockFilterData{
				{val: "foobar", match: false},
				{val: "foozapbar", match: false},
				{val: "bazbar", match: true},
			},
		},
		{
			pattern: "ba?[0-9][!a-z]9",
			negate:  true,
			data: []mockFilterData{
				{val: "bar959", match: false},
				{val: "bat449", match: false},
				{val: "bar9", match: true},
			},
		},
		{
			pattern: "{ba,fx,car}*",
			negate:  true,
			data: []mockFilterData{
				{val: "ba", match: false},
				{val: "fxy", match: false},
				{val: "car", match: false},
				{val: "ca", match: true},
			},
		},
	}

	for _, input := range inputs {
		f, err := NewFilterFromFilterValue(FilterValue{Pattern: input.pattern, Negate: input.negate})
		require.NoError(t, err)
		for _, testcase := range input.data {
			require.Equal(t, testcase.match, f.Matches([]byte(testcase.val)))
		}
	}
}

func TestFilters(t *testing.T) {
	filters := genAndValidateFilters(t, []testPattern{
		{pattern: "f[A-z]?*", expectedStr: "StartsWith(Equals(\"f\") then Range(\"A-z\") then AnyChar)"},
		{pattern: "*ba[a-z]", expectedStr: "EndsWith(Equals(\"ba\") then Range(\"a-z\"))"},
		{pattern: "wa*?ba[!0-9][0-9]{8,9}", expectedStr: "StartsWith(Equals(\"wa\")) && EndsWith(AnyChar then Equals(\"ba\") then Not(Range(\"0-9\")) then Range(\"0-9\") then Range(\"8,9\"))"},
	})

	inputs := []testInput{
		newTestInput("foo", true, false, false),
		newTestInput("test", false, false, false),
		newTestInput("bar", false, true, false),
		newTestInput("foobar", true, true, false),
		newTestInput("waxbar08", false, false, true),
		newTestInput("waxybar09", false, false, true),
	}

	for _, input := range inputs {
		for i, expectedMatch := range input.matches {
			require.Equal(t, expectedMatch, filters[i].Matches(input.val),
				fmt.Sprintf("input: %s, pattern: %s", input.val, filters[i].String()))
		}
	}
}

func TestEqualityFilter(t *testing.T) {
	inputs := []mockFilterData{
		{val: "foo", match: true},
		{val: "fx", match: false},
		{val: "foob", match: false},
	}
	// Use NewFilter with a pattern that has no wildcards - creates an equalityFilter internally
	f, err := NewFilter([]byte("foo"))
	require.NoError(t, err)
	for _, input := range inputs {
		require.Equal(t, input.match, f.Matches([]byte(input.val)))
	}
}

func TestEmptyFilter(t *testing.T) {
	f, err := NewFilter(nil)
	require.NoError(t, err)
	require.True(t, f.Matches([]byte("")))
	require.False(t, f.Matches([]byte(" ")))
	require.False(t, f.Matches([]byte("foo")))
}

func TestWildcardFilters(t *testing.T) {
	filters := genAndValidateFilters(t, []testPattern{
		{pattern: "foo", expectedStr: "Equals(\"foo\")"},
		{pattern: "*bar", expectedStr: "EndsWith(Equals(\"bar\"))"},
		{pattern: "baz*", expectedStr: "StartsWith(Equals(\"baz\"))"},
		{pattern: "*cat*", expectedStr: "Contains(\"cat\")"},
		{pattern: "foo*bar", expectedStr: "StartsWith(Equals(\"foo\")) && EndsWith(Equals(\"bar\"))"},
		{pattern: "*", expectedStr: "All"},
	})

	inputs := []testInput{
		newTestInput("foo", true, false, false, false, false, true),
		newTestInput("foobar", false, true, false, false, true, true),
		newTestInput("foozapbar", false, true, false, false, true, true),
		newTestInput("bazbar", false, true, true, false, false, true),
		newTestInput("bazzzbar", false, true, true, false, false, true),
		newTestInput("cat", false, false, false, true, false, true),
		newTestInput("catbar", false, true, false, true, false, true),
		newTestInput("baztestcat", false, false, true, true, false, true),
		newTestInput("foocatbar", false, true, false, true, true, true),
		newTestInput("footestcatbar", false, true, false, true, true, true),
	}

	for _, input := range inputs {
		for i, expectedMatch := range input.matches {
			require.Equal(t, expectedMatch, filters[i].Matches(input.val),
				fmt.Sprintf("input: %s, pattern: %s", input.val, filters[i].String()))
		}
	}
}

func TestRangeFilters(t *testing.T) {
	filters := genAndValidateFilters(t, []testPattern{
		{pattern: "ax[a-zA-Z0-9]", expectedStr: "Equals(\"ax\") then Range(\"a-z || A-Z || 0-9\")"},
		{pattern: "f[o-o]?", expectedStr: "Equals(\"f\") then Range(\"o-o\") then AnyChar"},
		{pattern: "???", expectedStr: "AnyChar then AnyChar then AnyChar"},
		{pattern: "ba?", expectedStr: "Equals(\"ba\") then AnyChar"},
		{pattern: "[!cC]ar", expectedStr: "Not(Range(\"cC\")) then Equals(\"ar\")"},
		{pattern: "ba?[0-9][!a-z]9", expectedStr: "Equals(\"ba\") then AnyChar then Range(\"0-9\") then Not(Range(\"a-z\")) then Equals(\"9\")"},
		{pattern: "{ba,fx,car}*", expectedStr: "StartsWith(Range(\"ba,fx,car\"))"},
		{pattern: "ba{r,t}*[!a-zA-Z]", expectedStr: "StartsWith(Equals(\"ba\") then Range(\"r,t\")) && EndsWith(Not(Range(\"a-z || A-Z\")))"},
		{pattern: "*{9}", expectedStr: "EndsWith(Range(\"9\"))"},
	})

	inputs := []testInput{
		newTestInput("axo", true, false, true, false, false, false, false, false, false),
		newTestInput("ax!", false, false, true, false, false, false, false, false, false),
		newTestInput("foo", false, true, true, false, false, false, false, false, false),
		newTestInput("boo", false, false, true, false, false, false, false, false, false),
		newTestInput("bar", false, false, true, true, true, false, true, false, false),
		newTestInput("Bar", false, false, true, false, true, false, false, false, false),
		newTestInput("car", false, false, true, false, false, false, true, false, false),
		newTestInput("fxy", false, false, true, false, false, false, true, false, false),
		newTestInput("bar9", false, false, false, false, false, false, true, true, true),
		newTestInput("bar990", false, false, false, false, false, false, true, true, false),
		newTestInput("bar959", false, false, false, false, false, true, true, true, true),
		newTestInput("bar009", false, false, false, false, false, true, true, true, true),
		newTestInput("bat449", false, false, false, false, false, true, true, true, true),
	}

	for _, input := range inputs {
		for i, expectedMatch := range input.matches {
			require.Equal(t, expectedMatch, filters[i].Matches(input.val),
				fmt.Sprintf("input: %s, pattern: %s", input.val, filters[i].String()))
		}
	}
}

func TestNegationFilter(t *testing.T) {
	filters := genAndValidateFilters(t, []testPattern{
		{pattern: "!foo", expectedStr: "Not(Equals(\"foo\"))"},
		{pattern: "!*bar", expectedStr: "Not(EndsWith(Equals(\"bar\")))"},
		{pattern: "!baz*", expectedStr: "Not(StartsWith(Equals(\"baz\")))"},
		{pattern: "!*cat*", expectedStr: "Not(Contains(\"cat\"))"},
		{pattern: "!foo*bar", expectedStr: "Not(StartsWith(Equals(\"foo\")) && EndsWith(Equals(\"bar\")))"},
		{pattern: "foo!", expectedStr: "Equals(\"foo!\")"},
	})

	inputs := []testInput{
		newTestInput("foo", false, true, true, true, true, false),
		newTestInput("foo!", true, true, true, true, true, true),
		newTestInput("foobar", true, false, true, true, false, false),
		newTestInput("bazbar", true, false, false, true, true, false),
		newTestInput("cat", true, true, true, false, true, false),
		newTestInput("catbar", true, false, true, false, true, false),
		newTestInput("baztestcat", true, true, false, false, true, false),
		newTestInput("foocatbar", true, false, true, false, false, false),
		newTestInput("footestcatbar", true, false, true, false, false, false),
	}

	for _, input := range inputs {
		for i, expectedMatch := range input.matches {
			require.Equal(t, expectedMatch, filters[i].Matches(input.val),
				fmt.Sprintf("input: %s, pattern: %s", input.val, filters[i].String()))
		}
	}
}

func TestBadPatterns(t *testing.T) {
	patterns := []string{
		"!", // negation of nothing is everything, so user should use *.
		"**",
		"***",
		"*too*many*",
		"*too**many",
		"to*o*many",
		"to*o*ma*ny",
		"abc[sdf",
		"ab]c[sdf",
		"abc[z-a]",
		"*con[tT]ains*",
		"*con{tT}ains*",
		"*con?ains*",
		"abc[a-zA-Z0-]",
		"abc[a-zA-Z0]",
		"abc[a-zZ-A]",
		"ab}c{sdf",
		"ab{}sdf",
		"ab[]sdf",
	}

	for _, pattern := range patterns {
		_, err := NewFilter([]byte(pattern))
		require.Error(t, err, fmt.Sprintf("pattern: %s", pattern))
	}
}

func TestParseTagFilterValueMap(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		expected    TagFilterValueMap
	}{
		{
			name:        "simple key:value",
			input:       "__name__:foo",
			expectError: false,
			expected: TagFilterValueMap{
				{Name: "__name__", Exclude: false}: {Pattern: "foo", Negate: false},
			},
		},
		{
			name:        "wildcard pattern",
			input:       "__name__:foo*",
			expectError: false,
			expected: TagFilterValueMap{
				{Name: "__name__", Exclude: false}: {Pattern: "foo*", Negate: false},
			},
		},
		{
			name:        "multiple filters",
			input:       "__name__:foo* job:bar",
			expectError: false,
			expected: TagFilterValueMap{
				{Name: "__name__", Exclude: false}: {Pattern: "foo*", Negate: false},
				{Name: "job", Exclude: false}:      {Pattern: "bar", Negate: false},
			},
		},
		{
			name:        "exclude tag",
			input:       "!internal:*",
			expectError: false,
			expected: TagFilterValueMap{
				{Name: "internal", Exclude: true}: {Pattern: "*", Negate: false},
			},
		},
		{
			name:        "empty string",
			input:       "",
			expectError: false,
			expected:    TagFilterValueMap{},
		},
		{
			name:        "empty tag name",
			input:       ":foo",
			expectError: true,
		},
		{
			name:        "empty pattern",
			input:       "__name__:",
			expectError: true,
		},
		{
			name:        "duplicate tag",
			input:       "__name__:foo __name__:bar",
			expectError: true,
		},
		{
			name:        "exclude with non-wildcard pattern",
			input:       "!internal:specific",
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ParseTagFilterValueMap(tc.input)
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestNewTagsFilter(t *testing.T) {
	tests := []struct {
		name        string
		filters     TagFilterValueMap
		op          LogicalOp
		expectError bool
	}{
		{
			name: "simple conjunction filter",
			filters: TagFilterValueMap{
				{Name: "__name__", Exclude: false}: {Pattern: "foo*", Negate: false},
			},
			op:          Conjunction,
			expectError: false,
		},
		{
			name: "simple disjunction filter",
			filters: TagFilterValueMap{
				{Name: "__name__", Exclude: false}: {Pattern: "foo*", Negate: false},
			},
			op:          Disjunction,
			expectError: false,
		},
		{
			name: "exclude with conjunction",
			filters: TagFilterValueMap{
				{Name: "internal", Exclude: true}: {Pattern: "*", Negate: false},
			},
			op:          Conjunction,
			expectError: false,
		},
		{
			name: "exclude with disjunction - error",
			filters: TagFilterValueMap{
				{Name: "internal", Exclude: true}: {Pattern: "*", Negate: false},
			},
			op:          Disjunction,
			expectError: true,
		},
		{
			name:        "empty filters",
			filters:     TagFilterValueMap{},
			op:          Conjunction,
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewTagsFilter(tc.filters, tc.op, TagsFilterOptions{})
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestTagsFilterMatchLabels(t *testing.T) {
	tests := []struct {
		name          string
		filterStr     string
		op            LogicalOp
		labels        map[string]string
		expectedMatch bool
	}{
		{
			name:          "exact match",
			filterStr:     "__name__:foo",
			op:            Conjunction,
			labels:        map[string]string{"__name__": "foo"},
			expectedMatch: true,
		},
		{
			name:          "exact match - no match",
			filterStr:     "__name__:foo",
			op:            Conjunction,
			labels:        map[string]string{"__name__": "bar"},
			expectedMatch: false,
		},
		{
			name:          "wildcard prefix match",
			filterStr:     "__name__:foo*",
			op:            Conjunction,
			labels:        map[string]string{"__name__": "foobar"},
			expectedMatch: true,
		},
		{
			name:          "wildcard suffix match",
			filterStr:     "__name__:*bar",
			op:            Conjunction,
			labels:        map[string]string{"__name__": "foobar"},
			expectedMatch: true,
		},
		{
			name:          "multiple filters - conjunction all match",
			filterStr:     "__name__:foo* job:test",
			op:            Conjunction,
			labels:        map[string]string{"__name__": "foobar", "job": "test"},
			expectedMatch: true,
		},
		{
			name:          "multiple filters - conjunction partial match",
			filterStr:     "__name__:foo* job:test",
			op:            Conjunction,
			labels:        map[string]string{"__name__": "foobar", "job": "other"},
			expectedMatch: false,
		},
		{
			name:          "multiple filters - disjunction partial match",
			filterStr:     "__name__:foo* job:test",
			op:            Disjunction,
			labels:        map[string]string{"__name__": "foobar", "job": "other"},
			expectedMatch: true,
		},
		{
			name:          "multiple filters - disjunction no match",
			filterStr:     "__name__:foo* job:test",
			op:            Disjunction,
			labels:        map[string]string{"__name__": "bar", "job": "other"},
			expectedMatch: false,
		},
		{
			name:          "exclude tag - tag absent",
			filterStr:     "!internal:*",
			op:            Conjunction,
			labels:        map[string]string{"__name__": "foo"},
			expectedMatch: true,
		},
		{
			name:          "exclude tag - tag present",
			filterStr:     "!internal:*",
			op:            Conjunction,
			labels:        map[string]string{"__name__": "foo", "internal": "true"},
			expectedMatch: false,
		},
		{
			name:          "missing required label",
			filterStr:     "__name__:foo",
			op:            Conjunction,
			labels:        map[string]string{"job": "test"},
			expectedMatch: false,
		},
		{
			name:          "empty filter matches all",
			filterStr:     "",
			op:            Conjunction,
			labels:        map[string]string{"__name__": "foo"},
			expectedMatch: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filterMap, err := ParseTagFilterValueMap(tc.filterStr)
			require.NoError(t, err)

			filter, err := NewTagsFilter(filterMap, tc.op, TagsFilterOptions{})
			require.NoError(t, err)

			lbls := labels.FromMap(tc.labels)
			require.Equal(t, tc.expectedMatch, filter.MatchLabels(lbls),
				fmt.Sprintf("filter: %s, labels: %v", tc.filterStr, tc.labels))
		})
	}
}

type mockFilterData struct {
	val   string
	match bool
}

type testPattern struct {
	pattern     string
	expectedStr string
}

type testInput struct {
	val     []byte
	matches []bool
}

func newTestInput(val string, matches ...bool) testInput {
	return testInput{val: []byte(val), matches: matches}
}

func genAndValidateFilters(t *testing.T, patterns []testPattern) []Filter {
	var err error
	filters := make([]Filter, len(patterns))
	for i, pattern := range patterns {
		filters[i], err = NewFilter([]byte(pattern.pattern))
		require.NoError(t, err, fmt.Sprintf("No error expected, but got: %v for pattern: %s", err, pattern.pattern))
		require.Equal(t, pattern.expectedStr, filters[i].String())
	}

	return filters
}
