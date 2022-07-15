// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package util

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

func TestLabelMatchersToString(t *testing.T) {
	tests := []struct {
		input    []*labels.Matcher
		expected string
	}{
		{
			input:    nil,
			expected: "{}",
		}, {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
			},
			expected: `{foo="bar"}`,
		}, {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchNotEqual, "who", "boh"),
			},
			expected: `{foo="bar",who!="boh"}`,
		}, {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "metric"),
				labels.MustNewMatcher(labels.MatchNotEqual, "who", "boh"),
			},
			expected: `{__name__="metric",who!="boh"}`,
		},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, LabelMatchersToString(tc.input))
	}
}
