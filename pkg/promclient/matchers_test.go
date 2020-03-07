// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package promclient

import (
	"testing"

	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMatchersToString(t *testing.T) {
	cases := []struct {
		ms       []storepb.LabelMatcher
		expected string
	}{
		{
			ms: []storepb.LabelMatcher{
				{
					Name:  "__name__",
					Type:  storepb.LabelMatcher_EQ,
					Value: "up",
				}},
			expected: `{__name__="up"}`,
		},
		{
			ms: []storepb.LabelMatcher{
				{
					Name:  "__name__",
					Type:  storepb.LabelMatcher_NEQ,
					Value: "up",
				},
				{
					Name:  "job",
					Type:  storepb.LabelMatcher_EQ,
					Value: "test",
				},
			},
			expected: `{__name__!="up", job="test"}`,
		},
		{
			ms: []storepb.LabelMatcher{
				{
					Name:  "__name__",
					Type:  storepb.LabelMatcher_EQ,
					Value: "up",
				},
				{
					Name:  "job",
					Type:  storepb.LabelMatcher_RE,
					Value: "test",
				},
			},
			expected: `{__name__="up", job=~"test"}`,
		},
		{
			ms: []storepb.LabelMatcher{
				{
					Name:  "job",
					Type:  storepb.LabelMatcher_NRE,
					Value: "test",
				}},
			expected: `{job!~"test"}`,
		},
		{
			ms: []storepb.LabelMatcher{
				{
					Name:  "__name__",
					Type:  storepb.LabelMatcher_EQ,
					Value: "up",
				},
				{
					Name:  "__name__",
					Type:  storepb.LabelMatcher_NEQ,
					Value: "up",
				},
			},
			// We cannot use up{__name__!="up"} in this case.
			expected: `{__name__="up", __name__!="up"}`,
		},
	}

	for i, c := range cases {
		actual, err := MatchersToString(c.ms)
		testutil.Ok(t, err)
		testutil.Assert(t, actual == c.expected, "test case %d failed, expected %s, actual %s", i, c.expected, actual)
	}
}
