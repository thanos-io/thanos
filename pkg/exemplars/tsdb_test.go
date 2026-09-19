// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exemplars

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/efficientgo/core/testutil"
)

func TestSelectorsMatchExternalLabels(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		selectors         [][]*labels.Matcher
		extLabels         labels.Labels
		shouldMatch       bool
		expectedSelectors [][]*labels.Matcher
	}{
		"should return true for matching labels": {
			selectors: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "code", "200"),
					labels.MustNewMatcher(labels.MatchEqual, "receive", "true"),
				},
			},
			extLabels:   labels.FromStrings("receive", "true"),
			shouldMatch: true,
			expectedSelectors: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "code", "200"),
				},
			},
		},
		"should return true when the external labels are not present in input at all": {
			selectors: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "code", "200"),
				},
			},
			extLabels:   labels.FromStrings("receive", "true"),
			shouldMatch: true,
			expectedSelectors: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "code", "200"),
				},
			},
		},
		"should return true when only some of matchers slice are matching": {
			selectors: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "code", "200"),
					labels.MustNewMatcher(labels.MatchRegexp, "receive", "false"),
				},
				{
					labels.MustNewMatcher(labels.MatchEqual, "code", "500"),
					labels.MustNewMatcher(labels.MatchRegexp, "receive", "true"),
				},
			},
			extLabels:   labels.FromStrings("receive", "true", "replica", "0"),
			shouldMatch: true,
			expectedSelectors: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "code", "500"),
				},
			},
		},
		"should return false when the external labels are not matching": {
			selectors: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "code", "200"),
					labels.MustNewMatcher(labels.MatchNotEqual, "replica", "1"),
				},
				{
					labels.MustNewMatcher(labels.MatchEqual, "op", "get"),
					labels.MustNewMatcher(labels.MatchEqual, "replica", "0"),
				},
			},
			extLabels:         labels.FromStrings("replica", "1"),
			shouldMatch:       false,
			expectedSelectors: nil,
		},
	}

	for name, tdata := range tests {
		t.Run(name, func(t *testing.T) {
			match, selectors := selectorsMatchesExternalLabels(tdata.selectors, tdata.extLabels)
			testutil.Equals(t, tdata.shouldMatch, match)

			testutil.Equals(t, tdata.expectedSelectors, selectors)
		})
	}
}
