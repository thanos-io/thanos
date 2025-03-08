// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
)

func TestAggregationLabelRewriter_Rewrite(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name               string
		desiredLabelValue  string // Empty means disabled
		inputMatchers      []*labels.Matcher
		expectedMatchers   []*labels.Matcher
		expectedSkipCount  float64
		expectedAddCount   float64
		expectedRewriteMap map[string]float64
	}{
		{
			name:              "disabled rewriter should not modify label matchers",
			desiredLabelValue: "",
			inputMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test:sum"),
			},
			expectedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test:sum"),
			},
		},
		{
			name:              "should add label for aggregated metric if no existing aggregation label",
			desiredLabelValue: "5m",
			inputMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test:sum"),
			},
			expectedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test:sum"),
				labels.MustNewMatcher(labels.MatchEqual, "__rollup__", "5m"),
			},
			expectedAddCount: 1,
		},
		{
			name:              "should rewrite existing aggregation label for aggregated metric",
			desiredLabelValue: "5m",
			inputMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test:sum"),
				labels.MustNewMatcher(labels.MatchEqual, "__rollup__", "1h"),
			},
			expectedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test:sum"),
				labels.MustNewMatcher(labels.MatchEqual, "__rollup__", "5m"),
			},
			expectedRewriteMap: map[string]float64{"1h": 1},
		},
		{
			name:              "should skip non-aggregated metric",
			desiredLabelValue: "5m",
			inputMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric"),
			},
			expectedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric"),
			},
			expectedSkipCount: 1,
		},
		{
			name:              "should skip non-equal name matcher",
			desiredLabelValue: "5m",
			inputMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test:sum"),
			},
			expectedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test:sum"),
			},
			expectedSkipCount: 1,
		},
		{
			name:              "should skip when no name matcher",
			desiredLabelValue: "5m",
			inputMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "prometheus"),
			},
			expectedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "prometheus"),
			},
			expectedSkipCount: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			rewriter := NewAggregationLabelRewriter(
				nil,
				reg,
				tc.desiredLabelValue,
			)

			result := rewriter.Rewrite(tc.inputMatchers)
			testutil.Equals(t, len(tc.expectedMatchers), len(result))
			for i := range tc.inputMatchers {
				testutil.Equals(t, tc.expectedMatchers[i].Name, result[i].Name)
				testutil.Equals(t, tc.expectedMatchers[i].Type, result[i].Type)
				testutil.Equals(t, tc.expectedMatchers[i].Value, result[i].Value)
			}

			metrics, err := reg.Gather()
			testutil.Ok(t, err)

			if tc.expectedSkipCount > 0 {
				var skipCount float64
				for _, m := range metrics {
					if m.GetName() == "skipped_total" {
						skipCount += *m.Metric[0].Counter.Value
					}
				}
				testutil.Equals(t, tc.expectedSkipCount, skipCount)
			}

			if tc.expectedAddCount > 0 {
				var addCount float64
				for _, m := range metrics {
					if m.GetName() == "label_added_total" {
						addCount += *m.Metric[0].Counter.Value
					}
				}
				testutil.Equals(t, tc.expectedAddCount, addCount)
			}

			if len(tc.expectedRewriteMap) > 0 {
				for _, m := range metrics {
					if m.GetName() == "label_rewritten_total" {
						for _, metric := range m.Metric {
							oldValue := ""
							for _, label := range metric.Label {
								if *label.Name == "old_value" {
									oldValue = *label.Value
									break
								}
							}
							if count, ok := tc.expectedRewriteMap[oldValue]; ok {
								testutil.Equals(t, count, *metric.Counter.Value)
							}
						}
					}
				}
			}
		})
	}
}
