// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryprojection

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAnalyzeQuery(t *testing.T) {
	analyzer := NewQueryAnalyzer()

	for _, tc := range []struct {
		name             string
		expression       string
		labels           []string
		expectedBy       bool
		expectedGrouping bool
		expectedLabels   []string
	}{
		{
			name:       "vector selector, not projectable",
			expression: "up",
		},
		{
			name:       "matrix selector, not projectable",
			expression: "up[5m]",
		},
		{
			name:       "contains label_replace, not projectable",
			expression: `label_replace(sum(up), "dst_label", "$1", "src_label", "re")`,
		},
		{
			name:       "binary expression",
			expression: `http_requests_total{code="400"} / http_requests_total`,
		},
		{
			name:       "binary expression with constant",
			expression: `http_requests_total{code="400"} / 4`,
		},
		{
			name:             "binary expression with empty vector matching",
			expression:       `http_requests_total{code="400"} / on () http_requests_total`,
			expectedBy:       true,
			expectedGrouping: true,
		},
		{
			name:             "binary expression with vector matching",
			expression:       `http_requests_total{code="400"} / on (pod) http_requests_total`,
			expectedBy:       true,
			expectedGrouping: true,
			expectedLabels:   []string{"pod"},
		},
		{
			name:             "binary aggregation with different grouping labels",
			expression:       `sum by (pod) (http_requests_total{code="400"}) / sum by (cluster) (http_requests_total)`,
			expectedBy:       true,
			expectedGrouping: true,
			expectedLabels:   []string{"pod", "cluster"},
		},
		{
			name:       "binary expression with vector matching and label_replace",
			expression: `http_requests_total{code="400"} / on (pod) label_replace(metric, "dst_label", "$1", "src_label", "re")`,
		},
		{
			name:       "multiple binary expressions",
			expression: `(http_requests_total{code="400"} + http_requests_total{code="500"}) / http_requests_total`,
		},
		{
			name: "multiple binary expressions with empty vector matchers",
			expression: `
(http_requests_total{code="400"} + on (cluster, pod) http_requests_total{code="500"})
/ on ()
http_requests_total`,
			expectedBy:       true,
			expectedGrouping: true,
			expectedLabels:   []string{"cluster", "pod"},
		},
		{
			name:             "outer aggregation with by grouping",
			expression:       "count(sum by (pod) (http_requests_total))",
			expectedBy:       true,
			expectedGrouping: true,
			expectedLabels:   []string{"pod"},
		},
		{
			name:             "outer aggregation with without grouping",
			expression:       "count(sum without (pod) (http_requests_total))",
			expectedBy:       false,
			expectedGrouping: true,
			expectedLabels:   []string{"pod"},
		},
		{
			name:             "nested aggregations",
			expression:       "max(count(sum by (pod, instance) (http_requests_total)) by (pod))",
			expectedBy:       true,
			expectedGrouping: true,
			expectedLabels:   []string{"pod", "instance"},
		},
		{
			name:             "aggregation sum no grouping labels",
			expression:       "sum(up)",
			expectedBy:       true,
			expectedGrouping: true,
		},
		{
			name:             "aggregation sum by labels",
			expression:       "sum(up) by (instance, job)",
			expectedBy:       true,
			expectedGrouping: true,
			expectedLabels:   []string{"instance", "job"},
		},
		{
			name:             "aggregation sum without labels",
			expression:       "sum(up) without (env, pod)",
			expectedBy:       false,
			expectedGrouping: true,
			expectedLabels:   []string{"env", "pod"},
		},
		{
			name:             "histogram quantile",
			expression:       "histogram_quantile(0.95, sum(rate(metric[1m])) by (le, cluster))",
			expectedBy:       true,
			expectedGrouping: true,
			expectedLabels:   []string{"cluster", "le"},
		},
		{
			name:             "subquery",
			expression:       "sum(http_requests_total) by (pod, cluster) [1h:1m]",
			expectedBy:       true,
			expectedGrouping: true,
			expectedLabels:   []string{"cluster", "pod"},
		},
		{
			name:             "subquery with function",
			expression:       "increase(sum(http_requests_total) by (pod, cluster) [1h:1m])",
			expectedBy:       true,
			expectedGrouping: true,
			expectedLabels:   []string{"cluster", "pod"},
		},
		{
			name:             "ignore vector matching with 2 aggregations",
			expression:       `sum(rate(node_cpu_seconds_total[3h])) by (cluster_id, mode) / ignoring(mode) group_left sum(rate(node_cpu_seconds_total[3h])) by (cluster_id)`,
			expectedBy:       true,
			expectedGrouping: true,
			expectedLabels:   []string{"cluster_id"},
		},
		{
			name:             "multiple aggregations with without grouping",
			expression:       "max without (pod) (sum without (pod, cluster) (http_requests_total))",
			expectedBy:       false,
			expectedGrouping: true,
			expectedLabels:   []string{"pod"},
		},
		{
			name:             "binary expression with without vector matching and grouping",
			expression:       `sum without (cluster, pod) (http_requests_total{code="400"}) / ignoring (pod) sum without (cluster, pod) (http_requests_total)`,
			expectedBy:       false,
			expectedGrouping: true,
			expectedLabels:   []string{"pod"},
		},
		{
			name:             "multiple binary expressions with without grouping",
			expression:       `(http_requests_total{code="400"} + ignoring (pod) http_requests_total{code="500"}) / ignoring (cluster, pod) http_requests_total`,
			expectedBy:       false,
			expectedGrouping: true,
			expectedLabels:   []string{"pod"},
		},
		{
			name: "multiple binary expressions with without vector matchers",
			expression: `
(http_requests_total{code="400"} + ignoring (cluster, pod) http_requests_total{code="500"})
/ ignoring (pod)
http_requests_total`,
			expectedBy:       false,
			expectedGrouping: true,
			expectedLabels:   []string{"pod"},
		},
		{
			name:             "histogram quantile",
			expression:       "histogram_quantile(0.95, sum(rate(metric[1m])) without (le, cluster))",
			expectedBy:       false,
			expectedGrouping: true,
			expectedLabels:   []string{"cluster", "le"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			analysis, err := analyzer.Analyze(tc.expression)
			require.NoError(t, err)
			require.Equal(t, tc.expectedGrouping, analysis.Grouping())
			require.Equal(t, tc.expectedBy, analysis.By())
			sort.Strings(tc.expectedLabels)
			actualLabels := analysis.Labels()
			sort.Strings(actualLabels)
			require.Equal(t, tc.expectedLabels, actualLabels)
		})
	}
}
