// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package querysharding

import (
	"sort"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeQuery(t *testing.T) {

	type testCase struct {
		name           string
		expression     string
		shardingLabels []string
	}

	nonShardable := []testCase{
		{
			name:       "aggregation",
			expression: "sum(http_requests_total)",
		},
		{
			name:       "outer aggregation with no grouping",
			expression: "count(sum by (pod) (http_requests_total))",
		},
		{
			name:       "outer aggregation with without grouping",
			expression: "count(sum without (pod) (http_requests_total))",
		},
		{
			name:       "aggregate expression with label_replace",
			expression: `sum by (pod) (label_replace(metric, "dst_label", "$1", "src_label", "re"))`,
		},
		{
			name:       "aggregate without expression with label_replace",
			expression: `sum without (pod) (label_replace(metric, "dst_label", "$1", "src_label", "re"))`,
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
			name:       "binary expression with empty vector matching",
			expression: `http_requests_total{code="400"} / on () http_requests_total`,
		},
		{
			name:       "binary aggregation with different grouping labels",
			expression: `sum by (pod) (http_requests_total{code="400"}) / sum by (cluster) (http_requests_total)`,
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
		},
	}

	shardableByLabels := []testCase{
		{
			name:           "aggregation with grouping",
			expression:     "sum by (pod) (http_requests_total)",
			shardingLabels: []string{"pod"},
		},
		{
			name:           "multiple aggregations with grouping",
			expression:     "max by (pod) (sum by (pod, cluster) (http_requests_total))",
			shardingLabels: []string{"pod"},
		},
		{
			name:           "binary expression with vector matching",
			expression:     `http_requests_total{code="400"} / on (pod) http_requests_total`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "binary aggregation with same grouping labels",
			expression:     `sum by (pod) (http_requests_total{code="400"}) / sum by (pod) (http_requests_total)`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "binary expression with vector matching and grouping",
			expression:     `sum by (cluster, pod) (http_requests_total{code="400"}) / on (pod) sum by (cluster, pod) (http_requests_total)`,
			shardingLabels: []string{"pod"},
		},
		{
			name: "multiple binary expressions with vector matchers",
			expression: `
(http_requests_total{code="400"} + on (cluster, pod) http_requests_total{code="500"})
/ on (pod)
http_requests_total`,
			shardingLabels: []string{"pod"},
		},
		{
			name: "multiple binary expressions with grouping",
			expression: `
sum by (container) (
	(http_requests_total{code="400"} + on (cluster, pod, container) http_requests_total{code="500"})
	/ on (pod, container)
	http_requests_total
)`,
			shardingLabels: []string{"container"},
		},
		{
			name:           "multiple binary expressions with grouping",
			expression:     `(http_requests_total{code="400"} + on (pod) http_requests_total{code="500"}) / on (cluster, pod) http_requests_total`,
			shardingLabels: []string{"cluster", "pod"},
		},
		{
			name:           "histogram quantile",
			expression:     "histogram_quantile(0.95, sum(rate(metric[1m])) by (le, cluster))",
			shardingLabels: []string{"cluster"},
		},
		{
			name:           "subquery",
			expression:     "sum(http_requests_total) by (pod, cluster) [1h:1m]",
			shardingLabels: []string{"cluster", "pod"},
		},
		{
			name:           "subquery with function",
			expression:     "increase(sum(http_requests_total) by (pod, cluster) [1h:1m])",
			shardingLabels: []string{"cluster", "pod"},
		},
	}

	shardableWithoutLabels := []testCase{
		{
			name:           "aggregation without grouping",
			expression:     "sum without (pod) (http_requests_total)",
			shardingLabels: []string{"pod"},
		},
		{
			name:           "multiple aggregations with without grouping",
			expression:     "max without (pod) (sum without (pod, cluster) (http_requests_total))",
			shardingLabels: []string{"pod", "cluster"},
		},
		{
			name:           "binary expression with without vector matching and grouping",
			expression:     `sum without (cluster, pod) (http_requests_total{code="400"}) / ignoring (pod) sum without (cluster, pod) (http_requests_total)`,
			shardingLabels: []string{"pod", "cluster", model.MetricNameLabel},
		},
		{
			name:           "multiple binary expressions with without grouping",
			expression:     `(http_requests_total{code="400"} + ignoring (pod) http_requests_total{code="500"}) / ignoring (cluster, pod) http_requests_total`,
			shardingLabels: []string{"cluster", "pod", model.MetricNameLabel},
		},
		{
			name: "multiple binary expressions with without vector matchers",
			expression: `
(http_requests_total{code="400"} + ignoring (cluster, pod) http_requests_total{code="500"})
/ ignoring (pod)
http_requests_total`,
			shardingLabels: []string{"cluster", "pod", model.MetricNameLabel},
		},
		{
			name:           "histogram quantile",
			expression:     "histogram_quantile(0.95, sum(rate(metric[1m])) without (le, cluster))",
			shardingLabels: []string{"cluster"},
		},
	}

	for _, test := range nonShardable {
		t.Run(test.name, func(t *testing.T) {
			analyzer, err := NewQueryAnalyzer()
			require.NoError(t, err)
			analysis, err := analyzer.Analyze(test.expression)
			require.NoError(t, err)
			require.False(t, analysis.IsShardable())
		})
	}

	for _, test := range shardableByLabels {
		t.Run(test.name, func(t *testing.T) {
			analyzer, err := NewQueryAnalyzer()
			require.NoError(t, err)
			analysis, err := analyzer.Analyze(test.expression)
			require.NoError(t, err)
			require.True(t, analysis.IsShardable())
			require.True(t, analysis.ShardBy())

			sort.Strings(test.shardingLabels)
			sort.Strings(analysis.ShardingLabels())
			require.Equal(t, test.shardingLabels, analysis.ShardingLabels())
		})
	}

	for _, test := range shardableWithoutLabels {
		t.Run(test.name, func(t *testing.T) {
			analyzer, err := NewQueryAnalyzer()
			require.NoError(t, err)
			analysis, err := analyzer.Analyze(test.expression)
			require.NoError(t, err)
			require.True(t, analysis.IsShardable())
			require.False(t, analysis.ShardBy())

			sort.Strings(test.shardingLabels)
			sort.Strings(analysis.ShardingLabels())
			require.Equal(t, test.shardingLabels, analysis.ShardingLabels())
		})
	}
}
