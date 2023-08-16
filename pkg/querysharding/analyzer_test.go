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
		{
			name:       "aggregate by expression with label_replace, sharding label is dynamic",
			expression: `sum by (dst_label) (label_replace(metric, "dst_label", "$1", "src_label", "re"))`,
		},
		{
			name:       "aggregate by expression with label_join, sharding label is dynamic",
			expression: `sum by (dst_label) (label_join(metric, "dst_label", ",", "src_label"))`,
		},
		{
			name:       "absent_over_time is not shardable",
			expression: `sum by (url) (absent_over_time(http_requests_total{code="400"}[5m]))`,
		},
		{
			name:       "absent is not shardable",
			expression: `sum by (url) (absent(http_requests_total{code="400"}))`,
		},
		{
			name:       "scalar is not shardable",
			expression: `scalar(sum by (url) (http_requests_total{code="400"}))`,
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
			name:           "binary expression with vector matching with outer aggregation",
			expression:     `sum(http_requests_total{code="400"} * http_requests_total) by (pod)`,
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
			shardingLabels: []string{"pod"},
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
		{
			name:           "ignore vector matching with 2 aggregations",
			expression:     `sum(rate(node_cpu_seconds_total[3h])) by (cluster_id, mode) / ignoring(mode) group_left sum(rate(node_cpu_seconds_total[3h])) by (cluster_id)`,
			shardingLabels: []string{"cluster_id"},
		},
		{
			name:           "aggregate by expression with label_replace, sharding label is not dynamic",
			expression:     `sum by (pod) (label_replace(metric, "dst_label", "$1", "src_label", "re"))`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "aggregate by expression with label_join, sharding label is not dynamic",
			expression:     `sum by (pod) (label_join(metric, "dst_label", ",", "src_label"))`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "label_join and aggregation on multiple labels. Can be sharded by the static one",
			expression:     `sum by (pod, dst_label) (label_join(metric, "dst_label", ",", "src_label"))`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "binary expression with vector matching and label_replace",
			expression:     `http_requests_total{code="400"} / on (pod) label_replace(metric, "dst_label", "$1", "src_label", "re")`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "nested label joins",
			expression:     `label_join(sum by (pod) (label_join(metric, "dst_label", ",", "src_label")), "dst_label1", ",", "dst_label")`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "complex query with label_replace, binary expr and aggregations on dynamic label",
			expression:     `sum(sum_over_time(container_memory_working_set_bytes{container_name!="POD",container_name!="",namespace="kube-system"}[1d:5m])) by (instance, cluster) / avg(label_replace(sum(sum_over_time(kube_node_status_capacity_memory_bytes[1d:5m])) by (node, cluster), "instance", "$1", "node", "(.*)")) by (instance, cluster)`,
			shardingLabels: []string{"cluster"},
		},
		{
			name:           "complex query with label_replace and nested aggregations",
			expression:     `avg(label_replace(label_replace(avg(count_over_time(kube_pod_container_resource_requests{resource="memory", unit="byte", container!="",container!="POD", node!="", }[1h] )*avg_over_time(kube_pod_container_resource_requests{resource="memory", unit="byte", container!="",container!="POD", node!="", }[1h] )) by (namespace,container,pod,node,cluster_id) , "container_name","$1","container","(.+)"), "pod_name","$1","pod","(.+)")) by (namespace,container_name,pod_name,node,cluster_id)`,
			shardingLabels: []string{"namespace", "node", "cluster_id"},
		},
		{
			name:           "complex query with label_replace, nested aggregations and binary expressions",
			expression:     `sort_desc(avg(label_replace(label_replace(label_replace(count_over_time(container_memory_working_set_bytes{container!="", container!="POD", instance!="", }[1h] ), "node", "$1", "instance", "(.+)"), "container_name", "$1", "container", "(.+)"), "pod_name", "$1", "pod", "(.+)")*label_replace(label_replace(label_replace(avg_over_time(container_memory_working_set_bytes{container!="", container!="POD", instance!="", }[1h] ), "node", "$1", "instance", "(.+)"), "container_name", "$1", "container", "(.+)"), "pod_name", "$1", "pod", "(.+)")) by (namespace, container_name, pod_name, node, cluster_id))`,
			shardingLabels: []string{"namespace", "cluster_id"},
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
			name:           "binary expression with outer without grouping",
			expression:     `sum(http_requests_total{code="400"} * http_requests_total) without (pod)`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "binary expression with vector matching and outer without grouping",
			expression:     `sum(http_requests_total{code="400"} * ignoring(cluster) http_requests_total) without ()`,
			shardingLabels: []string{"__name__", "cluster"},
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
		{
			name:           "aggregate without expression with label_replace, sharding label is not dynamic",
			expression:     `sum without (dst_label) (label_replace(metric, "dst_label", "$1", "src_label", "re"))`,
			shardingLabels: []string{"dst_label"},
		},
		{
			name:           "aggregate without expression with label_join, sharding label is not dynamic",
			expression:     `sum without (dst_label) (label_join(metric, "dst_label", ",", "src_label"))`,
			shardingLabels: []string{"dst_label"},
		},
		{
			name:           "aggregate without expression with label_replace",
			expression:     `sum without (pod) (label_replace(metric, "dst_label", "$1", "src_label", "re"))`,
			shardingLabels: []string{"pod", "dst_label"},
		},
	}

	for _, test := range nonShardable {
		t.Run(test.name, func(t *testing.T) {
			analyzer := NewQueryAnalyzer()
			analysis, err := analyzer.Analyze(test.expression)
			require.NoError(t, err)
			require.False(t, analysis.IsShardable())
		})
	}

	for _, test := range shardableByLabels {
		t.Run(test.name, func(t *testing.T) {
			analyzer := NewQueryAnalyzer()
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
			analyzer := NewQueryAnalyzer()
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
