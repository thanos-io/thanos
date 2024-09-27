// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package filter

type MetricNameFilter interface {
	MatchesMetricName(metricName string) bool
	ResetAddMetricName(metricNames ...string)
}

type AllowAllMetricNameFilter struct{}

func (f AllowAllMetricNameFilter) MatchesMetricName(metricName string) bool {
	return true
}

func (f AllowAllMetricNameFilter) ResetAddMetricName(metricNames ...string) {}
