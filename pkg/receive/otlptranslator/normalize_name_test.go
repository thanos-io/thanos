// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package otlptranslator

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestTrimPromSuffixes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		promName   string
		metricType pmetric.MetricType
		unit       string
		expected   string
	}{
		{
			name:       "counter with _total suffix",
			promName:   "http_requests_total",
			metricType: pmetric.MetricTypeSum,
			unit:       "",
			expected:   "http_requests",
		},
		{
			name:       "gauge with unit suffix seconds",
			promName:   "request_duration_seconds",
			metricType: pmetric.MetricTypeGauge,
			unit:       "seconds",
			expected:   "request_duration",
		},
		{
			name:       "counter with _total and unit suffix",
			promName:   "http_request_duration_seconds_total",
			metricType: pmetric.MetricTypeSum,
			unit:       "seconds",
			expected:   "http_request_duration",
		},
		{
			name:       "empty unit string does not trim",
			promName:   "http_requests_total",
			metricType: pmetric.MetricTypeGauge,
			unit:       "",
			expected:   "http_requests_total",
		},
		{
			name:       "unit tokens equal name tokens in length",
			promName:   "bytes_total",
			metricType: pmetric.MetricTypeSum,
			unit:       "bytes",
			expected:   "bytes",
		},
		{
			name:       "total in middle of name is not trimmed",
			promName:   "total_http_requests",
			metricType: pmetric.MetricTypeSum,
			unit:       "",
			expected:   "total_http_requests",
		},
		{
			name:       "single token name no trim",
			promName:   "requests",
			metricType: pmetric.MetricTypeGauge,
			unit:       "",
			expected:   "requests",
		},
		{
			name:       "gauge type does not strip total suffix",
			promName:   "my_metric_total",
			metricType: pmetric.MetricTypeGauge,
			unit:       "",
			expected:   "my_metric_total",
		},
		{
			name:       "multi-token unit suffix",
			promName:   "disk_io_read_bytes_per_second",
			metricType: pmetric.MetricTypeGauge,
			unit:       "bytes_per_second",
			expected:   "disk_io_read",
		},
		{
			name:       "unit not present as suffix",
			promName:   "cpu_usage_percent",
			metricType: pmetric.MetricTypeGauge,
			unit:       "seconds",
			expected:   "cpu_usage_percent",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := TrimPromSuffixes(tc.promName, tc.metricType, tc.unit)
			testutil.Equals(t, tc.expected, result)
		})
	}
}
