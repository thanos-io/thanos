// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extpromql_test

import (
	"fmt"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/thanos/pkg/extpromql"
)

func TestParseMetricSelector(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{
			name:  "single selector",
			input: `http_requests_total{method="GET"}`,
		},
		{
			name:  "empty selectors",
			input: `process_cpu_seconds_total`,
		},
		{
			name:  "multiple selectors",
			input: `http_requests_total{method="GET",code="200"}`,
		},
		{
			name:  "multiple selectors with different matchers",
			input: `http_requests_total{method="GET",code!="200"}`,
		},
		{
			name:  "multiple selectors with regex",
			input: `http_requests_total{method="GET",code=~"2.*"}`,
		},
		{
			name:  "selector with negative regex",
			input: `{code!~"2.*"}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			//lint:ignore faillint Testing against prometheus parser.
			want, err := parser.ParseMetricSelector(tc.input)
			if err != nil {
				t.Fatalf("Prometheus ParseMetricSelector failed: %v", err)
			}

			got, err := extpromql.ParseMetricSelector(tc.input)
			if err != nil {
				t.Fatalf("ParseMetricSelector failed: %v", err)
			}

			testutil.Equals(t, stringFmt(want), stringFmt(got))
		})
	}
}

func stringFmt(got []*labels.Matcher) string {
	return fmt.Sprintf("%v", got)
}
