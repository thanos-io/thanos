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
			want, err := parser.NewParser(parser.Options{}).ParseMetricSelector(tc.input)
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

func TestNewParserAndNilFallback(t *testing.T) {
	for _, tc := range []struct {
		name               string
		enableExperimental bool
		enableXFunctions   bool
		expr               string
		wantErr            bool
	}{
		{
			name:    "experimental disabled",
			expr:    "double_exponential_smoothing(up[1h], 0.5, 0.5)",
			wantErr: true,
		},
		{
			name:               "experimental enabled",
			enableExperimental: true,
			expr:               "double_exponential_smoothing(up[1h], 0.5, 0.5)",
		},
		{
			name:    "xfunctions disabled",
			expr:    "xrate(http_requests_total[5m])",
			wantErr: true,
		},
		{
			name:             "xfunctions enabled",
			enableXFunctions: true,
			expr:             "xrate(http_requests_total[5m])",
		},
		{
			name:               "both enabled",
			enableExperimental: true,
			enableXFunctions:   true,
			expr:               "double_exponential_smoothing(xrate(http_requests_total[5m])[1h:1m], 0.5, 0.5)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := extpromql.NewParser(tc.enableExperimental, tc.enableXFunctions)
			_, err := extpromql.ParseExprWithParser(p, tc.expr)
			testutil.Equals(t, tc.wantErr, err != nil)
		})
	}

	t.Run("nil parser falls back to ParseExpr with xfunctions", func(t *testing.T) {
		_, err := extpromql.ParseExprWithParser(nil, "xrate(http_requests_total[5m])")
		testutil.Ok(t, err)
	})

	// Ensure NewParser never mutates Prometheus's global parser.Functions map.
	_, exists := parser.Functions["xrate"]
	testutil.Assert(t, !exists, "global parser.Functions should not be mutated with xrate")
}

func stringFmt(got []*labels.Matcher) string {
	return fmt.Sprintf("%v", got)
}
