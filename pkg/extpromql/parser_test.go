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

func TestParserOptions(t *testing.T) {
	previous := extpromql.DefaultParserOptions
	t.Cleanup(func() { extpromql.DefaultParserOptions = previous })

	extpromql.DefaultParserOptions = parser.Options{}
	disabled := extpromql.NewParser()
	for _, query := range []string{"rate(up[5m])", "xrate(up[5m])", "xincrease(up[5m])", "xdelta(up[5m])"} {
		_, err := disabled.ParseExpr(query)
		testutil.Ok(t, err)
	}

	const experimental = `sort_by_label(up, "job")`
	_, err := disabled.ParseExpr(experimental)
	if err == nil {
		t.Fatal("experimental functions must be disabled by default")
	}

	extpromql.DefaultParserOptions.EnableExperimentalFunctions = true
	_, err = extpromql.ParseExpr(experimental)
	testutil.Ok(t, err)
	_, err = disabled.ParseExpr(experimental)
	if err == nil {
		t.Fatal("changing startup options must not mutate an existing parser")
	}
}

func stringFmt(got []*labels.Matcher) string {
	return fmt.Sprintf("%v", got)
}
