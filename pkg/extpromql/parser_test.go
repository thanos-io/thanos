package extpromql_test

import (
	"fmt"
	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/extpromql"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Call your implementation of ParseMetricSelector
			got, err := extpromql.ParseMetricSelector(tc.input)
			if err != nil {
				t.Fatalf("ParseMetricSelector failed: %v", err)
			}

			want, err := parser.ParseMetricSelector(tc.input)
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
