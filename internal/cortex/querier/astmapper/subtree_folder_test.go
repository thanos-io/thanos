// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package astmapper

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestPredicate(t *testing.T) {
	for i, tc := range []struct {
		input    string
		fn       predicate
		expected bool
		err      bool
	}{
		{
			input: "selector1{} or selector2{}",
			fn: predicate(func(node parser.Node) (bool, error) {
				return false, errors.New("some err")
			}),
			expected: false,
			err:      true,
		},
		{
			input: "selector1{} or selector2{}",
			fn: predicate(func(node parser.Node) (bool, error) {
				return false, nil
			}),
			expected: false,
			err:      false,
		},
		{
			input: "selector1{} or selector2{}",
			fn: predicate(func(node parser.Node) (bool, error) {
				return true, nil
			}),
			expected: true,
			err:      false,
		},
		{
			input:    `sum without(__cortex_shard__) (__embedded_queries__{__cortex_queries__="tstquery"}) or sum(selector)`,
			fn:       predicate(isEmbedded),
			expected: true,
			err:      false,
		},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			expr, err := parser.ParseExpr(tc.input)
			require.Nil(t, err)

			res, err := Predicate(expr.(parser.Node), tc.fn)
			if tc.err {
				require.Error(t, err)
			} else {
				require.Nil(t, err)
			}

			require.Equal(t, tc.expected, res)
		})
	}
}

func TestSubtreeMapper(t *testing.T) {
	for i, tc := range []struct {
		input    string
		expected string
	}{
		// embed an entire histogram
		{
			input:    "histogram_quantile(0.5, rate(alertmanager_http_request_duration_seconds_bucket[1m]))",
			expected: `__embedded_queries__{__cortex_queries__="{\"Concat\":[\"histogram_quantile(0.5, rate(alertmanager_http_request_duration_seconds_bucket[1m]))\"]}"}`,
		},
		// embed a binary expression across two functions
		{
			input:    `rate(http_requests_total{cluster="eu-west2"}[5m]) or rate(http_requests_total{cluster="us-central1"}[5m])`,
			expected: `__embedded_queries__{__cortex_queries__="{\"Concat\":[\"rate(http_requests_total{cluster=\\\"eu-west2\\\"}[5m]) or rate(http_requests_total{cluster=\\\"us-central1\\\"}[5m])\"]}"}`,
		},

		// the first leg (histogram) hasn't been embedded at any level, so embed that, but ignore the right leg
		// which has already been embedded.
		{
			input: `sum(histogram_quantile(0.5, rate(selector[1m]))) +
				sum without(__cortex_shard__) (__embedded_queries__{__cortex_queries__="tstquery"})`,
			expected: `
			  __embedded_queries__{__cortex_queries__="{\"Concat\":[\"sum(histogram_quantile(0.5, rate(selector[1m])))\"]}"} +
			  sum without(__cortex_shard__) (__embedded_queries__{__cortex_queries__="tstquery"})
`,
		},
		// should not embed scalars
		{
			input:    `histogram_quantile(0.5, __embedded_queries__{__cortex_queries__="tstquery"})`,
			expected: `histogram_quantile(0.5, __embedded_queries__{__cortex_queries__="tstquery"})`,
		},
	} {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			mapper := NewSubtreeFolder()

			expr, err := parser.ParseExpr(tc.input)
			require.Nil(t, err)
			res, err := mapper.Map(expr)
			require.Nil(t, err)

			expected, err := parser.ParseExpr(tc.expected)
			require.Nil(t, err)

			require.Equal(t, expected.String(), res.String())

		})
	}
}
