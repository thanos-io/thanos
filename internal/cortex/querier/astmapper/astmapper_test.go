// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package astmapper

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestCloneNode(t *testing.T) {
	var testExpr = []struct {
		input    parser.Expr
		expected parser.Expr
	}{
		// simple unmodified case
		{
			&parser.BinaryExpr{
				Op:  parser.ADD,
				LHS: &parser.NumberLiteral{Val: 1},
				RHS: &parser.NumberLiteral{Val: 1},
			},
			&parser.BinaryExpr{
				Op:  parser.ADD,
				LHS: &parser.NumberLiteral{Val: 1, PosRange: parser.PositionRange{Start: 0, End: 1}},
				RHS: &parser.NumberLiteral{Val: 1, PosRange: parser.PositionRange{Start: 4, End: 5}},
			},
		},
		{
			&parser.AggregateExpr{
				Op:      parser.SUM,
				Without: true,
				Expr: &parser.VectorSelector{
					Name: "some_metric",
					LabelMatchers: []*labels.Matcher{
						mustLabelMatcher(labels.MatchEqual, string(model.MetricNameLabel), "some_metric"),
					},
				},
				Grouping: []string{"foo"},
			},
			&parser.AggregateExpr{
				Op:      parser.SUM,
				Without: true,
				Expr: &parser.VectorSelector{
					Name: "some_metric",
					LabelMatchers: []*labels.Matcher{
						mustLabelMatcher(labels.MatchEqual, string(model.MetricNameLabel), "some_metric"),
					},
					PosRange: parser.PositionRange{
						Start: 19,
						End:   30,
					},
				},
				Grouping: []string{"foo"},
				PosRange: parser.PositionRange{
					Start: 0,
					End:   31,
				},
			},
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			res, err := CloneNode(c.input)
			require.NoError(t, err)
			require.Equal(t, c.expected, res)
		})
	}
}

func TestCloneNode_String(t *testing.T) {
	var testExpr = []struct {
		input    string
		expected string
	}{
		{
			input:    `rate(http_requests_total{cluster="us-central1"}[1m])`,
			expected: `rate(http_requests_total{cluster="us-central1"}[1m])`,
		},
		{
			input: `sum(
sum(rate(http_requests_total{cluster="us-central1"}[1m]))
/
sum(rate(http_requests_total{cluster="ops-tools1"}[1m]))
)`,
			expected: `sum(sum(rate(http_requests_total{cluster="us-central1"}[1m])) / sum(rate(http_requests_total{cluster="ops-tools1"}[1m])))`,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			expr, err := parser.ParseExpr(c.input)
			require.Nil(t, err)
			res, err := CloneNode(expr)
			require.Nil(t, err)
			require.Equal(t, c.expected, res.String())
		})
	}
}

func mustLabelMatcher(mt labels.MatchType, name, val string) *labels.Matcher {
	m, err := labels.NewMatcher(mt, name, val)
	if err != nil {
		panic(err)
	}
	return m
}
