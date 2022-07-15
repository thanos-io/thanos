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

func TestCanParallel(t *testing.T) {
	var testExpr = []struct {
		input    parser.Expr
		expected bool
	}{
		// simple sum
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
			true,
		},
		/*
			  sum(
				  sum by (foo) bar1{baz=”blip”}[1m])
				/
				  sum by (foo) bar2{baz=”blip”}[1m]))
			  )
		*/
		{
			&parser.AggregateExpr{
				Op: parser.SUM,
				Expr: &parser.BinaryExpr{
					Op: parser.DIV,
					LHS: &parser.AggregateExpr{
						Op:       parser.SUM,
						Grouping: []string{"foo"},
						Expr: &parser.VectorSelector{
							Name: "idk",
							LabelMatchers: []*labels.Matcher{
								mustLabelMatcher(labels.MatchEqual, string(model.MetricNameLabel), "bar1"),
							}},
					},
					RHS: &parser.AggregateExpr{
						Op:       parser.SUM,
						Grouping: []string{"foo"},
						Expr: &parser.VectorSelector{
							Name: "idk",
							LabelMatchers: []*labels.Matcher{
								mustLabelMatcher(labels.MatchEqual, string(model.MetricNameLabel), "bar2"),
							}},
					},
				},
			},
			false,
		},
		// sum by (foo) bar1{baz=”blip”}[1m]) ---- this is the first leg of the above
		{
			&parser.AggregateExpr{
				Op:       parser.SUM,
				Grouping: []string{"foo"},
				Expr: &parser.VectorSelector{
					Name: "idk",
					LabelMatchers: []*labels.Matcher{
						mustLabelMatcher(labels.MatchEqual, string(model.MetricNameLabel), "bar1"),
					}},
			},
			true,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			res := CanParallelize(c.input)
			require.Equal(t, c.expected, res)
		})
	}
}

func TestCanParallel_String(t *testing.T) {
	var testExpr = []struct {
		input    string
		expected bool
	}{
		{
			`sum by (foo) (rate(bar1{baz="blip"}[1m]))`,
			true,
		},
		{
			`sum by (foo) (histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[10m])))`,
			false,
		},
		{
			`sum by (foo) (
			  quantile_over_time(0.9, http_request_duration_seconds_bucket[10m])
			)`,
			false,
		},
		{
			`sum(
				count(
					count(
						foo{bar="baz"}
					)  by (a,b)
				)  by (instance)
			)`,
			false,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			expr, err := parser.ParseExpr(c.input)
			require.Nil(t, err)
			res := CanParallelize(expr)
			require.Equal(t, c.expected, res)
		})
	}
}
