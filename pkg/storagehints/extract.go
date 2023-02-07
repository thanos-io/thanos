// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storagehints

import (
	"strconv"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

const SelectorIDLabel = "__thanos_selector_id"

// ExtractThanosHints walks the PromQL AST, extracts extended hints for series selectors
// and injects a synthetic __thanos_selector_id label in each selector.
// This label can be used to find the hint for each selector when retrieving series
// from storage.
func ExtractThanosHints(query string) (string, map[string]storepb.QueryHints, error) {
	node, err := parser.ParseExpr(query)
	if err != nil {
		return "", nil, err
	}

	var (
		selectorID    int
		hints         = make(map[string]storepb.QueryHints)
		selectorHints = storepb.QueryHints{}
	)

	traverse(&node, func(expr *parser.Expr) {
		switch e := (*expr).(type) {
		case *parser.VectorSelector:
			hints[strconv.Itoa(selectorID)] = selectorHints
			selectorHints = storepb.QueryHints{}

			e.LabelMatchers = append(e.LabelMatchers, &labels.Matcher{
				Type:  labels.MatchEqual,
				Name:  SelectorIDLabel,
				Value: strconv.Itoa(selectorID),
			})
			selectorID++
			*expr = e
		case *parser.Call:
			if isOverTimeFunction(e.Func.Name) {
				selectorHints.TimeFunc = &storepb.Func{Name: e.Func.Name}
			} else {
				selectorHints.AggrFunc = &storepb.Func{Name: e.Func.Name}
				selectorHints.Grouping = nil
			}
		case *parser.AggregateExpr:
			selectorHints.AggrFunc = &storepb.Func{Name: e.Op.String()}
			selectorHints.Grouping = &storepb.Grouping{
				By:     !e.Without,
				Labels: e.Grouping,
			}
		}
	})

	return node.String(), hints, nil
}

func isOverTimeFunction(funcName string) bool {
	for _, arg := range parser.Functions[funcName].ArgTypes {
		if arg == parser.ValueTypeMatrix {
			return true
		}
	}
	return false
}

func traverse(expr *parser.Expr, transform func(*parser.Expr)) {
	switch node := (*expr).(type) {
	case *parser.StepInvariantExpr:
		traverse(&node.Expr, transform)
	case *parser.VectorSelector:
		transform(expr)
	case *parser.MatrixSelector:
		traverse(&node.VectorSelector, transform)
	case *parser.AggregateExpr:
		transform(expr)
		traverse(&node.Expr, transform)
	case *parser.Call:
		transform(expr)
		for _, n := range node.Args {
			traverse(&n, transform)
		}
	case *parser.BinaryExpr:
		traverse(&node.LHS, transform)
		traverse(&node.RHS, transform)
	case *parser.UnaryExpr:
		traverse(&node.Expr, transform)
	case *parser.ParenExpr:
		traverse(&node.Expr, transform)
	case *parser.SubqueryExpr:
		traverse(&node.Expr, transform)
	}
}
