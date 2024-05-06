// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extpromql

import (
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/function"
)

// ParseExpr parses the input PromQL expression and returns the parsed representation.
func ParseExpr(input string) (parser.Expr, error) {
	p := parser.NewParser(input, parser.WithFunctions(function.XFunctions))
	defer p.Close()
	return p.ParseExpr()
}

// ParseMetricSelector parses the provided textual metric selector into a list of
// label matchers.
func ParseMetricSelector(input string) ([]*labels.Matcher, error) {
	// Parse the input string as a PromQL expression.
	expr, err := ParseExpr(input)
	if err != nil {
		return nil, err
	}

	// The type of the expression should be *parser.VectorSelector.
	vs, ok := expr.(*parser.VectorSelector)
	if !ok {
		return nil, fmt.Errorf("expected type *parser.VectorSelector, got %T", expr)
	}

	// Convert the label matchers from the vector selector to the desired type.
	matchers := make([]*labels.Matcher, len(vs.LabelMatchers))
	for i, lm := range vs.LabelMatchers {
		matchers[i] = &labels.Matcher{
			Type:  labels.MatchType(lm.Type),
			Name:  lm.Name,
			Value: lm.Value,
		}
	}

	return matchers, nil
}
