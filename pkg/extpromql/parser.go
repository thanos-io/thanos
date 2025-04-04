// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extpromql

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/function"
)

// ParseExpr parses the input PromQL expression and returns the parsed representation.
func ParseExpr(input string) (parser.Expr, error) {
	allFuncs := make(map[string]*parser.Function, len(function.XFunctions)+len(parser.Functions))
	for k, v := range parser.Functions {
		allFuncs[k] = v
	}
	for k, v := range function.XFunctions {
		allFuncs[k] = v
	}
	p := parser.NewParser(input, parser.WithFunctions(allFuncs))
	defer p.Close()
	return p.ParseExpr()
}

// ParseMetricSelector parses the provided textual metric selector into a list of
// label matchers.
func ParseMetricSelector(input string) ([]*labels.Matcher, error) {
	expr, err := ParseExpr(input)
	// because of the AST checking present in the ParseExpr function,
	// we need to ignore the error if it is just the check for empty name matcher.
	if err != nil && !isEmptyNameMatcherErr(err) {
		return nil, err
	}

	vs, ok := expr.(*parser.VectorSelector)
	if !ok {
		return nil, fmt.Errorf("expected type *parser.VectorSelector, got %T", expr)
	}

	return vs.LabelMatchers, nil
}

func isEmptyNameMatcherErr(err error) bool {
	var parseErrs parser.ParseErrors
	if errors.As(err, &parseErrs) {
		return len(parseErrs) == 1 &&
			strings.HasSuffix(parseErrs[0].Error(), "vector selector must contain at least one non-empty matcher")
	}

	return false
}
