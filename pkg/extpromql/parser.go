// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extpromql

import (
	"fmt"
	"maps"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/parse"
)

// DefaultParserOptions is configured at startup, before serving queries.
var DefaultParserOptions parser.Options

func init() {
	// Prometheus 3.15 does not support per-parser custom functions. Register
	// Thanos functions during initialization, before any concurrent parsing.
	maps.Copy(parser.Functions, parse.XFunctions)
}

// NewParser returns a parser configured with the startup options.
func NewParser() parser.Parser {
	return parser.NewParser(DefaultParserOptions)
}

// ParseExpr parses the input PromQL expression and returns the parsed representation.
func ParseExpr(input string) (parser.Expr, error) {
	return NewParser().ParseExpr(input)
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
