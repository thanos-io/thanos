// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extpromql

import (
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/execution/function"
)

func ParserExpr(input string) (parser.Expr, error) {
	p := parser.NewParser(input, parser.WithFunctions(function.XFunctions))
	defer p.Close()
	return p.ParseExpr()
}
