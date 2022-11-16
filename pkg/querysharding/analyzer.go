// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querysharding

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"
)

type Analyzer interface {
	Analyze(string) (QueryAnalysis, error)
}

// QueryAnalyzer is an analyzer which determines
// whether a PromQL Query is shardable and using which labels.
type QueryAnalyzer struct{}

type CachedQueryAnalyzer struct {
	analyzer *QueryAnalyzer
	cache    *lru.Cache
}

// NewQueryAnalyzer creates a new QueryAnalyzer.
func NewQueryAnalyzer() *CachedQueryAnalyzer {
	// Ignore the error check since it throws error
	// only if size is <= 0.
	cache, _ := lru.New(256)
	return &CachedQueryAnalyzer{
		analyzer: &QueryAnalyzer{},
		cache:    cache,
	}
}

type cachedValue struct {
	QueryAnalysis QueryAnalysis
	err           error
}

func (a *CachedQueryAnalyzer) Analyze(query string) (QueryAnalysis, error) {
	if a.cache.Contains(query) {
		value, ok := a.cache.Get(query)
		if ok {
			return value.(cachedValue).QueryAnalysis, value.(cachedValue).err
		}
	}

	// Analyze if needed.
	analysis, err := a.analyzer.Analyze(query)

	// Adding to cache.
	_ = a.cache.Add(query, cachedValue{QueryAnalysis: analysis, err: err})

	return analysis, err
}

// Analyze analyzes a query and returns a QueryAnalysis.

// Analyze uses the following algorithm:
//   - if a query has functions which cannot be sharded such as
//     label_join or label_replace, then treat the query as non shardable.
//   - Walk the query and find the least common labelset
//     used in grouping expressions. If non-empty, treat the query
//     as shardable by those labels.
//   - otherwise, treat the query as non-shardable.
//
// The le label is excluded from sharding.
func (a *QueryAnalyzer) Analyze(query string) (QueryAnalysis, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nonShardableQuery(), err
	}

	var (
		analysis      QueryAnalysis
		dynamicLabels []string
	)
	parser.Inspect(expr, func(node parser.Node, nodes []parser.Node) error {
		switch n := node.(type) {
		case *parser.Call:
			if n.Func != nil {
				if n.Func.Name == "label_join" || n.Func.Name == "label_replace" {
					dstLabel := stringFromArg(n.Args[1])
					dynamicLabels = append(dynamicLabels, dstLabel)
				}
			}
		case *parser.BinaryExpr:
			if n.VectorMatching != nil {
				shardingLabels := without(n.VectorMatching.MatchingLabels, []string{"le"})
				if !n.VectorMatching.On && len(shardingLabels) > 0 {
					shardingLabels = append(shardingLabels, model.MetricNameLabel)
				}
				analysis = analysis.scopeToLabels(shardingLabels, n.VectorMatching.On)
			}
		case *parser.AggregateExpr:
			shardingLabels := make([]string, 0)
			if len(n.Grouping) > 0 {
				shardingLabels = without(n.Grouping, []string{"le"})
			}
			analysis = analysis.scopeToLabels(shardingLabels, !n.Without)
		}

		return nil
	})

	// If currently it is shard by, it is still shardable if there is
	// any label left after removing the dynamic labels.
	// If currently it is shard without, it is still shardable if we
	// shard without the union of the labels.
	// TODO(yeya24): we can still make dynamic labels shardable if we push
	// down the label_replace and label_join computation to the store level.
	if len(dynamicLabels) > 0 {
		analysis = analysis.scopeToLabels(dynamicLabels, false)
	}

	return analysis, nil
}

// Copied from https://github.com/prometheus/prometheus/blob/v2.40.1/promql/functions.go#L1416.
func stringFromArg(e parser.Expr) string {
	tmp := unwrapStepInvariantExpr(e) // Unwrap StepInvariant
	unwrapParenExpr(&tmp)             // Optionally unwrap ParenExpr
	return tmp.(*parser.StringLiteral).Val
}

// Copied from https://github.com/prometheus/prometheus/blob/v2.40.1/promql/engine.go#L2642.
// unwrapParenExpr does the AST equivalent of removing parentheses around a expression.
func unwrapParenExpr(e *parser.Expr) {
	for {
		if p, ok := (*e).(*parser.ParenExpr); ok {
			*e = p.Expr
		} else {
			break
		}
	}
}

// Copied from https://github.com/prometheus/prometheus/blob/v2.40.1/promql/engine.go#L2652.
func unwrapStepInvariantExpr(e parser.Expr) parser.Expr {
	if p, ok := e.(*parser.StepInvariantExpr); ok {
		return p.Expr
	}
	return e
}
