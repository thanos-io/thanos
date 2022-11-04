// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryprojection

import (
	"fmt"

	lru "github.com/hashicorp/golang-lru"
	"github.com/prometheus/prometheus/promql/parser"
)

// Analyzer determines whether a PromQL Query is projectable and using which labels.
type Analyzer interface {
	Analyze(string) (QueryAnalysis, error)
}

type QueryAnalyzer struct{}

type CachedQueryAnalyzer struct {
	analyzer *QueryAnalyzer
	cache    *lru.Cache
}

var nonProjectableFuncs = []string{
	"label_join",
	"label_replace",
}

// NewQueryAnalyzer creates a new QueryAnalyzer.
func NewQueryAnalyzer() *CachedQueryAnalyzer {
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
func (a *QueryAnalyzer) Analyze(query string) (QueryAnalysis, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nonProjectableQuery(), err
	}

	isProjectable := true
	var analysis QueryAnalysis
	parser.Inspect(expr, func(node parser.Node, nodes []parser.Node) error {
		switch n := node.(type) {
		case *parser.Call:
			if n.Func != nil && contains(n.Func.Name, nonProjectableFuncs) {
				isProjectable = false
				return fmt.Errorf("expressions with %s are not projectable", n.Func.Name)
			}
		case *parser.BinaryExpr:
			if n.VectorMatching != nil {
				if n.VectorMatching.On || len(n.VectorMatching.MatchingLabels) > 0 {
					analysis = analysis.scopeToLabels(n.VectorMatching.MatchingLabels, n.VectorMatching.On)
				}
			}
		case *parser.AggregateExpr:
			analysis = analysis.scopeToLabels(n.Grouping, !n.Without)
		}

		return nil
	})

	if !isProjectable {
		return nonProjectableQuery(), nil
	}

	return analysis, nil
}

func contains(needle string, haystack []string) bool {
	for _, item := range haystack {
		if needle == item {
			return true
		}
	}

	return false
}
