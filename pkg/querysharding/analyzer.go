// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package querysharding

import (
	"fmt"
	"os"

	"github.com/go-kit/log"
	lru "github.com/hashicorp/golang-lru"
	"github.com/prometheus/prometheus/promql/parser"
)

// QueryAnalyzer is an analyzer which determines
// whether a PromQL Query is shardable and using which labels.
type QueryAnalyzer struct{}

var LRU *lru.Cache
var logger log.Logger

var nonShardableFuncs = []string{
	"label_join",
	"label_replace",
}

// NewQueryAnalyzer creates a new QueryAnalyzer.
func NewQueryAnalyzer() *QueryAnalyzer {
	inIT() // Initializing cache
	return &QueryAnalyzer{}
}

func inIT() {
	lruCache, err := lru.New(256)

	if err != nil {
		logger.Log("Error: ", err)
	}

	LRU = lruCache

	logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
}

type cachedValue struct {
	QueryAnalysis QueryAnalysis
	err           error
}

// Analyze analyzes a query and returns a QueryAnalysis.

// Analyze uses the following algorithm:
//   - if a query has subqueries, such as label_join or label_replace,
//     or has functions which cannot be sharded, then treat the query as non shardable.
//   - if the query's root expression has grouping labels,
//     then treat the query as shardable by those labels.
//   - if the query's root expression has no grouping labels,
//     then walk the query and find the least common labelset
//     used in grouping expressions. If non-empty, treat the query
//     as shardable by those labels.
//   - otherwise, treat the query as non-shardable.
//
// The le label is excluded from sharding.
func (a *QueryAnalyzer) Analyze(query string) (QueryAnalysis, error) {
	nonShardableQuery := nonShardableQuery()

	if LRU.Len() == 0 {
		inIT()
	}

	var queryResult cachedValue

	if LRU.Len() != 0 && LRU.Contains(query) {
		value, ok := LRU.Get(query)

		if !ok {
			logger.Log("FetchingError=", query)
		} else {
			queryResult = value.(cachedValue)
			return queryResult.QueryAnalysis, queryResult.err
		}
	}

	expr, err := parser.ParseExpr(query)
	if err != nil {
		if ok := LRU.Add(query, cachedValue{
			QueryAnalysis: nonShardableQuery,
			err:           err,
		}); ok {
			logger.Log("parsingError=", query)
		}
		return nonShardableQuery, err
	}

	isShardable := true
	var analysis QueryAnalysis
	parser.Inspect(expr, func(node parser.Node, nodes []parser.Node) error {
		switch n := node.(type) {
		case *parser.SubqueryExpr:
			isShardable = false
			return fmt.Errorf("expressions with subqueries are not shardable")
		case *parser.Call:
			if n.Func != nil && contains(n.Func.Name, nonShardableFuncs) {
				isShardable = false
				return fmt.Errorf("expressions with %s are not shardable", n.Func.Name)
			}
		case *parser.BinaryExpr:
			if n.VectorMatching != nil {
				shardingLabels := without(n.VectorMatching.MatchingLabels, []string{"le"})
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

	if !isShardable {

		if ok := LRU.Add(query, cachedValue{
			QueryAnalysis: nonShardableQuery,
			err:           nil,
		}); ok {
			logger.Log("cachingError=", query)
		}
		return nonShardableQuery, nil
	}

	rootAnalysis := analyzeRootExpression(expr)
	if rootAnalysis.IsShardable() && rootAnalysis.shardBy {
		if ok := LRU.Add(query, cachedValue{
			QueryAnalysis: rootAnalysis,
			err:           nil,
		}); ok {
			logger.Log("cachingError=", query)
		}
		return rootAnalysis, nil
	}

	if ok := LRU.Add(query, cachedValue{
		QueryAnalysis: analysis,
		err:           nil,
	}); ok {
		logger.Log("cachingError=", query)
	}

	return analysis, nil
}

func analyzeRootExpression(node parser.Node) QueryAnalysis {
	switch n := node.(type) {
	case *parser.BinaryExpr:
		if n.VectorMatching != nil && n.VectorMatching.On {
			shardingLabels := without(n.VectorMatching.MatchingLabels, []string{"le"})
			return newShardableByLabels(shardingLabels, n.VectorMatching.On)
		} else {
			return nonShardableQuery()
		}
	case *parser.AggregateExpr:
		if len(n.Grouping) == 0 {
			return nonShardableQuery()
		}

		shardingLabels := without(n.Grouping, []string{"le"})
		return newShardableByLabels(shardingLabels, !n.Without)
	}

	return nonShardableQuery()
}

func contains(needle string, haystack []string) bool {
	for _, item := range haystack {
		if needle == item {
			return true
		}
	}

	return false
}
