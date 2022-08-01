// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// This is a modified copy from
// https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/split_by_interval.go.

package queryfrontend

import (
	"context"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"

	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// PromQLShardingMiddleware creates a new Middleware that shards PromQL aggregations using grouping labels.
func PromQLShardingMiddleware(queryAnalyzer *querysharding.QueryAnalyzer, numShards int, limits queryrange.Limits, merger queryrange.Merger) queryrange.Middleware {
	return queryrange.MiddlewareFunc(func(next queryrange.Handler) queryrange.Handler {
		return querySharder{
			next:          next,
			limits:        limits,
			queryAnalyzer: queryAnalyzer,
			numShards:     numShards,
			merger:        merger,
		}
	})
}

type querySharder struct {
	next   queryrange.Handler
	limits queryrange.Limits

	queryAnalyzer *querysharding.QueryAnalyzer
	numShards     int
	merger        queryrange.Merger
}

func (s querySharder) Do(ctx context.Context, r queryrange.Request) (queryrange.Response, error) {
	analysis, err := s.queryAnalyzer.Analyze(r.GetQuery())
	if err != nil {
		return nil, err
	}

	if !analysis.IsShardable() {
		return s.next.Do(ctx, r)
	}

	reqs := s.shardQuery(r, analysis)

	reqResps, err := queryrange.DoRequests(ctx, s.next, reqs, s.limits)
	if err != nil {
		return nil, err
	}

	resps := make([]queryrange.Response, 0, len(reqResps))
	for _, reqResp := range reqResps {
		resps = append(resps, reqResp.Response)
	}

	response, err := s.merger.MergeResponse(resps...)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (s querySharder) shardQuery(r queryrange.Request, analysis querysharding.QueryAnalysis) []queryrange.Request {
	tr, ok := r.(*ThanosQueryRangeRequest)
	if !ok {
		return []queryrange.Request{tr}
	}

	reqs := make([]queryrange.Request, s.numShards)
	for i := 0; i < s.numShards; i++ {
		reqs[i] = tr.WithShardInfo(&storepb.ShardInfo{
			TotalShards: int64(s.numShards),
			ShardIndex:  int64(i),
			By:          analysis.ShardBy(),
			Labels:      analysis.ShardingLabels(),
		})
	}

	return reqs
}
