// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"context"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	"github.com/thanos-io/thanos/pkg/queryprojection"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// PromQLProjectionMiddleware creates a new Middleware that shards PromQL aggregations using grouping labels.
func PromQLProjectionMiddleware(analyzer queryprojection.Analyzer) queryrange.Middleware {
	return queryrange.MiddlewareFunc(func(next queryrange.Handler) queryrange.Handler {
		return queryProjection{next: next, queryAnalyzer: analyzer}
	})
}

type queryProjection struct {
	next          queryrange.Handler
	queryAnalyzer queryprojection.Analyzer
}

func (s queryProjection) Do(ctx context.Context, r queryrange.Request) (queryrange.Response, error) {
	tr, ok := r.(ProjectionRequest)
	if !ok {
		return s.next.Do(ctx, r)
	}

	analysis, err := s.queryAnalyzer.Analyze(r.GetQuery())
	if err != nil {
		return s.next.Do(ctx, r)
	}

	r = tr.WithProjectionInfo(&storepb.ProjectionInfo{
		Grouping: analysis.Grouping(),
		By:       analysis.By(),
		Labels:   analysis.Labels(),
	})
	return s.next.Do(ctx, r)
}
