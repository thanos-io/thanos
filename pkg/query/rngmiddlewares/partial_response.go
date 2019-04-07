package rngmiddlewares

import (
	"context"
	"github.com/cortexproject/cortex/pkg/querier/frontend/queryrange"
	"github.com/prometheus/client_golang/prometheus"
)

func PartialResponse(name string, queryRangeDuration *prometheus.HistogramVec) queryrange.Middleware {
	return queryrange.MiddlewareFunc(func(next queryrange.Handler) queryrange.Handler {
		return queryrange.HandlerFunc(func(ctx context.Context, req *queryrange.Request) (*queryrange.APIResponse, error) {


			// TODO: Parse partial response options etx etc
			// TODO Add other middlewares.

			return next.Do(ctx, req)
		})
	})
}
