// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"net/http"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	// labels used in metrics.
	labelQuery      = "query"
	labelQueryRange = "query_range"
)

func NewTripperWare(
	limits queryrange.Limits,
	cacheConfig *queryrange.ResultsCacheConfig,
	codec queryrange.Codec,
	cacheExtractor queryrange.Extractor,
	splitQueryInterval time.Duration,
	maxRetries int,
	reg prometheus.Registerer,
	logger log.Logger,
) (frontend.Tripperware, error) {

	queriesCount := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_query_frontend_queries_total",
		Help: "Total queries passing through query frontend",
	}, []string{"op"})
	queriesCount.WithLabelValues(labelQuery)
	queriesCount.WithLabelValues(labelQueryRange)

	metrics := queryrange.NewInstrumentMiddlewareMetrics(reg)
	queryRangeMiddleware := []queryrange.Middleware{queryrange.LimitsMiddleware(limits)}

	// step align middleware.
	queryRangeMiddleware = append(
		queryRangeMiddleware,
		queryrange.InstrumentMiddleware("step_align", metrics),
		queryrange.StepAlignMiddleware,
	)

	if splitQueryInterval != 0 {
		queryRangeMiddleware = append(
			queryRangeMiddleware,
			queryrange.InstrumentMiddleware("split_by_interval", metrics),
			queryrange.SplitByIntervalMiddleware(splitQueryInterval, limits, codec, reg),
		)
	}

	if cacheConfig != nil {
		// constSplitter will panic when splitQueryInterval is 0.
		if splitQueryInterval == 0 {
			return nil, errors.New("cannot create results cache middleware when split interval is 0")
		}

		queryCacheMiddleware, _, err := queryrange.NewResultsCacheMiddleware(
			logger,
			*cacheConfig,
			newThanosCacheKeyGenerator(splitQueryInterval),
			limits,
			codec,
			cacheExtractor,
			nil,
			reg,
		)
		if err != nil {
			return nil, errors.Wrap(err, "create results cache middleware")
		}

		queryRangeMiddleware = append(
			queryRangeMiddleware,
			queryrange.InstrumentMiddleware("results_cache", metrics),
			queryCacheMiddleware,
		)
	}

	if maxRetries > 0 {
		queryRangeMiddleware = append(
			queryRangeMiddleware,
			queryrange.InstrumentMiddleware("retry", metrics),
			queryrange.NewRetryMiddleware(logger, maxRetries, queryrange.NewRetryMiddlewareMetrics(reg)),
		)
	}

	return func(next http.RoundTripper) http.RoundTripper {
		queryRangeTripper := queryrange.NewRoundTripper(next, codec, queryRangeMiddleware...)
		return frontend.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch r.URL.Path {
			case "/api/v1/query":
				if r.Method == http.MethodGet || r.Method == http.MethodPost {
					queriesCount.WithLabelValues(labelQuery).Inc()
				}
			case "/api/v1/query_range":
				if r.Method == http.MethodGet || r.Method == http.MethodPost {
					queriesCount.WithLabelValues(labelQueryRange).Inc()
					return queryRangeTripper.RoundTrip(r)
				}
			default:
			}
			return next.RoundTrip(r)
		})
	}, nil
}
