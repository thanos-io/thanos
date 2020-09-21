// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"net/http"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/cortexproject/cortex/pkg/util/validation"

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

// NewTripperware returns a Tripperware configured with middlewares to
// limit, align, split,cache requests and retry.
// Not using the cortex one as it uses  query parallelisations based on
// storage sharding configuration and query ASTs.
func NewTripperware(
	config Config,
	reg prometheus.Registerer,
	logger log.Logger,
) (frontend.Tripperware, error) {

	queriesCount := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_query_frontend_queries_total",
		Help: "Total queries passing through query frontend",
	}, []string{"op"})
	queriesCount.WithLabelValues(labelQuery)
	queriesCount.WithLabelValues(labelQueryRange)

	limits, err := validation.NewOverrides(*config.CortexLimits, nil)
	if err != nil {
		return nil, errors.Wrap(err, "initialize limits")
	}

	metrics := queryrange.NewInstrumentMiddlewareMetrics(reg)
	queryRangeMiddleware := []queryrange.Middleware{queryrange.LimitsMiddleware(limits)}

	// step align middleware.
	queryRangeMiddleware = append(
		queryRangeMiddleware,
		queryrange.InstrumentMiddleware("step_align", metrics),
		queryrange.StepAlignMiddleware,
	)

	codec := NewThanosCodec(config.PartialResponseStrategy)

	if config.SplitQueriesByInterval != 0 {
		// TODO(yeya24): make interval dynamic in next pr.
		queryIntervalFn := func(_ queryrange.Request) time.Duration {
			return config.SplitQueriesByInterval
		}
		queryRangeMiddleware = append(
			queryRangeMiddleware,
			queryrange.InstrumentMiddleware("split_by_interval", metrics),
			queryrange.SplitByIntervalMiddleware(queryIntervalFn, limits, codec, reg),
		)
	}

	if config.CortexResultsCacheConfig != nil {
		queryCacheMiddleware, _, err := queryrange.NewResultsCacheMiddleware(
			logger,
			*config.CortexResultsCacheConfig,
			newThanosCacheKeyGenerator(config.SplitQueriesByInterval),
			limits,
			codec,
			queryrange.PrometheusResponseExtractor{},
			nil,
			shouldCache,
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

	if config.MaxRetries > 0 {
		queryRangeMiddleware = append(
			queryRangeMiddleware,
			queryrange.InstrumentMiddleware("retry", metrics),
			queryrange.NewRetryMiddleware(logger, config.MaxRetries, queryrange.NewRetryMiddlewareMetrics(reg)),
		)
	}

	return func(next http.RoundTripper) http.RoundTripper {
		queryRangeTripper := queryrange.NewRoundTripper(next, codec, queryRangeMiddleware...)
		return frontend.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/api/v1/query") {
				if r.Method == http.MethodGet || r.Method == http.MethodPost {
					queriesCount.WithLabelValues(labelQuery).Inc()
				}
			} else if strings.HasSuffix(r.URL.Path, "/api/v1/query_range") {
				if r.Method == http.MethodGet || r.Method == http.MethodPost {
					queriesCount.WithLabelValues(labelQueryRange).Inc()
					return queryRangeTripper.RoundTrip(r)
				}
			}
			return next.RoundTrip(r)
		})
	}, nil
}

// Don't go to response cache if StoreMatchers are set.
func shouldCache(r queryrange.Request) bool {
	if thanosReq, ok := r.(*ThanosRequest); ok {
		if len(thanosReq.StoreMatchers) > 0 {
			return false
		}
	}

	return !r.GetCachingOptions().Disabled
}
