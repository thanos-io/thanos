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
	rangeQueryOp   = "query_range"
	instantQueryOp = "query"
	labelNamesOp   = "label_names"
	labelValuesOp  = "label_values"
	seriesOp       = "series"
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
	limits, err := validation.NewOverrides(*config.CortexLimits, nil)
	if err != nil {
		return nil, errors.Wrap(err, "initialize limits")
	}

	queryRangeCodec := NewThanosQueryRangeCodec(config.PartialResponseStrategy)
	metadataCodec := NewThanosMetadataCodec(config.PartialResponseStrategy)

	//if config.SplitQueriesByInterval != 0 {
	//	// TODO(yeya24): make interval dynamic in next pr.
	//	queryIntervalFn := func(_ queryrange.Request) time.Duration {
	//		return config.SplitQueriesByInterval
	//	}
	//	splitIntervalMiddleware = queryrange.SplitByIntervalMiddleware(queryIntervalFn, limits, codec, reg)
	//}
	//
	//if config.MaxRetries > 0 {
	//	retryMiddleware = queryrange.NewRetryMiddleware(logger, config.MaxRetries, queryrange.NewRetryMiddlewareMetrics(reg))
	//}

	queryRangeTripperware, err := newQueryRangeTripperware(config, limits, queryRangeCodec, prometheus.WrapRegistererWith(prometheus.Labels{"tripperware": "query_range"}, reg), logger)
	if err != nil {
		return nil, err
	}

	metadataTripperware, err := newMetadataTripperware(config, limits, metadataCodec, prometheus.WrapRegistererWith(prometheus.Labels{"tripperware": "metadata"}, reg), logger)
	if err != nil {
		return nil, err
	}

	return func(next http.RoundTripper) http.RoundTripper {
		return newRoundTripper(next, queryRangeTripperware(next), metadataTripperware(next), reg)
	}, nil
}

type roundTripper struct {
	next, queryRange, metadata http.RoundTripper

	queriesCount *prometheus.CounterVec
}

func newRoundTripper(next, queryRange, metadata http.RoundTripper, reg prometheus.Registerer) roundTripper {
	r := roundTripper{
		next:       next,
		queryRange: queryRange,
		metadata:   metadata,
		queriesCount: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_query_frontend_queries_total",
			Help: "Total queries passing through query frontend",
		}, []string{"op"}),
	}

	r.queriesCount.WithLabelValues(instantQueryOp)
	r.queriesCount.WithLabelValues(rangeQueryOp)
	r.queriesCount.WithLabelValues(labelNamesOp)
	r.queriesCount.WithLabelValues(labelValuesOp)
	r.queriesCount.WithLabelValues(seriesOp)
	return r
}

func (r roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	switch op := getOperation(req); op {
	case instantQueryOp:
		r.queriesCount.WithLabelValues(instantQueryOp).Inc()
	case rangeQueryOp:
		r.queriesCount.WithLabelValues(rangeQueryOp).Inc()
		return r.queryRange.RoundTrip(req)
	case labelNamesOp, labelValuesOp, seriesOp:
		r.queriesCount.WithLabelValues(op).Inc()
		return r.metadata.RoundTrip(req)
	default:
	}

	return r.next.RoundTrip(req)
}

func getOperation(r *http.Request) string {
	if r.Method == http.MethodGet || r.Method == http.MethodPost {
		switch {
		case strings.HasSuffix(r.URL.Path, "/api/v1/query"):
			return instantQueryOp
		case strings.HasSuffix(r.URL.Path, "/api/v1/query_range"):
			return rangeQueryOp
		case strings.HasSuffix(r.URL.Path, "/api/v1/labels"):
			return labelNamesOp
		case strings.HasSuffix(r.URL.Path, "/values") && strings.Contains(r.URL.Path, "/api/v1/label"):
			return labelValuesOp
		case strings.HasSuffix(r.URL.Path, "/api/v1/series"):
			return seriesOp
		}
	}

	return ""
}

func newQueryRangeTripperware(
	config Config,
	limits queryrange.Limits,
	codec *queryRangeCodec,
	reg prometheus.Registerer,
	logger log.Logger,
) (frontend.Tripperware, error) {
	queryRangeMiddleware := []queryrange.Middleware{queryrange.LimitsMiddleware(limits)}
	instrumentMetrics := queryrange.NewInstrumentMiddlewareMetrics(reg)

	// step align middleware.
	queryRangeMiddleware = append(
		queryRangeMiddleware,
		queryrange.InstrumentMiddleware("step_align", instrumentMetrics),
		queryrange.StepAlignMiddleware,
	)

	queryIntervalFn := func(_ queryrange.Request) time.Duration {
		return config.SplitQueriesByInterval
	}

	if config.SplitQueriesByInterval != 0 {
		queryRangeMiddleware = append(
			queryRangeMiddleware,
			queryrange.InstrumentMiddleware("split_by_interval", instrumentMetrics),
			SplitByIntervalMiddleware(queryIntervalFn, limits, codec, reg),
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
			queryrange.InstrumentMiddleware("results_cache", instrumentMetrics),
			queryCacheMiddleware,
		)
	}

	if config.MaxRetries > 0 {
		queryRangeMiddleware = append(
			queryRangeMiddleware,
			queryrange.InstrumentMiddleware("retry", instrumentMetrics),
			queryrange.NewRetryMiddleware(logger, config.MaxRetries, queryrange.NewRetryMiddlewareMetrics(reg)),
		)
	}

	return func(next http.RoundTripper) http.RoundTripper {
		rt := queryrange.NewRoundTripper(next, codec, queryRangeMiddleware...)
		return frontend.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			return rt.RoundTrip(r)
		})
	}, nil
}

func newMetadataTripperware(
	config Config,
	limits queryrange.Limits,
	codec *metadataCodec,
	reg prometheus.Registerer,
	logger log.Logger,
) (frontend.Tripperware, error) {
	// metadata query don't have the limits middleware.
	metadataMiddleware := []queryrange.Middleware{queryrange.LimitsMiddleware(limits)}
	instrumentMetrics := queryrange.NewInstrumentMiddlewareMetrics(reg)

	queryIntervalFn := func(_ queryrange.Request) time.Duration {
		return config.SplitQueriesByInterval
	}

	metadataMiddleware = append(
		metadataMiddleware,
		queryrange.InstrumentMiddleware("split_interval", instrumentMetrics),
		SplitByIntervalMiddleware(queryIntervalFn, limits, codec, reg),
	)

	if config.MaxRetries > 0 {
		metadataMiddleware = append(
			metadataMiddleware,
			queryrange.InstrumentMiddleware("retry", instrumentMetrics),
			queryrange.NewRetryMiddleware(logger, config.MaxRetries, queryrange.NewRetryMiddlewareMetrics(reg)),
		)
	}
	return func(next http.RoundTripper) http.RoundTripper {
		rt := queryrange.NewRoundTripper(next, codec, metadataMiddleware...)
		return frontend.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			return rt.RoundTrip(r)
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
