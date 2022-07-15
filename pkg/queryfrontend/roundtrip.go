// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	"github.com/thanos-io/thanos/internal/cortex/util/validation"
)

const (
	// labels used in metrics.
	rangeQueryOp   = "query_range"
	instantQueryOp = "query"
	labelNamesOp   = "label_names"
	labelValuesOp  = "label_values"
	seriesOp       = "series"
)

var labelValuesPattern = regexp.MustCompile("/api/v1/label/.+/values$")

// NewTripperware returns a Tripperware which sends requests to different sub tripperwares based on the query type.
func NewTripperware(config Config, reg prometheus.Registerer, logger log.Logger) (queryrange.Tripperware, error) {
	var (
		queryRangeLimits, labelsLimits queryrange.Limits
		err                            error
	)
	if config.QueryRangeConfig.Limits != nil {
		queryRangeLimits, err = validation.NewOverrides(*config.QueryRangeConfig.Limits, nil)
		if err != nil {
			return nil, errors.Wrap(err, "initialize query range limits")
		}
	}

	if config.LabelsConfig.Limits != nil {
		labelsLimits, err = validation.NewOverrides(*config.LabelsConfig.Limits, nil)
		if err != nil {
			return nil, errors.Wrap(err, "initialize labels limits")
		}
	}

	queryRangeCodec := NewThanosQueryRangeCodec(config.QueryRangeConfig.PartialResponseStrategy)
	labelsCodec := NewThanosLabelsCodec(config.LabelsConfig.PartialResponseStrategy, config.DefaultTimeRange)

	queryRangeTripperware, err := newQueryRangeTripperware(config.QueryRangeConfig, queryRangeLimits, queryRangeCodec,
		prometheus.WrapRegistererWith(prometheus.Labels{"tripperware": "query_range"}, reg), logger, config.ForwardHeaders)
	if err != nil {
		return nil, err
	}

	labelsTripperware, err := newLabelsTripperware(config.LabelsConfig, labelsLimits, labelsCodec,
		prometheus.WrapRegistererWith(prometheus.Labels{"tripperware": "labels"}, reg), logger, config.ForwardHeaders)
	if err != nil {
		return nil, err
	}

	return func(next http.RoundTripper) http.RoundTripper {
		return newRoundTripper(next, queryRangeTripperware(next), labelsTripperware(next), reg)
	}, nil
}

type roundTripper struct {
	next, queryRange, labels http.RoundTripper

	queriesCount *prometheus.CounterVec
}

func newRoundTripper(next, queryRange, metadata http.RoundTripper, reg prometheus.Registerer) roundTripper {
	r := roundTripper{
		next:       next,
		queryRange: queryRange,
		labels:     metadata,
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
		return r.labels.RoundTrip(req)
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
		case strings.HasSuffix(r.URL.Path, "/api/v1/series"):
			return seriesOp
		default:
			if labelValuesPattern.MatchString(r.URL.Path) {
				return labelValuesOp
			}
		}
	}

	return ""
}

// newQueryRangeTripperware returns a Tripperware for range queries configured with middlewares of
// limit, step align, downsampled, split by interval, cache requests and retry.
func newQueryRangeTripperware(
	config QueryRangeConfig,
	limits queryrange.Limits,
	codec *queryRangeCodec,
	reg prometheus.Registerer,
	logger log.Logger,
	forwardHeaders []string,
) (queryrange.Tripperware, error) {
	queryRangeMiddleware := []queryrange.Middleware{queryrange.NewLimitsMiddleware(limits)}
	m := queryrange.NewInstrumentMiddlewareMetrics(reg)

	// step align middleware.
	if config.AlignRangeWithStep {
		queryRangeMiddleware = append(
			queryRangeMiddleware,
			queryrange.InstrumentMiddleware("step_align", m),
			queryrange.StepAlignMiddleware,
		)
	}

	if config.RequestDownsampled {
		queryRangeMiddleware = append(
			queryRangeMiddleware,
			queryrange.InstrumentMiddleware("downsampled", m),
			DownsampledMiddleware(codec, reg),
		)
	}

	queryIntervalFn := func(_ queryrange.Request) time.Duration {
		return config.SplitQueriesByInterval
	}

	if config.SplitQueriesByInterval != 0 {
		queryRangeMiddleware = append(
			queryRangeMiddleware,
			queryrange.InstrumentMiddleware("split_by_interval", m),
			SplitByIntervalMiddleware(queryIntervalFn, limits, codec, reg),
		)
	}

	if config.ResultsCacheConfig != nil {
		queryCacheMiddleware, _, err := queryrange.NewResultsCacheMiddleware(
			logger,
			*config.ResultsCacheConfig,
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
			queryrange.InstrumentMiddleware("results_cache", m),
			queryCacheMiddleware,
		)
	}

	if config.MaxRetries > 0 {
		queryRangeMiddleware = append(
			queryRangeMiddleware,
			queryrange.InstrumentMiddleware("retry", m),
			queryrange.NewRetryMiddleware(logger, config.MaxRetries, queryrange.NewRetryMiddlewareMetrics(reg)),
		)
	}

	return func(next http.RoundTripper) http.RoundTripper {
		rt := queryrange.NewRoundTripper(next, codec, forwardHeaders, queryRangeMiddleware...)
		return queryrange.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			return rt.RoundTrip(r)
		})
	}, nil
}

// newLabelsTripperware returns a Tripperware for labels and series requests
// configured with middlewares of split by interval and retry.
func newLabelsTripperware(
	config LabelsConfig,
	limits queryrange.Limits,
	codec *labelsCodec,
	reg prometheus.Registerer,
	logger log.Logger,
	forwardHeaders []string,
) (queryrange.Tripperware, error) {
	labelsMiddleware := []queryrange.Middleware{}
	m := queryrange.NewInstrumentMiddlewareMetrics(reg)

	queryIntervalFn := func(_ queryrange.Request) time.Duration {
		return config.SplitQueriesByInterval
	}

	if config.SplitQueriesByInterval != 0 {
		labelsMiddleware = append(
			labelsMiddleware,
			queryrange.InstrumentMiddleware("split_interval", m),
			SplitByIntervalMiddleware(queryIntervalFn, limits, codec, reg),
		)
	}

	if config.ResultsCacheConfig != nil {
		queryCacheMiddleware, _, err := queryrange.NewResultsCacheMiddleware(
			logger,
			*config.ResultsCacheConfig,
			newThanosCacheKeyGenerator(config.SplitQueriesByInterval),
			limits,
			codec,
			ThanosResponseExtractor{},
			nil,
			shouldCache,
			reg,
		)
		if err != nil {
			return nil, errors.Wrap(err, "create results cache middleware")
		}

		labelsMiddleware = append(
			labelsMiddleware,
			queryrange.InstrumentMiddleware("results_cache", m),
			queryCacheMiddleware,
		)
	}

	if config.MaxRetries > 0 {
		labelsMiddleware = append(
			labelsMiddleware,
			queryrange.InstrumentMiddleware("retry", m),
			queryrange.NewRetryMiddleware(logger, config.MaxRetries, queryrange.NewRetryMiddlewareMetrics(reg)),
		)
	}
	return func(next http.RoundTripper) http.RoundTripper {
		rt := queryrange.NewRoundTripper(next, codec, forwardHeaders, labelsMiddleware...)
		return queryrange.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			return rt.RoundTrip(r)
		})
	}, nil
}

// shouldCache controls what kind of Thanos request should be cached.
// For more information about requests that skip caching logic, please visit
// the query-frontend documentation.
func shouldCache(r queryrange.Request) bool {
	if thanosReqStoreMatcherGettable, ok := r.(ThanosRequestStoreMatcherGetter); ok {
		if len(thanosReqStoreMatcherGettable.GetStoreMatchers()) > 0 {
			return false
		}
	}

	if thanosReqDedup, ok := r.(ThanosRequestDedup); ok {
		if !thanosReqDedup.IsDedupEnabled() {
			return false
		}
	}

	return !r.GetCachingOptions().Disabled
}
