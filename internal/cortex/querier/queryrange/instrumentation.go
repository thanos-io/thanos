// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/instrument"
)

const DAY = 24 * time.Hour
const queryRangeBucket = "query_range_bucket"

// InstrumentMiddleware can be inserted into the middleware chain to expose timing information.
func InstrumentMiddleware(name string, metrics *InstrumentMiddlewareMetrics) Middleware {

	var durationCol instrument.Collector
	// Support the case metrics shouldn't be tracked (ie. unit tests).
	if metrics != nil {
		durationCol = NewDurationHistogramCollector(metrics.duration)
	} else {
		durationCol = &NoopCollector{}
	}

	return MiddlewareFunc(func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, req Request) (Response, error) {
			queryRangeDurationBucket := getRangeBucket(req)
			ctx = context.WithValue(ctx, queryRangeBucket, queryRangeDurationBucket)
			var resp Response
			err := instrument.CollectedRequest(ctx, name, durationCol, instrument.ErrorCode, func(ctx context.Context) error {
				var err error
				resp, err = next.Do(ctx, req)
				return err
			})
			return resp, err
		})
	})
}

func getRangeBucket(req Request) string {
	queryRangeDuration := req.GetEnd() - req.GetStart()
	switch {
	case queryRangeDuration < 0:
		return "Invalid"
	case queryRangeDuration <= time.Hour.Milliseconds():
		return "1h"
	case queryRangeDuration <= 6*time.Hour.Milliseconds():
		return "6h"
	case queryRangeDuration <= 12*time.Hour.Milliseconds():
		return "12h"
	case queryRangeDuration <= DAY.Milliseconds():
		return "1d"
	case queryRangeDuration <= 2*DAY.Milliseconds():
		return "2d"
	case queryRangeDuration <= 7*DAY.Milliseconds():
		return "7d"
	case queryRangeDuration <= 30*DAY.Milliseconds():
		return "30d"
	default:
		return "+INF"
	}
}

// InstrumentMiddlewareMetrics holds the metrics tracked by InstrumentMiddleware.
type InstrumentMiddlewareMetrics struct {
	duration *prometheus.HistogramVec
}

// NewInstrumentMiddlewareMetrics makes a new InstrumentMiddlewareMetrics.
func NewInstrumentMiddlewareMetrics(registerer prometheus.Registerer) *InstrumentMiddlewareMetrics {
	return &InstrumentMiddlewareMetrics{
		duration: promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "frontend_query_range_duration_seconds",
			Help:      "Total time spent in seconds doing query range requests.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "status_code", queryRangeBucket}),
	}
}

// NoopCollector is a noop collector that can be used as placeholder when no metric
// should tracked by the instrumentation.
type NoopCollector struct{}

// Register implements instrument.Collector.
func (c *NoopCollector) Register() {}

// Before implements instrument.Collector.
func (c *NoopCollector) Before(ctx context.Context, method string, start time.Time) {}

// After implements instrument.Collector.
func (c *NoopCollector) After(ctx context.Context, method, statusCode string, start time.Time) {}

// DurationHistogramCollector collects the duration of a request
type DurationHistogramCollector struct {
	metric *prometheus.HistogramVec
}

func (c *DurationHistogramCollector) Register() {
	prometheus.MustRegister(c.metric)
}

func (c *DurationHistogramCollector) Before(ctx context.Context, method string, start time.Time) {
}

func (c *DurationHistogramCollector) After(ctx context.Context, method, statusCode string, start time.Time) {
	durationBucket, ok := ctx.Value(queryRangeBucket).(string)

	if !ok {
		durationBucket = "null"
	}
	if c.metric != nil {
		instrument.ObserveWithExemplar(ctx, c.metric.WithLabelValues(method, statusCode, durationBucket), time.Since(start).Seconds())
	}
}

func NewDurationHistogramCollector(metric *prometheus.HistogramVec) *DurationHistogramCollector {
	return &DurationHistogramCollector{metric}
}
