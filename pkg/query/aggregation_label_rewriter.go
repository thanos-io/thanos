// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
)

// RewriterStrategy defines the strategy used by the AggregationLabelRewriter.
type RewriterStrategy string

const (
	// NoopLabelRewriter is a no-op strategy that basically disables the rewriter.
	NoopLabelRewriter RewriterStrategy = "noop"
	// UpsertLabelRewriter is a strategy that upserts the aggregation label.
	UpsertLabelRewriter RewriterStrategy = "upsert"
	// InsertOnlyLabelRewriter is a strategy that only inserts the aggregation label if it does not exist.
	InsertOnlyLabelRewriter RewriterStrategy = "insert-only"
)

const (
	aggregationLabelName = "__agg_rule_type__"
)

type AggregationLabelRewriter struct {
	logger            log.Logger
	metrics           *aggregationLabelRewriterMetrics
	strategy          RewriterStrategy
	desiredLabelValue string
}

type aggregationLabelRewriterMetrics struct {
	aggregationLabelRewriteSkippedCount    *prometheus.CounterVec
	aggregationLabelRewrittenCount         *prometheus.CounterVec
	aggregationLabelAddedCount             prometheus.Counter
	aggregationLabelRewriterRuntimeSeconds *prometheus.HistogramVec
}

func isAggregatedMetric(metricName string) bool {
	return strings.HasSuffix(metricName, ":aggr") ||
		strings.HasSuffix(metricName, ":avg") ||
		strings.HasSuffix(metricName, ":count") ||
		strings.HasSuffix(metricName, ":sum") ||
		strings.HasSuffix(metricName, ":min") ||
		strings.HasSuffix(metricName, ":max")
}

func newAggregationLabelRewriterMetrics(reg prometheus.Registerer, desiredLabelValue string) *aggregationLabelRewriterMetrics {
	m := &aggregationLabelRewriterMetrics{}

	m.aggregationLabelRewriteSkippedCount = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "skipped_total",
		Help: "Total number of times aggregation-label-rewriter was enabled but skipped",
	}, []string{"reason"})
	m.aggregationLabelRewrittenCount = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name:        "label_rewritten_total",
		Help:        "Total number of times aggregation-label-rewriter rewrote the aggregation label",
		ConstLabels: prometheus.Labels{"new_value": desiredLabelValue},
	}, []string{"old_value"})
	m.aggregationLabelAddedCount = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name:        "label_added_total",
		Help:        "Total number of times aggregation-label-rewriter added the aggregation label",
		ConstLabels: prometheus.Labels{"new_value": desiredLabelValue},
	})
	m.aggregationLabelRewriterRuntimeSeconds = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "total_runtime_seconds",
		Help:    "Runtime of aggregation-label-rewriter in seconds",
		Buckets: []float64{0.0001, 0.00025, 0.0005, 0.001, 0.005, 0.01, 0.1, 1.0, 10.0},
	}, []string{"is_rewritten"})

	return m
}

func NewNopAggregationLabelRewriter() *AggregationLabelRewriter {
	return &AggregationLabelRewriter{
		strategy: NoopLabelRewriter,
	}
}

func NewAggregationLabelRewriter(logger log.Logger, reg prometheus.Registerer, strategy RewriterStrategy, desiredLabelValue string) *AggregationLabelRewriter {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if desiredLabelValue == "" {
		strategy = NoopLabelRewriter
	}
	return &AggregationLabelRewriter{
		strategy:          strategy,
		logger:            logger,
		metrics:           newAggregationLabelRewriterMetrics(reg, desiredLabelValue),
		desiredLabelValue: desiredLabelValue,
	}
}

func (a *AggregationLabelRewriter) Rewrite(ms []*labels.Matcher) []*labels.Matcher {
	if a.strategy == NoopLabelRewriter {
		return ms
	}

	startOfRun := time.Now()
	needsRewrite := false
	skipReason := "no-name-matcher"
	var aggregationLabelMatcher *labels.Matcher
	aggregationLabelIndex := -1
	for i := 0; i < len(ms); i++ {
		// If we already know we need to rewrite, and we found the aggregation label
		// we don't need to loop further, break
		if needsRewrite && aggregationLabelMatcher != nil {
			break
		}
		m := ms[i]
		// If we are not sure if we need rewrite, and see a name matcher
		// We check if this is an aggregated metric and decide needsRewrite
		if !needsRewrite && m.Name == labels.MetricName {
			if m.Type == labels.MatchEqual {
				if isAggregatedMetric(m.Value) {
					needsRewrite = true
				} else {
					skipReason = "not-aggregated-metric"
				}
			} else {
				skipReason = "not-using-equal-name-match"
				level.Debug(a.logger).Log("msg", "skipped due to not using equal in name matcher", "matcher", m)
				break
			}
			// In any case, if we see an aggregation label, we store that for later use
		} else if m.Name == aggregationLabelName {
			aggregationLabelMatcher = m
			aggregationLabelIndex = i
		}
	}

	if aggregationLabelMatcher != nil && a.strategy == InsertOnlyLabelRewriter {
		needsRewrite = false
		skipReason = "insert-only"
	}

	// After the for loop, if needsRewrite is false, no need to do anything
	// but if it is true, we either append or modify an aggregation label
	if needsRewrite {
		newMatcher := &labels.Matcher{
			Name:  aggregationLabelName,
			Type:  labels.MatchRegexp,
			Value: a.desiredLabelValue,
		}
		if aggregationLabelMatcher != nil {
			a.metrics.aggregationLabelRewrittenCount.WithLabelValues(aggregationLabelMatcher.Value).Inc()
			ms[aggregationLabelIndex] = newMatcher
		} else {
			a.metrics.aggregationLabelAddedCount.Inc()
			ms = append(ms, newMatcher)
		}
		a.metrics.aggregationLabelRewriterRuntimeSeconds.WithLabelValues("true").Observe(
			time.Since(startOfRun).Seconds(),
		)
	} else {
		a.metrics.aggregationLabelRewriteSkippedCount.WithLabelValues(skipReason).Inc()
		a.metrics.aggregationLabelRewriterRuntimeSeconds.WithLabelValues("false").Observe(
			time.Since(startOfRun).Seconds(),
		)
	}

	return ms
}
