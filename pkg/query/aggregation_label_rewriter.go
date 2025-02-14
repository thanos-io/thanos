package query

import (
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gobwas/glob"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	aggregationLabelName            = "__rollup__"
	aggregatedMetricNameGlobPattern = "*:(aggr|sum|count|avg|min|max)"
)

type AggregationLabelRewriter struct {
	logger  log.Logger
	metrics *aggregationLabelRewriterMetrics

	enabled           bool
	desiredLabelValue string

	aggregatedMetricNameGlob glob.Glob
}

type aggregationLabelRewriterMetrics struct {
	aggregationLabelRewriteSkippedCount    *prometheus.CounterVec
	aggregationLabelRewrittenCount         *prometheus.CounterVec
	aggregationLabelAddedCount             prometheus.Counter
	aggregationLabelRewriterRuntimeSeconds *prometheus.HistogramVec
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
		Buckets: prometheus.DefBuckets,
	}, []string{"is_rewritten"})

	return m
}

func NewAggregationLabelRewriter(logger log.Logger, reg prometheus.Registerer, desiredLabelValue string) *AggregationLabelRewriter {
	g := glob.MustCompile(aggregatedMetricNameGlobPattern)
	return &AggregationLabelRewriter{
		enabled:                  desiredLabelValue != "",
		logger:                   logger,
		metrics:                  newAggregationLabelRewriterMetrics(reg, desiredLabelValue),
		desiredLabelValue:        desiredLabelValue,
		aggregatedMetricNameGlob: g,
	}
}

func (a *AggregationLabelRewriter) Rewrite(ms []*labels.Matcher) []*labels.Matcher {
	if a.enabled {
		startOfRun := time.Now()
		needsRewrite := false
		skipReason := "no-name-matcher"
		var aggregationLabelMatcher *labels.Matcher
		for i := 0; i < len(ms); i++ {
			m := ms[i]
			if !needsRewrite && m.Name == labels.MetricName {
				if m.Type == labels.MatchEqual {
					if a.aggregatedMetricNameGlob.Match(m.Value) {
						needsRewrite = true
					} else {
						skipReason = "not-aggregated-metric"
					}
				} else {
					skipReason = "not-using-equal-name-match"
					level.Debug(a.logger).Log("msg", "skipped due to not using equal in name matcher", "matcher", m)
					break
				}
			} else if needsRewrite && aggregationLabelMatcher != nil {
				break
			} else if m.Name == aggregationLabelName {
				aggregationLabelMatcher = m
			}
		}
		if needsRewrite {
			if aggregationLabelMatcher != nil {
				a.metrics.aggregationLabelRewrittenCount.WithLabelValues(aggregationLabelMatcher.Value).Inc()
				aggregationLabelMatcher.Type = labels.MatchEqual
				aggregationLabelMatcher.Value = a.desiredLabelValue
			} else {
				a.metrics.aggregationLabelAddedCount.Inc()
				newMatcher := &labels.Matcher{
					Name:  aggregationLabelName,
					Type:  labels.MatchEqual,
					Value: a.desiredLabelValue,
				}
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

	}
	return ms
}
