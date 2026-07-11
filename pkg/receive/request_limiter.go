// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/histogram"

	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

const (
	seriesLimitName    = "series"
	samplesLimitName   = "samples"
	sizeBytesLimitName = "body_size"
)

var unlimitedRequestLimitsConfig = NewEmptyRequestLimitsConfig().
	SetSizeBytesLimit(0).
	SetSeriesLimit(0).
	SetSamplesLimit(0).
	SetNativeHistogramBucketsLimit(0)

// configRequestLimiter implements requestLimiter interface.
type configRequestLimiter struct {
	tenantLimits        map[string]*requestLimitsConfig
	cachedDefaultLimits *requestLimitsConfig
	limitsHit           *prometheus.SummaryVec
	configuredLimits    *prometheus.GaugeVec
}

func newConfigRequestLimiter(reg prometheus.Registerer, writeLimits *WriteLimitsConfig) *configRequestLimiter {
	// Merge the default limits configuration with an unlimited configuration
	// to ensure the nils are overwritten with zeroes.
	defaultRequestLimits := writeLimits.DefaultLimits.RequestLimits.OverlayWith(unlimitedRequestLimitsConfig)

	// Load up the request limits into a map with the tenant name as key and
	// merge with the defaults to provide easy and fast access when checking
	// limits.
	// The merge with the default happen because a tenant limit that isn't
	// present means the value is inherited from the default configuration.
	tenantsLimits := writeLimits.TenantsLimits
	tenantRequestLimits := make(map[string]*requestLimitsConfig)
	for tenant, limitConfig := range tenantsLimits {
		if limitConfig.RequestLimits != nil {
			tenantRequestLimits[tenant] = limitConfig.RequestLimits.OverlayWith(defaultRequestLimits)
		}
	}

	limiter := configRequestLimiter{
		tenantLimits:        tenantRequestLimits,
		cachedDefaultLimits: defaultRequestLimits,
	}
	limiter.registerMetrics(reg)
	return &limiter
}

func (l *configRequestLimiter) registerMetrics(reg prometheus.Registerer) {
	l.limitsHit = promauto.With(reg).NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:  "thanos",
			Subsystem:  "receive",
			Name:       "write_limits_hit",
			Help:       "Summary of how much beyond the limit a refused remote write request was.",
			Objectives: map[float64]float64{0.50: 0.1, 0.95: 0.1, 0.99: 0.001},
		}, []string{"tenant", "limit"},
	)
	l.configuredLimits = promauto.With(reg).NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "thanos",
			Subsystem: "receive",
			Name:      "write_limits",
			Help:      "The configured write limits.",
		}, []string{"tenant", "limit"},
	)
	for tenant, limits := range l.tenantLimits {
		l.configuredLimits.WithLabelValues(tenant, sizeBytesLimitName).Set(float64(*limits.SizeBytesLimit))
		l.configuredLimits.WithLabelValues(tenant, seriesLimitName).Set(float64(*limits.SeriesLimit))
		l.configuredLimits.WithLabelValues(tenant, samplesLimitName).Set(float64(*limits.SamplesLimit))
	}
	l.configuredLimits.WithLabelValues("", sizeBytesLimitName).Set(float64(*l.cachedDefaultLimits.SizeBytesLimit))
	l.configuredLimits.WithLabelValues("", seriesLimitName).Set(float64(*l.cachedDefaultLimits.SeriesLimit))
	l.configuredLimits.WithLabelValues("", samplesLimitName).Set(float64(*l.cachedDefaultLimits.SamplesLimit))
}

func (l *configRequestLimiter) AllowSizeBytes(tenant string, contentLengthBytes int64) bool {
	limit := l.limitsFor(tenant).SizeBytesLimit
	if *limit <= 0 {
		return true
	}

	allowed := *limit >= contentLengthBytes
	if !allowed {
		l.limitsHit.
			WithLabelValues(tenant, sizeBytesLimitName).
			Observe(float64(contentLengthBytes - *limit))
	}
	return allowed
}

func (l *configRequestLimiter) AllowSeries(tenant string, amount int64) bool {
	limit := l.limitsFor(tenant).SeriesLimit
	if *limit <= 0 {
		return true
	}

	allowed := *limit >= amount
	if !allowed && l.limitsHit != nil {
		l.limitsHit.
			WithLabelValues(tenant, seriesLimitName).
			Observe(float64(amount - *limit))
	}
	return allowed
}

func (l *configRequestLimiter) AllowSamples(tenant string, amount int64) bool {
	limit := l.limitsFor(tenant).SamplesLimit
	if *limit <= 0 {
		return true
	}
	allowed := *limit >= amount
	if !allowed && l.limitsHit != nil {
		l.limitsHit.
			WithLabelValues(tenant, samplesLimitName).
			Observe(float64(amount - *limit))
	}
	return allowed
}

func (l *configRequestLimiter) AllowNativeHistogram(tenant string, hp prompb.Histogram) (prompb.Histogram, bool) {
	limit := l.limitsFor(tenant).NativeHistogramBucketsLimit
	if *limit <= 0 {
		return hp, true
	}

	var (
		exceedLimit bool
	)
	// TODO: Increment limit hit metrics for native histogram buckets.
	// Existing metric is a summary not a counter.
	if hp.IsFloatHistogram() {
		exceedLimit = int64(len(hp.PositiveCounts)+len(hp.NegativeCounts)) > *limit
		if !exceedLimit {
			return hp, true
		}
		// Exceed limit and there is no way to reduce resolution further.
		if hp.Schema <= histogram.ExponentialSchemaMin {
			return prompb.Histogram{}, false
		}
		fh := prompb.FloatHistogramProtoToFloatHistogram(hp)
		for int64(len(fh.PositiveBuckets)+len(fh.NegativeBuckets)) > *limit {
			if fh.Schema <= histogram.ExponentialSchemaMin {
				return prompb.Histogram{}, false
			}
			fh = fh.ReduceResolution(fh.Schema - 1)
		}
		return prompb.FloatHistogramToHistogramProto(hp.Timestamp, fh), true
	}

	exceedLimit = int64(len(hp.PositiveDeltas)+len(hp.NegativeDeltas)) > *limit
	if !exceedLimit {
		return hp, true
	}
	// Exceed limit and there is no way to reduce resolution further.
	if hp.Schema <= histogram.ExponentialSchemaMin {
		return prompb.Histogram{}, false
	}
	h := prompb.HistogramProtoToHistogram(hp)
	for int64(len(h.PositiveBuckets)+len(h.NegativeBuckets)) > *limit {
		if h.Schema <= histogram.ExponentialSchemaMin {
			return prompb.Histogram{}, false
		}
		h = h.ReduceResolution(h.Schema - 1)
	}
	return prompb.HistogramToHistogramProto(hp.Timestamp, h), true
}

func (l *configRequestLimiter) limitsFor(tenant string) *requestLimitsConfig {
	limits, ok := l.tenantLimits[tenant]
	if !ok {
		limits = l.cachedDefaultLimits
	}
	return limits
}

type noopRequestLimiter struct{}

func (l *noopRequestLimiter) AllowSizeBytes(tenant string, contentLengthBytes int64) bool {
	return true
}

func (l *noopRequestLimiter) AllowSeries(tenant string, amount int64) bool {
	return true
}

func (l *noopRequestLimiter) AllowSamples(tenant string, amount int64) bool {
	return true
}

func (l *noopRequestLimiter) AllowNativeHistogram(tenant string, h prompb.Histogram) (prompb.Histogram, bool) {
	return h, true
}
