// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	seriesLimitName    = "series"
	samplesLimitName   = "samples"
	sizeBytesLimitName = "body_size"
)

var unlimitedRequestLimitsConfig = newEmptyRequestLimitsConfig().
	SetSizeBytesLimit(0).
	SetSeriesLimit(0).
	SetSamplesLimit(0)

type configRequestLimiter struct {
	tenantLimits        map[string]*requestLimitsConfig
	cachedDefaultLimits *requestLimitsConfig
	limitsHit           *prometheus.SummaryVec
	configuredLimits    *prometheus.GaugeVec
}

func newConfigRequestLimiter(reg prometheus.Registerer, writeLimits *writeLimitsConfig) *configRequestLimiter {
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
		tenantRequestLimits[tenant] = limitConfig.RequestLimits.OverlayWith(defaultRequestLimits)
	}

	limiter := configRequestLimiter{
		tenantLimits:        tenantRequestLimits,
		cachedDefaultLimits: defaultRequestLimits,
	}
	limiter.limitsHit = promauto.With(reg).NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:  "thanos",
			Subsystem:  "receive",
			Name:       "write_limits_hit",
			Help:       "Summary of how much beyond the limit a refused remote write request was.",
			Objectives: map[float64]float64{0.50: 0.1, 0.95: 0.1, 0.99: 0.001},
		}, []string{"tenant", "limit"},
	)
	limiter.configuredLimits = promauto.With(reg).NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "thanos",
			Subsystem: "receive",
			Name:      "write_limits",
			Help:      "The configured write limits.",
		}, []string{"tenant", "limit"},
	)
	for tenant, limits := range tenantRequestLimits {
		limiter.configuredLimits.WithLabelValues(tenant, sizeBytesLimitName).Set(float64(*limits.SizeBytesLimit))
		limiter.configuredLimits.WithLabelValues(tenant, seriesLimitName).Set(float64(*limits.SeriesLimit))
		limiter.configuredLimits.WithLabelValues(tenant, samplesLimitName).Set(float64(*limits.SamplesLimit))
	}
	limiter.configuredLimits.WithLabelValues("", sizeBytesLimitName).Set(float64(*defaultRequestLimits.SizeBytesLimit))
	limiter.configuredLimits.WithLabelValues("", seriesLimitName).Set(float64(*defaultRequestLimits.SeriesLimit))
	limiter.configuredLimits.WithLabelValues("", samplesLimitName).Set(float64(*defaultRequestLimits.SamplesLimit))

	return &limiter
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
	if !allowed {
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
	if !allowed {
		l.limitsHit.
			WithLabelValues(tenant, samplesLimitName).
			Observe(float64(amount - *limit))
	}
	return allowed
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
