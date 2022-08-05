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

type requestLimiter struct {
	sizeBytesLimit   int64
	seriesLimit      int64
	samplesLimit     int64
	limitsHit        *prometheus.SummaryVec
	configuredLimits *prometheus.GaugeVec
}

func newRequestLimiter(sizeBytesLimit, seriesLimit, samplesLimit int64, reg prometheus.Registerer) requestLimiter {
	limiter := requestLimiter{
		sizeBytesLimit: sizeBytesLimit,
		seriesLimit:    seriesLimit,
		samplesLimit:   samplesLimit,
		limitsHit: promauto.With(reg).NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace:  "thanos",
				Subsystem:  "receive",
				Name:       "write_limits_hit",
				Help:       "Summary of how far beyond the limit a refused remote write request was.",
				Objectives: map[float64]float64{0.50: 0.1, 0.95: 0.1, 0.99: 0.001},
			}, []string{"tenant", "limit"},
		),
		configuredLimits: promauto.With(reg).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "thanos",
				Subsystem: "receive",
				Name:      "write_limits",
				Help:      "The configured write limits.",
			}, []string{"limit"},
		),
	}
	limiter.configuredLimits.WithLabelValues(sizeBytesLimitName).Set(float64(sizeBytesLimit))
	limiter.configuredLimits.WithLabelValues(seriesLimitName).Set(float64(seriesLimit))
	limiter.configuredLimits.WithLabelValues(samplesLimitName).Set(float64(samplesLimit))
	return limiter
}

func (l *requestLimiter) AllowSizeBytes(tenant string, contentLengthBytes int64) bool {
	if l.sizeBytesLimit <= 0 {
		return true
	}

	// This happens when the content length is unknown, then we allow it.
	if contentLengthBytes < 0 {
		return true
	}
	allowed := l.sizeBytesLimit >= contentLengthBytes
	if !allowed {
		l.limitsHit.
			WithLabelValues(tenant, sizeBytesLimitName).
			Observe(float64(contentLengthBytes - l.sizeBytesLimit))
	}
	return allowed
}

func (l *requestLimiter) AllowSeries(tenant string, amount int64) bool {
	if l.seriesLimit <= 0 {
		return true
	}
	allowed := l.seriesLimit >= amount
	if !allowed {
		l.limitsHit.
			WithLabelValues(tenant, seriesLimitName).
			Observe(float64(amount - l.seriesLimit))
	}
	return allowed
}

func (l *requestLimiter) AllowSamples(tenant string, amount int64) bool {
	if l.samplesLimit <= 0 {
		return true
	}
	allowed := l.samplesLimit >= amount
	if !allowed {
		l.limitsHit.
			WithLabelValues(tenant, samplesLimitName).
			Observe(float64(amount - l.samplesLimit))
	}
	return allowed
}
