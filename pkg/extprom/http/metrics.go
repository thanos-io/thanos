// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package http

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type defaultMetrics struct {
	requestDuration      *prometheus.HistogramVec
	requestSize          *prometheus.HistogramVec
	requestsTotal        *prometheus.CounterVec
	responseSize         *prometheus.HistogramVec
	inflightHTTPRequests *prometheus.GaugeVec
}

func newDefaultMetrics(reg prometheus.Registerer, durationBuckets []float64, extraLabels []string) *defaultMetrics {
	if durationBuckets == nil {
		durationBuckets = []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120, 240, 360, 720}
	}

	bytesBuckets := prometheus.ExponentialBuckets(64, 2, 10)
	bucketFactor := 1.1
	maxBuckets := uint32(100)

	return &defaultMetrics{
		requestDuration: promauto.With(reg).NewHistogramVec(
			prometheus.HistogramOpts{
				Name:                           "http_request_duration_seconds",
				Help:                           "Tracks the latencies for HTTP requests.",
				Buckets:                        durationBuckets,
				NativeHistogramBucketFactor:    bucketFactor,
				NativeHistogramMaxBucketNumber: maxBuckets,
			},
			append([]string{"code", "handler", "method"}, extraLabels...),
		),

		requestSize: promauto.With(reg).NewHistogramVec(
			prometheus.HistogramOpts{
				Name:                           "http_request_size_bytes",
				Help:                           "Tracks the size of HTTP requests.",
				Buckets:                        bytesBuckets,
				NativeHistogramBucketFactor:    bucketFactor,
				NativeHistogramMaxBucketNumber: maxBuckets,
			},
			append([]string{"code", "handler", "method"}, extraLabels...),
		),

		requestsTotal: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_requests_total",
				Help: "Tracks the number of HTTP requests.",
			},
			append([]string{"code", "handler", "method"}, extraLabels...),
		),

		responseSize: promauto.With(reg).NewHistogramVec(
			prometheus.HistogramOpts{
				Name:                           "http_response_size_bytes",
				Help:                           "Tracks the size of HTTP responses.",
				Buckets:                        bytesBuckets,
				NativeHistogramBucketFactor:    bucketFactor,
				NativeHistogramMaxBucketNumber: maxBuckets,
			},
			append([]string{"code", "handler", "method"}, extraLabels...),
		),

		inflightHTTPRequests: promauto.With(reg).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "http_inflight_requests",
				Help: "Current number of HTTP requests the handler is responding to.",
			},
			append([]string{"handler", "method"}, extraLabels...),
		),
	}
}
