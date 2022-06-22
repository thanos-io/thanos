// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package http

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
)

type tenantInstrumentationMiddleware struct {
	metrics          *defaultMetrics
	tenantHeaderName string
}

// NewTenantInstrumentationMiddleware provides the same instrumentation as defaultInstrumentationMiddleware,
// but with a tenant label fetched from the given tenantHeaderName header.
// Passing nil as buckets uses the default buckets.
func NewTenantInstrumentationMiddleware(tenantHeaderName string, reg prometheus.Registerer, buckets []float64) InstrumentationMiddleware {
	return &tenantInstrumentationMiddleware{
		tenantHeaderName: tenantHeaderName,
		metrics:          newDefaultMetrics(reg, buckets, []string{"tenant"}),
	}
}

// NewHandler wraps the given HTTP handler for instrumentation. It
// registers four metric collectors (if not already done) and reports HTTP
// metrics to the (newly or already) registered collectors: http_requests_total
// (CounterVec), http_request_duration_seconds (Histogram),
// http_request_size_bytes (Summary), http_response_size_bytes (Summary).
// Each has a constant label named "handler" with the provided handlerName as value.
func (ins *tenantInstrumentationMiddleware) NewHandler(handlerName string, next http.Handler) http.HandlerFunc {
	tenantWrapper := func(w http.ResponseWriter, r *http.Request) {
		tenant := r.Header.Get(ins.tenantHeaderName)
		baseLabels := prometheus.Labels{"handler": handlerName, "tenant": tenant}
		handlerStack := httpInstrumentationHandler(baseLabels, ins.metrics, next)
		handlerStack.ServeHTTP(w, r)
	}
	return tenantWrapper
}
