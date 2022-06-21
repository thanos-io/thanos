// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package http

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/uber/jaeger-client-go"
)

// InstrumentationMiddleware holds necessary metrics to instrument an http.Server
// and provides necessary behaviors.
type InstrumentationMiddleware interface {
	// NewHandler wraps the given HTTP handler for instrumentation.
	NewHandler(handlerName string, handler http.Handler) http.HandlerFunc
}

type nopInstrumentationMiddleware struct{}

func (ins nopInstrumentationMiddleware) NewHandler(handlerName string, handler http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
	}
}

// NewNopInstrumentationMiddleware provides a InstrumentationMiddleware which does nothing.
func NewNopInstrumentationMiddleware() InstrumentationMiddleware {
	return nopInstrumentationMiddleware{}
}

type defaultInstrumentationMiddleware struct {
	metrics *defaultMetrics
}

// NewInstrumentationMiddleware provides default InstrumentationMiddleware.
// Passing nil as buckets uses the default buckets.
func NewInstrumentationMiddleware(reg prometheus.Registerer, buckets []float64) InstrumentationMiddleware {
	return &defaultInstrumentationMiddleware{
		metrics: newDefaultMetrics(reg, buckets, []string{}),
	}
}

// NewHandler wraps the given HTTP handler for instrumentation. It
// registers five metric collectors (if not already done) and reports HTTP
// metrics to the (newly or already) registered collectors: http_requests_total
// (CounterVec), http_request_duration_seconds (Histogram),
// http_request_size_bytes (Summary), http_response_size_bytes (Summary),
// http_inflight_requests (Gauge). Each has a constant label named "handler"
// with the provided handlerName as value.
func (ins *defaultInstrumentationMiddleware) NewHandler(handlerName string, handler http.Handler) http.HandlerFunc {
	baseLabels := prometheus.Labels{"handler": handlerName}
	return httpInstrumentationHandler(baseLabels, ins.metrics, handler)
}

func httpInstrumentationHandler(baseLabels prometheus.Labels, metrics *defaultMetrics, next http.Handler) http.HandlerFunc {
	return promhttp.InstrumentHandlerRequestSize(
		metrics.requestSize.MustCurryWith(baseLabels),
		promhttp.InstrumentHandlerCounter(
			metrics.requestsTotal.MustCurryWith(baseLabels),
			promhttp.InstrumentHandlerResponseSize(
				metrics.responseSize.MustCurryWith(baseLabels),
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					now := time.Now()

					wd := &responseWriterDelegator{w: w}
					next.ServeHTTP(wd, r)

					requestLabels := prometheus.Labels{"code": wd.Status(), "method": strings.ToLower(r.Method)}
					observer := metrics.requestDuration.MustCurryWith(baseLabels).With(requestLabels)
					observer.Observe(time.Since(now).Seconds())

					// If we find a tracingID we'll expose it as Exemplar.
					span := opentracing.SpanFromContext(r.Context())
					if span != nil {
						spanCtx, ok := span.Context().(jaeger.SpanContext)
						if ok && spanCtx.IsSampled() {
							observer.(prometheus.ExemplarObserver).ObserveWithExemplar(
								time.Since(now).Seconds(),
								prometheus.Labels{
									"traceID": spanCtx.TraceID().String(),
								},
							)
						}
					}
				}),
			),
		),
	)
}

// responseWriterDelegator implements http.ResponseWriter and extracts the statusCode.
type responseWriterDelegator struct {
	w          http.ResponseWriter
	written    bool
	statusCode int
}

func (wd *responseWriterDelegator) Header() http.Header {
	return wd.w.Header()
}

func (wd *responseWriterDelegator) Write(bytes []byte) (int, error) {
	return wd.w.Write(bytes)
}

func (wd *responseWriterDelegator) WriteHeader(statusCode int) {
	wd.written = true
	wd.statusCode = statusCode
	wd.w.WriteHeader(statusCode)
}

func (wd *responseWriterDelegator) StatusCode() int {
	if !wd.written {
		return http.StatusOK
	}
	return wd.statusCode
}

func (wd *responseWriterDelegator) Status() string {
	return fmt.Sprintf("%d", wd.StatusCode())
}

// NewInstrumentHandlerInflightTenant creates a middleware used to export the current amount of concurrent requests
// being handled. It has an optional tenant label whenever a tenant is present in the context.
func NewInstrumentHandlerInflightTenant(gaugeVec *prometheus.GaugeVec, tenantHeader string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenant := r.Header.Get(tenantHeader)
		promhttp.InstrumentHandlerInFlight(gaugeVec.With(prometheus.Labels{"tenant": tenant}), next).ServeHTTP(w, r)
	}
}
