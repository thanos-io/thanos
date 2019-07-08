package extprom

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "http_request_duration_seconds",
			Help: "Tracks the latencies for HTTP requests.",
		},
		[]string{"code", "handler", "method"},
	)

	requestSize = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "http_request_size_bytes",
			Help: "Tracks the size of HTTP requests.",
		},
		[]string{"code", "handler", "method"},
	)

	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Tracks the number of HTTP requests.",
		}, []string{"code", "handler", "method"},
	)

	responseSize = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "http_response_size_bytes",
			Help: "Tracks the size of HTTP responses.",
		},
		[]string{"code", "handler", "method"},
	)
)

func init() {
	prometheus.MustRegister(requestDuration, requestSize, requestsTotal, responseSize)
}

// NewInstrumentedHandler wraps the given HTTP handler for instrumentation. It
// registers four metric collectors (if not already done) and reports HTTP
// metrics to the (newly or already) registered collectors: http_requests_total
// (CounterVec), http_request_duration_seconds (Histogram),
// http_request_size_bytes (Summary), http_response_size_bytes (Summary). Each
// has a constant label named "handler" with the provided handlerName as
// value. http_requests_total is a metric vector partitioned by HTTP method
// (label name "method") and HTTP status code (label name "code").
func NewInstrumentedHandler(handlerName string, handler http.Handler) http.HandlerFunc {
	return NewInstrumentedHandlerFunc(handlerName, handler.ServeHTTP)
}

// NewInstrumentedHandlerFunc wraps the given function for instrumentation. It
// otherwise works in the same way as NewInstrumentedHandler.
func NewInstrumentedHandlerFunc(handlerName string, next func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return promhttp.InstrumentHandlerDuration(
		requestDuration.MustCurryWith(prometheus.Labels{"handler": handlerName}),
		promhttp.InstrumentHandlerRequestSize(
			requestSize.MustCurryWith(prometheus.Labels{"handler": handlerName}),
			promhttp.InstrumentHandlerCounter(
				requestsTotal.MustCurryWith(prometheus.Labels{"handler": handlerName}),
				promhttp.InstrumentHandlerResponseSize(
					responseSize.MustCurryWith(prometheus.Labels{"handler": handlerName}),
					http.HandlerFunc(next),
				),
			),
		),
	)
}
