package http

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ServerInstrumentor holds necessary metrics to instrument an http.Server
// and provides necessary behaviors
type ServerInstrumentor interface {
	// NewInstrumentedHandler wraps the given HTTP handler for instrumentation.
	NewInstrumentedHandler(handlerName string, handler http.Handler) http.HandlerFunc
	// NewInstrumentedHandlerFunc wraps the given function for instrumentation.
	NewInstrumentedHandlerFunc(handlerName string, next func(http.ResponseWriter, *http.Request)) http.HandlerFunc
}

type nopServerInstrumentor struct{}

func (ins nopServerInstrumentor) NewInstrumentedHandler(handlerName string, handler http.Handler) http.HandlerFunc {
	return ins.NewInstrumentedHandlerFunc(handlerName, handler.ServeHTTP)
}

func (ins nopServerInstrumentor) NewInstrumentedHandlerFunc(handlerName string, next func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return http.HandlerFunc(next)
}

// NewNopServerInstrumentor provides a ServerInstrumentor which does nothing
func NewNopServerInstrumentor() ServerInstrumentor {
	return nopServerInstrumentor{}
}

type serverInstrumentor struct {
	requestDuration *prometheus.HistogramVec
	requestSize     *prometheus.SummaryVec
	requestsTotal   *prometheus.CounterVec
	responseSize    *prometheus.SummaryVec
}

// NewServerInstrumentor provides default ServerInstrumentor
func NewServerInstrumentor(reg *prometheus.Registry) ServerInstrumentor {
	ins := serverInstrumentor{
		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "http_request_duration_seconds",
				Help: "Tracks the latencies for HTTP requests.",
			},
			[]string{"code", "handler", "method"},
		),

		requestSize: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name: "http_request_size_bytes",
				Help: "Tracks the size of HTTP requests.",
			},
			[]string{"code", "handler", "method"},
		),

		requestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_requests_total",
				Help: "Tracks the number of HTTP requests.",
			}, []string{"code", "handler", "method"},
		),

		responseSize: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name: "http_response_size_bytes",
				Help: "Tracks the size of HTTP responses.",
			},
			[]string{"code", "handler", "method"},
		),
	}
	reg.MustRegister(ins.requestDuration, ins.requestSize, ins.requestsTotal, ins.responseSize)
	return &ins
}

// NewInstrumentedHandler wraps the given HTTP handler for instrumentation. It
// registers four metric collectors (if not already done) and reports HTTP
// metrics to the (newly or already) registered collectors: http_requests_total
// (CounterVec), http_request_duration_seconds (Histogram),
// http_request_size_bytes (Summary), http_response_size_bytes (Summary). Each
// has a constant label named "handler" with the provided handlerName as
// value. http_requests_total is a metric vector partitioned by HTTP method
// (label name "method") and HTTP status code (label name "code").
func (ins *serverInstrumentor) NewInstrumentedHandler(handlerName string, handler http.Handler) http.HandlerFunc {
	return ins.NewInstrumentedHandlerFunc(handlerName, handler.ServeHTTP)
}

// NewInstrumentedHandlerFunc wraps the given function for instrumentation. It
// otherwise works in the same way as NewInstrumentedHandler.
func (ins *serverInstrumentor) NewInstrumentedHandlerFunc(handlerName string, next func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return promhttp.InstrumentHandlerDuration(
		ins.requestDuration.MustCurryWith(prometheus.Labels{"handler": handlerName}),
		promhttp.InstrumentHandlerRequestSize(
			ins.requestSize.MustCurryWith(prometheus.Labels{"handler": handlerName}),
			promhttp.InstrumentHandlerCounter(
				ins.requestsTotal.MustCurryWith(prometheus.Labels{"handler": handlerName}),
				promhttp.InstrumentHandlerResponseSize(
					ins.responseSize.MustCurryWith(prometheus.Labels{"handler": handlerName}),
					http.HandlerFunc(next),
				),
			),
		),
	)
}
