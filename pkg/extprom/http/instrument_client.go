package http

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

type ClientMetrics struct {
	inFlightGauge            prometheus.Gauge
	requestTotalCount        *prometheus.CounterVec
	dnsLatencyHistogram      *prometheus.HistogramVec
	tlsLatencyHistogram      *prometheus.HistogramVec
	requestDurationHistogram *prometheus.HistogramVec
}

func NewClientMetrics(reg prometheus.Registerer, namespace string) *ClientMetrics {
	var m ClientMetrics

	m.inFlightGauge = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "http_client",
		Name:      "in_flight_requests",
		Help:      "A gauge of in-flight requests.",
	})

	m.requestTotalCount = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "http_client",
		Name:      "request_total",
		Help:      "",
	}, []string{"code", "method"})

	m.dnsLatencyHistogram = promauto.With(reg).NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "http_client",
			Name:      "dns_duration_seconds",
			Help:      "Trace dns latency histogram.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"event"},
	)

	m.tlsLatencyHistogram = promauto.With(reg).NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "http_client",
			Name:      "tls_duration_seconds",
			Help:      "Trace tls latency histogram.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"event"},
	)

	m.requestDurationHistogram = promauto.With(reg).NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "http_client",
			Name:      "request_duration_seconds",
			Help:      "A histogram of request latencies.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	return &m
}

func InstrumentedRoundTripper(tripper http.RoundTripper, m *ClientMetrics) http.RoundTripper {
	if m == nil {
		return tripper
	}

	trace := &promhttp.InstrumentTrace{
		DNSStart: func(t float64) {
			m.dnsLatencyHistogram.WithLabelValues("dns_start").Observe(t)
		},
		DNSDone: func(t float64) {
			m.dnsLatencyHistogram.WithLabelValues("dns_done").Observe(t)
		},
		TLSHandshakeStart: func(t float64) {
			m.tlsLatencyHistogram.WithLabelValues("tls_handshake_start").Observe(t)
		},
		TLSHandshakeDone: func(t float64) {
			m.tlsLatencyHistogram.WithLabelValues("tls_handshake_done").Observe(t)
		},
	}

	return promhttp.InstrumentRoundTripperInFlight(
		m.inFlightGauge,
		promhttp.InstrumentRoundTripperCounter(
			m.requestTotalCount,
			promhttp.InstrumentRoundTripperTrace(
				trace,
				promhttp.InstrumentRoundTripperDuration(
					m.requestDurationHistogram,
					tripper,
				),
			),
		),
	)
}
