package prober

import (
	"io"
	"net/http"
	"sync/atomic"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/component"
)

type check func() bool

const (
	ready   = "ready"
	healthy = "healthy"
)

// HTTPProbe represents health and readiness status of given component, and provides HTTP integration.
type HTTPProbe struct {
	component component.Component
	logger    log.Logger

	ready   uint32
	healthy uint32

	status *prometheus.GaugeVec
}

// NewHTTP returns HTTPProbe representing readiness and healthiness of given component.
func NewHTTP(component component.Component, logger log.Logger, reg prometheus.Registerer) *HTTPProbe {
	p := &HTTPProbe{
		component: component,
		logger:    logger,
		status: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "status",
			Help:        "Represents status (0 indicates success, 1 indicates failure) of the component.",
			ConstLabels: map[string]string{"component": component.String()},
		},
			[]string{"check"},
		),
	}

	if reg != nil {
		reg.MustRegister(p.status)
	}

	return p
}

// HealthyHandler returns a HTTP Handler which responds health checks.
func (p *HTTPProbe) HealthyHandler() http.HandlerFunc {
	return p.handler(p.isHealthy)
}

// ReadyHandler returns a HTTP Handler which responds readiness checks.
func (p *HTTPProbe) ReadyHandler() http.HandlerFunc {
	return p.handler(p.isReady)
}

func (p *HTTPProbe) handler(c check) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if !c() {
			http.Error(w, "NOT OK", http.StatusServiceUnavailable)
			return
		}
		if _, err := io.WriteString(w, "OK"); err != nil {
			level.Error(p.logger).Log("msg", "failed to write probe response", "err", err)
		}
	}
}

// isReady returns true if component is ready.
func (p *HTTPProbe) isReady() bool {
	ready := atomic.LoadUint32(&p.ready)
	return ready > 0
}

// isHealthy returns true if component is healthy.
func (p *HTTPProbe) isHealthy() bool {
	healthy := atomic.LoadUint32(&p.healthy)
	return healthy > 0
}

// Ready sets components status to ready.
func (p *HTTPProbe) Ready() {
	old := atomic.SwapUint32(&p.ready, 1)

	if old == 0 {
		p.status.WithLabelValues(ready).Set(1)
		level.Info(p.logger).Log("msg", "changing probe status", "status", "ready")
	}
}

// NotReady sets components status to not ready with given error as a cause.
func (p *HTTPProbe) NotReady(err error) {
	old := atomic.SwapUint32(&p.ready, 0)

	if old == 1 {
		p.status.WithLabelValues(ready).Set(0)
		level.Warn(p.logger).Log("msg", "changing probe status", "status", "not-ready", "reason", err)
	}
}

// Healthy sets components status to healthy.
func (p *HTTPProbe) Healthy() {
	old := atomic.SwapUint32(&p.healthy, 1)

	if old == 0 {
		p.status.WithLabelValues(healthy).Set(1)
		level.Info(p.logger).Log("msg", "changing probe status", "status", "healthy")
	}
}

// NotHealthy sets components status to not healthy with given error as a cause.
func (p *HTTPProbe) NotHealthy(err error) {
	old := atomic.SwapUint32(&p.healthy, 0)

	if old == 1 {
		p.status.WithLabelValues(healthy).Set(0)
		level.Info(p.logger).Log("msg", "changing probe status", "status", "not-healthy", "reason", err)
	}
}
