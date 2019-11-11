package prober

import (
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/component"
)

const (
	healthyEndpointPath  = "/-/healthy"
	readyEndpointPath    = "/-/ready"
	probeErrorHTTPStatus = 503
	initialErrorFmt      = "thanos %s is initializing"
)

// Prober represents health and readiness status of given component.
//
// From Kubernetes documentation https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-probes/ :
//
//   liveness: Many applications running for long periods of time eventually transition to broken states,
//   (healthy) and cannot recover except by being restarted.
//             Kubernetes provides liveness probes to detect and remedy such situations.
//
//   readiness: Sometimes, applications are temporarily unable to serve traffic.
//   (ready)    For example, an application might need to load large data or configuration files during startup,
//              or depend on external services after startup. In such cases, you don’t want to kill the application,
//              but you don’t want to send it requests either. Kubernetes provides readiness probes to detect
//              and mitigate these situations. A pod with containers reporting that they are not ready
//              does not receive traffic through Kubernetes Services.
type Prober struct {
	logger             log.Logger
	component          component.Component
	readyMtx           sync.RWMutex
	readiness          error
	healthyMtx         sync.RWMutex
	healthiness        error
	readyStateMetric   prometheus.Gauge
	healthyStateMetric prometheus.Gauge
}

// NewProber returns Prober representing readiness and healthiness of given component.
func NewProber(component component.Component, logger log.Logger, reg prometheus.Registerer) *Prober {
	initialErr := fmt.Errorf(initialErrorFmt, component)
	p := &Prober{
		component:   component,
		logger:      logger,
		healthiness: initialErr,
		readiness:   initialErr,
		readyStateMetric: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "prober_ready",
			Help:        "Represents readiness status of the component Prober.",
			ConstLabels: map[string]string{"component": component.String()},
		}),
		healthyStateMetric: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "prober_healthy",
			Help:        "Represents health status of the component Prober.",
			ConstLabels: map[string]string{"component": component.String()},
		}),
	}
	if reg != nil {
		reg.MustRegister(p.readyStateMetric, p.healthyStateMetric)
	}
	return p
}

// RegisterInRouter registers readiness and liveness probes to router.
func (p *Prober) RegisterInRouter(router *route.Router) {
	router.Get(healthyEndpointPath, p.probeHandlerFunc(p.IsHealthy, "healthy"))
	router.Get(readyEndpointPath, p.probeHandlerFunc(p.IsReady, "ready"))
}

// RegisterInMux registers readiness and liveness probes to mux.
func (p *Prober) RegisterInMux(mux *http.ServeMux) {
	mux.HandleFunc(healthyEndpointPath, p.probeHandlerFunc(p.IsHealthy, "healthy"))
	mux.HandleFunc(readyEndpointPath, p.probeHandlerFunc(p.IsReady, "ready"))
}

func (p *Prober) writeResponse(w http.ResponseWriter, probeFn func() error, probeType string) {
	if err := probeFn(); err != nil {
		http.Error(w, fmt.Sprintf("thanos %v is not %v. Reason: %v", p.component, probeType, err), probeErrorHTTPStatus)
		return
	}
	if _, err := io.WriteString(w, fmt.Sprintf("thanos %v is %v", p.component, probeType)); err != nil {
		level.Error(p.logger).Log("msg", "failed to write probe response", "probe type", probeType, "err", err)
	}
}

func (p *Prober) probeHandlerFunc(probeFunc func() error, probeType string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		p.writeResponse(w, probeFunc, probeType)
	}
}

// IsReady returns error if component is not ready and nil otherwise.
func (p *Prober) IsReady() error {
	p.readyMtx.RLock()
	defer p.readyMtx.RUnlock()
	return p.readiness
}

// SetReady sets components status to ready.
func (p *Prober) SetReady() {
	p.readyMtx.Lock()
	defer p.readyMtx.Unlock()
	if p.readiness != nil {
		level.Info(p.logger).Log("msg", "changing probe status", "status", "ready")
		p.readyStateMetric.Set(1)
	}
	p.readiness = nil
}

// SetNotReady sets components status to not ready with given error as a cause.
func (p *Prober) SetNotReady(err error) {
	p.readyMtx.Lock()
	defer p.readyMtx.Unlock()
	if err != nil && p.readiness == nil {
		level.Warn(p.logger).Log("msg", "changing probe status", "status", "not-ready", "reason", err)
		p.readyStateMetric.Set(0)
	}
	p.readiness = err
}

// IsHealthy returns error if component is not healthy and nil if it is.
func (p *Prober) IsHealthy() error {
	p.healthyMtx.RLock()
	defer p.healthyMtx.RUnlock()
	return p.healthiness
}

// SetHealthy sets components status to healthy.
func (p *Prober) SetHealthy() {
	p.healthyMtx.Lock()
	defer p.healthyMtx.Unlock()
	if p.healthiness != nil {
		level.Info(p.logger).Log("msg", "changing probe status", "status", "healthy")
		p.healthyStateMetric.Set(1)
	}
	p.healthiness = nil
}

// SetNotHealthy sets components status to not healthy with given error as a cause.
func (p *Prober) SetNotHealthy(err error) {
	p.healthyMtx.Lock()
	defer p.healthyMtx.Unlock()
	if err != nil && p.healthiness == nil {
		level.Warn(p.logger).Log("msg", "changing probe status", "status", "unhealthy", "reason", err)
		p.healthyStateMetric.Set(0)
	}
	p.healthiness = err
}

// HandleIfReady if probe is ready calls the function otherwise returns 503.
func (p *Prober) HandleIfReady(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ready := p.IsReady()
		if ready == nil {
			f(w, r)
			return
		}
		p.writeResponse(w, func() error { return ready }, "ready")
	}
}
