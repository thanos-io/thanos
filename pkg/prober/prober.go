package prober

import (
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/component"
	"github.com/prometheus/common/route"
)

const (
	healthyEndpointPath  = "/-/healthy"
	readyEndpointPath    = "/-/ready"
	okProbeFmt           = "thanos %v is %v"
	errorProbeFmt        = "thanos %v is not %v. Reason: %v"
	probeErrorHTTPStatus = 503
	initialErrorFmt      = "thanos %s is initializing"
)

// Prober represents health and readiness status of given component.
type Prober struct {
	logger       log.Logger
	componentMtx sync.RWMutex
	component    component.Component
	readyMtx     sync.RWMutex
	readiness    error
	healthyMtx   sync.RWMutex
	healthiness  error
}

// SetComponent sets component name of the Prober displayed in responses.
func (p *Prober) SetComponent(component component.Component) {
	p.componentMtx.Lock()
	defer p.componentMtx.Unlock()
	p.component = component
}

func (p *Prober) getComponent() component.Component {
	p.componentMtx.RLock()
	defer p.componentMtx.RUnlock()
	return p.component
}

// NewProber returns Prober representing readiness and healthiness of given component.
func NewProber(component component.Component, logger log.Logger) *Prober {
	initialErr := fmt.Errorf(initialErrorFmt, component)
	return &Prober{
		component:   component,
		logger:      logger,
		healthiness: initialErr,
		readiness:   initialErr,
	}
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

func (p *Prober) writeResponse(w http.ResponseWriter, probeFunc func() error, probeType string) {
	err := probeFunc()
	if err != nil {
		http.Error(w, fmt.Sprintf(errorProbeFmt, p.getComponent(), probeType, err), probeErrorHTTPStatus)
		return
	}
	if _, e := io.WriteString(w, fmt.Sprintf(okProbeFmt, p.getComponent(), probeType)); e == nil {
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
	}
	p.readiness = nil
}

// SetNotReady sets components status to not ready with given error as a cause.
func (p *Prober) SetNotReady(err error) {
	p.readyMtx.Lock()
	defer p.readyMtx.Unlock()
	if err != nil && p.readiness == nil {
		level.Warn(p.logger).Log("msg", "changing probe status", "status", "not-ready", "reason", err)
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
	}
	p.healthiness = nil
}

// SetNotHealthy sets components status to not healthy with given error as a cause.
func (p *Prober) SetNotHealthy(err error) {
	p.healthyMtx.Lock()
	defer p.healthyMtx.Unlock()
	if err != nil && p.healthiness == nil {
		level.Warn(p.logger).Log("msg", "changing probe status", "status", "unhealthy", "reason", err)
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
