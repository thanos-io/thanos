// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package prober

import (
	"io"
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"go.uber.org/atomic"
)

type check func() bool

// HTTPProbe represents health and readiness status of given component, and provides HTTP integration.
type HTTPProbe struct {
	ready   atomic.Uint32
	healthy atomic.Uint32
}

// NewHTTP returns HTTPProbe representing readiness and healthiness of given component.
func NewHTTP() *HTTPProbe {
	return &HTTPProbe{}
}

// HealthyHandler returns a HTTP Handler which responds health checks.
func (p *HTTPProbe) HealthyHandler(logger log.Logger) http.HandlerFunc {
	return p.handler(logger, p.isHealthy)
}

// ReadyHandler returns a HTTP Handler which responds readiness checks.
func (p *HTTPProbe) ReadyHandler(logger log.Logger) http.HandlerFunc {
	return p.handler(logger, p.isReady)
}

func (p *HTTPProbe) handler(logger log.Logger, c check) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if !c() {
			http.Error(w, "NOT OK", http.StatusServiceUnavailable)
			return
		}
		if _, err := io.WriteString(w, "OK"); err != nil {
			level.Error(logger).Log("msg", "failed to write probe response", "err", err)
		}
	}
}

// isReady returns true if component is ready.
func (p *HTTPProbe) isReady() bool {
	ready := p.ready.Load()
	return ready > 0
}

// isHealthy returns true if component is healthy.
func (p *HTTPProbe) isHealthy() bool {
	healthy := p.healthy.Load()
	return healthy > 0
}

// Ready sets components status to ready.
func (p *HTTPProbe) Ready() {
	p.ready.Swap(1)
}

// NotReady sets components status to not ready with given error as a cause.
func (p *HTTPProbe) NotReady(err error) {
	p.ready.Swap(0)

}

// Healthy sets components status to healthy.
func (p *HTTPProbe) Healthy() {
	p.healthy.Swap(1)
}

// NotHealthy sets components status to not healthy with given error as a cause.
func (p *HTTPProbe) NotHealthy(err error) {
	p.healthy.Swap(0)
}
