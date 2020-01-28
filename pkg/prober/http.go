// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package prober

import (
	"io"
	"net/http"
	"sync/atomic"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

type check func() bool

// HTTPProbe represents health and readiness status of given component, and provides HTTP integration.
type HTTPProbe struct {
	ready   uint32
	healthy uint32
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
	atomic.SwapUint32(&p.ready, 1)
}

// NotReady sets components status to not ready with given error as a cause.
func (p *HTTPProbe) NotReady(err error) {
	atomic.SwapUint32(&p.ready, 0)

}

// Healthy sets components status to healthy.
func (p *HTTPProbe) Healthy() {
	atomic.SwapUint32(&p.healthy, 1)

}

// NotHealthy sets components status to not healthy with given error as a cause.
func (p *HTTPProbe) NotHealthy(err error) {
	atomic.SwapUint32(&p.healthy, 0)
}
