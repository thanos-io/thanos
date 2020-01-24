package prober

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/thanos-io/thanos/pkg/component"
	"google.golang.org/grpc/health"
	grpc_health "google.golang.org/grpc/health/grpc_health_v1"
)

// GRPCProbe represents health and readiness status of given component, and provides GRPC integration.
type GRPCProbe struct {
	component component.Component
	logger    log.Logger

	h *health.Server
}

// NewGRPC creates a Probe that wrapped around grpc/healt.Server which reflects status of server.
func NewGRPC(component component.Component, logger log.Logger) *GRPCProbe {
	h := health.NewServer()
	h.SetServingStatus("", grpc_health.HealthCheckResponse_NOT_SERVING)

	return &GRPCProbe{component: component, logger: logger, h: h}
}

// HealthServer returns a gRPC health server which responds readiness and liveness checks.
func (p *GRPCProbe) HealthServer() *health.Server {
	return p.h
}

// Ready sets components status to ready.
func (p *GRPCProbe) Ready() {
	p.h.SetServingStatus("", grpc_health.HealthCheckResponse_SERVING)
	level.Info(p.logger).Log("msg", "changing probe status", "status", "ready")
}

// NotReady sets components status to not ready with given error as a cause.
func (p *GRPCProbe) NotReady(err error) {
	p.h.SetServingStatus("", grpc_health.HealthCheckResponse_NOT_SERVING)
	level.Warn(p.logger).Log("msg", "changing probe status", "status", "not-ready", "reason", err)
}

// Healthy sets components status to healthy.
func (p *GRPCProbe) Healthy() {
	p.h.Resume()
	level.Info(p.logger).Log("msg", "changing probe status", "status", "healthy")
}

// NotHealthy sets components status to not healthy with given error as a cause.
func (p *GRPCProbe) NotHealthy(err error) {
	p.h.Shutdown()

	level.Info(p.logger).Log("msg", "changing probe status", "status", "not-healthy", "reason", err)
}
