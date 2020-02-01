// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package prober

import (
	"google.golang.org/grpc/health"
	grpc_health "google.golang.org/grpc/health/grpc_health_v1"
)

// GRPCProbe represents health and readiness status of given component, and provides GRPC integration.
type GRPCProbe struct {
	h *health.Server
}

// NewGRPC creates a Probe that wrapped around grpc/healt.Server which reflects status of server.
func NewGRPC() *GRPCProbe {
	h := health.NewServer()
	h.SetServingStatus("", grpc_health.HealthCheckResponse_NOT_SERVING)

	return &GRPCProbe{h: h}
}

// HealthServer returns a gRPC health server which responds readiness and liveness checks.
func (p *GRPCProbe) HealthServer() *health.Server {
	return p.h
}

// Ready sets components status to ready.
func (p *GRPCProbe) Ready() {
	p.h.SetServingStatus("", grpc_health.HealthCheckResponse_SERVING)
}

// NotReady sets components status to not ready with given error as a cause.
func (p *GRPCProbe) NotReady(err error) {
	p.h.SetServingStatus("", grpc_health.HealthCheckResponse_NOT_SERVING)
}

// Healthy sets components status to healthy.
func (p *GRPCProbe) Healthy() {
	p.h.Resume()
}

// NotHealthy sets components status to not healthy with given error as a cause.
func (p *GRPCProbe) NotHealthy(err error) {
	p.h.Shutdown()
}
