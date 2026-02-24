// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package prober

import (
	"context"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/pkg/errors"
	grpc_health "google.golang.org/grpc/health/grpc_health_v1"
)

func isServing(p *GRPCProbe) bool {
	// Use the health server's Check RPC to inspect status.
	resp, err := p.h.Check(context.Background(), &grpc_health.HealthCheckRequest{Service: ""})
	if err != nil {
		return false
	}
	return resp.Status == grpc_health.HealthCheckResponse_SERVING
}

func TestGRPCProbeInitialState(t *testing.T) {
	p := NewGRPC()
	testutil.Assert(t, !isServing(p), "initially should not be serving")
}

func TestGRPCProbeHealthyAloneDoesNotServe(t *testing.T) {
	p := NewGRPC()
	p.Healthy()
	testutil.Assert(t, !isServing(p), "Healthy() alone should not make probe serving")
}

func TestGRPCProbeReadySetsServing(t *testing.T) {
	p := NewGRPC()
	p.Ready()
	testutil.Assert(t, isServing(p), "Ready() should make probe serving")
}

func TestGRPCProbeNotReadyUnsetsServing(t *testing.T) {
	p := NewGRPC()
	p.Ready()
	testutil.Assert(t, isServing(p), "should be serving after Ready()")

	p.NotReady(errors.New("test"))
	testutil.Assert(t, !isServing(p), "should not be serving after NotReady()")
}

func TestGRPCProbeNotHealthyDoesNotAffectServing(t *testing.T) {
	p := NewGRPC()
	p.Ready()
	testutil.Assert(t, isServing(p), "should be serving after Ready()")

	p.NotHealthy(errors.New("test"))
	testutil.Assert(t, isServing(p), "NotHealthy() should not affect serving status")
}

func TestGRPCProbeHealthyAfterReadyDoesNotAffect(t *testing.T) {
	p := NewGRPC()
	p.Ready()
	p.Healthy()
	testutil.Assert(t, isServing(p), "Healthy() after Ready() should still be serving")
}
