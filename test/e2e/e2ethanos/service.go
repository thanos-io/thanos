// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2ethanos

import (
	"os/exec"
	"testing"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/thanos-io/thanos/pkg/testutil"
)

type Service struct {
	*e2e.HTTPService

	grpc int
}

func NewService(
	name string,
	image string,
	command *e2e.Command,
	readiness *e2e.ReadinessProbe,
	http, grpc int,
	otherPorts ...int,
) *Service {
	return &Service{
		HTTPService: e2e.NewHTTPService(name, image, command, readiness, http, append(otherPorts, grpc)...),
		grpc:        grpc,
	}
}

func (s *Service) GRPCEndpoint() string { return s.Endpoint(s.grpc) }

func (s *Service) GRPCNetworkEndpoint() string {
	return s.NetworkEndpoint(s.grpc)
}

func (s *Service) GRPCNetworkEndpointFor(networkName string) string {
	return s.NetworkEndpointFor(networkName, s.grpc)
}

func CleanScenario(t *testing.T, s *e2e.Scenario) func() {
	return func() {
		// Otherwise close will fail.
		testutil.Ok(t, exec.Command("chmod", "-R", "777", s.SharedDir()).Run())
		s.Close()
	}
}
