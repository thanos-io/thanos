// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2ethanos

import (
	"github.com/cortexproject/cortex/integration/e2e"
)

type Service struct {
	*e2e.HTTPService

	grpc int
}

func NewService(
	name string,
	image string,
	command *e2e.Command,
	readiness *e2e.HTTPReadinessProbe,
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
