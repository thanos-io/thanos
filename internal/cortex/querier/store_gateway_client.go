// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package querier

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/thanos-io/thanos/internal/cortex/storegateway/storegatewaypb"
	"github.com/thanos-io/thanos/internal/cortex/util/tls"
)

type storeGatewayClient struct {
	storegatewaypb.StoreGatewayClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

type ClientConfig struct {
	TLSEnabled bool             `yaml:"tls_enabled"`
	TLS        tls.ClientConfig `yaml:",inline"`
}
