// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryconfig

type GRPCConfig struct {
	EndpointAddrs []string `yaml:"endpoint_addresses"`
}
