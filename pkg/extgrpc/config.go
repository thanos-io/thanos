// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extgrpc

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/httpconfig"
)

// Config is a structure that allows pointing to various gRPC endpoint, e.g Querier connecting to StoreAPI.
type Config struct {
	GRPCClientConfig httpconfig.ClientConfig    `yaml:"grpc_config"`
	EndpointsConfig  httpconfig.EndpointsConfig `yaml:",inline"`
}

func DefaultConfig() Config {
	return Config{
		EndpointsConfig: httpconfig.EndpointsConfig{
			Addresses:     []string{},
			FileSDConfigs: []httpconfig.FileSDConfig{},
		},
	}
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig()
	type plain Config
	return unmarshal((*plain)(c))
}

// BuildConfig returns a configuration from a static addresses.
func BuildConfig(addrs []string) ([]Config, error) {
	configs := make([]Config, 0, len(addrs))
	for i, addr := range addrs {
		if addr == "" {
			return nil, errors.Errorf("static address cannot be empty, but was at index %d", i)
		}
		if strings.Contains(addr, "/") {
			return nil, errors.Errorf("gRPC address either has HTTP scheme or path. We expect only host+port with optional dns+ dnssrv+ prefix in it. Got %v", addr)
		}

		configs = append(configs, Config{
			EndpointsConfig: httpconfig.EndpointsConfig{
				Addresses: []string{addr},
			},
		})
	}
	return configs, nil
}
