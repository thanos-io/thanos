// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package clientconfig is a wrapper around github.com/prometheus/common/config with additional
// support for gRPC clients.
package clientconfig

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

// Config is a structure that allows pointing to various HTTP and GRPC endpoints, e.g. ruler connecting to queriers.
type Config struct {
	HTTPConfig HTTPConfig  `yaml:",inline"`
	GRPCConfig *GRPCConfig `yaml:"grpc_config"`
}

func DefaultConfig() Config {
	return Config{
		HTTPConfig: HTTPConfig{
			EndpointsConfig: HTTPEndpointsConfig{
				Scheme:          "http",
				StaticAddresses: []string{},
				FileSDConfigs:   []HTTPFileSDConfig{},
			},
		},
		GRPCConfig: &GRPCConfig{
			EndpointAddrs: []string{},
		},
	}
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig()
	type plain Config
	return unmarshal((*plain)(c))
}

// LoadConfigs loads a list of Config from YAML data.
func LoadConfigs(confYAML []byte) ([]Config, error) {
	var clientCfg []Config
	if err := yaml.UnmarshalStrict(confYAML, &clientCfg); err != nil {
		return nil, err
	}
	return clientCfg, nil
}

// BuildConfigFromHTTPAddresses returns a configuration from static addresses.
func BuildConfigFromHTTPAddresses(addrs []string) ([]Config, error) {
	configs := make([]Config, 0, len(addrs))
	for i, addr := range addrs {
		if addr == "" {
			return nil, errors.Errorf("static address cannot be empty at index %d", i)
		}
		// If addr is missing schema, add http.
		if !strings.Contains(addr, "://") {
			addr = fmt.Sprintf("http://%s", addr)
		}
		u, err := url.Parse(addr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse addr %q", addr)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, errors.Errorf("%q is not supported scheme for address", u.Scheme)
		}
		configs = append(configs, Config{
			HTTPConfig: HTTPConfig{
				EndpointsConfig: HTTPEndpointsConfig{
					Scheme:          u.Scheme,
					StaticAddresses: []string{u.Host},
					PathPrefix:      u.Path,
				},
			},
		})
	}
	return configs, nil
}

// BuildConfigFromGRPCAddresses returns a configuration from a static addresses.
func BuildConfigFromGRPCAddresses(addrs []string) ([]Config, error) {
	configs := make([]Config, 0, len(addrs))
	for i, addr := range addrs {
		if addr == "" {
			return nil, errors.Errorf("static address cannot be empty at index %d", i)
		}
		configs = append(configs, Config{
			GRPCConfig: &GRPCConfig{
				EndpointAddrs: []string{addr},
			},
		})
	}
	return configs, nil
}
