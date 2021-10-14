// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/exthttp"
	"gopkg.in/yaml.v2"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/discovery/file"
)

// EndpointConfig represents the configuration of a set of gRPC Store API endpoints.
// If `tls_config` is omitted then TLS will not be used.
// Configs must have a name and they must be unique.
type EndpointConfig struct {
	extgrpc.Config `yaml:",inline"`

	Mode EndpointMode `yaml:"mode"`

	// TODO(bwplotka): Allow filtering by API (e.g someone wants to have endpoint that serves Store and Exemplar API but want to connect to Store only.
}

type EndpointMode string

const (
	DefaultEndpointMode EndpointMode = ""
	StrictEndpointMode  EndpointMode = "strict"
)

// LoadConfig returns list of per-endpoint TLS config.
func LoadConfig(confYAML []byte, endpointAddrs, strictEndpointAddrs []string, globalFileSDConfig *file.SDConfig, globalTLSConfig exthttp.TLSConfig) ([]EndpointConfig, error) {
	var endpointConfig []EndpointConfig

	if len(confYAML) > 0 {
		if err := yaml.UnmarshalStrict(confYAML, &endpointConfig); err != nil {
			return nil, err
		}

		// Checking if wrong mode is provided.
		for _, config := range endpointConfig {
			if config.Mode != StrictEndpointMode && config.Mode != DefaultEndpointMode {
				return nil, errors.Errorf("%s is wrong mode", config.Mode)
			}
		}

		// No dynamic endpoints in strict mode.
		for _, config := range endpointConfig {
			if config.Mode == StrictEndpointMode && len(config.EndpointsConfig.FileSDConfigs) != 0 {
				return nil, errors.Errorf("no sd-files allowed in strict mode")
			}
		}
	}

	// Adding --store, rule, metadata, target, exemplar and --store.sd-files, if provided.
	// Global TLS config applies until deprecated.
	if len(endpointAddrs) > 0 || globalFileSDConfig != nil {
		cfg := EndpointConfig{}
		cfg.GRPCClientConfig.TLSConfig = globalTLSConfig
		cfg.EndpointsConfig.Addresses = endpointAddrs
		if globalFileSDConfig != nil {
			cfg.EndpointsConfig.FileSDConfigs = []exthttp.FileSDConfig{
				{
					Files:           globalFileSDConfig.Files,
					RefreshInterval: globalFileSDConfig.RefreshInterval,
				},
			}
		}
		endpointConfig = append(endpointConfig, cfg)
	}

	// Adding --store-strict endpoints, if provided.
	// Global TLS config applies until deprecated.
	if len(strictEndpointAddrs) > 0 {
		cfg := EndpointConfig{}
		cfg.GRPCClientConfig.TLSConfig = globalTLSConfig
		cfg.EndpointsConfig.Addresses = strictEndpointAddrs
		cfg.Mode = StrictEndpointMode
		endpointConfig = append(endpointConfig, cfg)
	}

	// Checking for duplicates.
	// NOTE: This does not check dynamic endpoints of course.
	allEndpoints := make(map[string]struct{})
	for _, config := range endpointConfig {
		for _, addr := range config.EndpointsConfig.Addresses {
			if _, exists := allEndpoints[addr]; exists {
				return nil, errors.Errorf("%s endpoint provided more than once", addr)
			}
			allEndpoints[addr] = struct{}{}
		}
	}
	return endpointConfig, nil
}
