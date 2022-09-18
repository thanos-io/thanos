package query

import (
	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/file"
	"gopkg.in/yaml.v2"
)

type EndpointConfig struct {
	Endpoints   []string       `yaml:"addresses"`
	EndpointsSD *file.SDConfig `yaml:"file_sd_configs"`
}

// LoadConfig returns list of per-endpoint TLS config.
func LoadConfig(logger log.Logger, confYAML []byte, endpointAddrs []string, globalFileSDFiles []string, globalFileSDInterval model.Duration) ([]EndpointConfig, error) {
	var endpointConfig []EndpointConfig

	if len(confYAML) > 0 {
		if err := yaml.UnmarshalStrict(confYAML, &endpointConfig); err != nil {
			return nil, err
		}

	}

	// Adding --store, rule, metadata, target, exemplar and --store.sd-files, if provided.
	// Global TLS config applies until deprecated.
	if len(endpointAddrs) > 0 || len(globalFileSDFiles) > 0 {
		cfg := EndpointConfig{}
		cfg.Endpoints = endpointAddrs

		cfg.EndpointsSD = &file.SDConfig{
			Files:           globalFileSDFiles,
			RefreshInterval: globalFileSDInterval,
		}
		endpointConfig = append(endpointConfig, cfg)
	}

	return endpointConfig, nil
}
