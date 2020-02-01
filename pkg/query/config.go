// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"gopkg.in/yaml.v2"

	http_util "github.com/thanos-io/thanos/pkg/http"
)

type Config struct {
	HTTPClientConfig http_util.ClientConfig    `yaml:"http_config"`
	EndpointsConfig  http_util.EndpointsConfig `yaml:",inline"`
}

func DefaultConfig() Config {
	return Config{
		EndpointsConfig: http_util.EndpointsConfig{
			Scheme:          "http",
			StaticAddresses: []string{},
			FileSDConfigs:   []http_util.FileSDConfig{},
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
	var queryCfg []Config
	if err := yaml.UnmarshalStrict(confYAML, &queryCfg); err != nil {
		return nil, err
	}
	return queryCfg, nil
}
