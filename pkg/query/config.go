// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"fmt"
	"net/url"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/pkg/errors"
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

// BuildQueryConfig returns a query client configuration from a static address.
func BuildQueryConfig(queryAddrs []string) ([]Config, error) {
	configs := make([]Config, 0, len(queryAddrs))
	for i, addr := range queryAddrs {
		if addr == "" {
			return nil, errors.Errorf("static querier address cannot be empty at index %d", i)
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
			return nil, errors.Errorf("%q is not supported scheme for querier address", u.Scheme)
		}
		configs = append(configs, Config{
			EndpointsConfig: http_util.EndpointsConfig{
				Scheme:          u.Scheme,
				StaticAddresses: []string{u.Host},
				PathPrefix:      u.Path,
			},
		})
	}
	return configs, nil
}
