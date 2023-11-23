// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package alert

import (
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/relabel"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/clientconfig"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
)

type AlertingConfig struct {
	Alertmanagers []AlertmanagerConfig `yaml:"alertmanagers"`
}

// AlertmanagerConfig represents a client to a cluster of Alertmanager endpoints.
type AlertmanagerConfig struct {
	HTTPClientConfig clientconfig.HTTPClientConfig    `yaml:"http_config"`
	EndpointsConfig  clientconfig.HTTPEndpointsConfig `yaml:",inline"`
	Timeout          model.Duration                   `yaml:"timeout"`
	APIVersion       APIVersion                       `yaml:"api_version"`
}

// APIVersion represents the API version of the Alertmanager endpoint.
type APIVersion string

const (
	APIv1 APIVersion = "v1"
	APIv2 APIVersion = "v2"
)

var supportedAPIVersions = []APIVersion{
	APIv1, APIv2,
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (v *APIVersion) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return errors.Wrap(err, "invalid Alertmanager API version")
	}

	for _, ver := range supportedAPIVersions {
		if APIVersion(s) == ver {
			*v = ver
			return nil
		}
	}
	return errors.Errorf("expected Alertmanager API version to be one of %v but got %q", supportedAPIVersions, s)
}

func DefaultAlertmanagerConfig() AlertmanagerConfig {
	return AlertmanagerConfig{
		EndpointsConfig: clientconfig.HTTPEndpointsConfig{
			Scheme:          "http",
			StaticAddresses: []string{},
			FileSDConfigs:   []clientconfig.HTTPFileSDConfig{},
		},
		Timeout:    model.Duration(time.Second * 10),
		APIVersion: APIv1,
	}
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *AlertmanagerConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultAlertmanagerConfig()
	type plain AlertmanagerConfig
	return unmarshal((*plain)(c))
}

// LoadAlertingConfig loads a list of AlertmanagerConfig from YAML data.
func LoadAlertingConfig(confYaml []byte) (AlertingConfig, error) {
	var cfg AlertingConfig
	if err := yaml.UnmarshalStrict(confYaml, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// BuildAlertmanagerConfig initializes and returns an Alertmanager client configuration from a static address.
func BuildAlertmanagerConfig(address string, timeout time.Duration) (AlertmanagerConfig, error) {
	parsed, err := url.Parse(address)
	if err != nil {
		return AlertmanagerConfig{}, err
	}

	scheme := parsed.Scheme
	if scheme == "" {
		return AlertmanagerConfig{}, errors.New("alertmanagers.url contains empty scheme")
	}

	host := parsed.Host
	if host == "" {
		return AlertmanagerConfig{}, errors.New("alertmanagers.url contains empty host")
	}

	for _, qType := range []dns.QType{dns.A, dns.SRV, dns.SRVNoA} {
		prefix := string(qType) + "+"
		if strings.HasPrefix(strings.ToLower(scheme), prefix) {
			// Scheme is of the form "<dns type>+<http scheme>".
			scheme = strings.TrimPrefix(scheme, prefix)
			host = prefix + parsed.Host
			if qType == dns.A {
				if _, _, err := net.SplitHostPort(parsed.Host); err != nil {
					// The host port could be missing. Append the defaultAlertmanagerPort.
					host = host + ":" + strconv.Itoa(defaultAlertmanagerPort)
				}
			}
			break
		}
	}
	var basicAuth clientconfig.BasicAuth
	if parsed.User != nil && parsed.User.String() != "" {
		basicAuth.Username = parsed.User.Username()
		pw, _ := parsed.User.Password()
		basicAuth.Password = pw
	}

	return AlertmanagerConfig{
		HTTPClientConfig: clientconfig.HTTPClientConfig{
			BasicAuth: basicAuth,
		},
		EndpointsConfig: clientconfig.HTTPEndpointsConfig{
			PathPrefix:      parsed.Path,
			Scheme:          scheme,
			StaticAddresses: []string{host},
		},
		Timeout:    model.Duration(timeout),
		APIVersion: APIv1,
	}, nil
}

// LoadRelabelConfigs loads a list of relabel.Config from YAML data.
func LoadRelabelConfigs(confYaml []byte) ([]*relabel.Config, error) {
	var cfg []*relabel.Config
	if err := yaml.UnmarshalStrict(confYaml, &cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
