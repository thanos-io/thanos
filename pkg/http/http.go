// Package http is a wrapper around github.com/prometheus/common/config.
package http

import (
	"fmt"
	"net/http"

	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/version"
	"gopkg.in/yaml.v2"
)

// ClientConfig configures an HTTP client.
type ClientConfig struct {
	// The HTTP basic authentication credentials for the targets.
	BasicAuth BasicAuth `yaml:"basic_auth"`
	// The bearer token for the targets.
	BearerToken string `yaml:"bearer_token"`
	// The bearer token file for the targets.
	BearerTokenFile string `yaml:"bearer_token_file"`
	// HTTP proxy server to use to connect to the targets.
	ProxyURL string `yaml:"proxy_url"`
	// TLSConfig to use to connect to the targets.
	TLSConfig TLSConfig `yaml:"tls_config"`
}

// TLSConfig configures TLS connections.
type TLSConfig struct {
	// The CA cert to use for the targets.
	CAFile string `yaml:"ca_file"`
	// The client cert file for the targets.
	CertFile string `yaml:"cert_file"`
	// The client key file for the targets.
	KeyFile string `yaml:"key_file"`
	// Used to verify the hostname for the targets.
	ServerName string `yaml:"server_name"`
	// Disable target certificate validation.
	InsecureSkipVerify bool `yaml:"insecure_skip_verify"`
}

// BasicAuth configures basic authentication for HTTP clients.
type BasicAuth struct {
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	PasswordFile string `yaml:"password_file"`
}

// IsZero returns false if basic authentication isn't enabled.
func (b BasicAuth) IsZero() bool {
	return b.Username == "" && b.Password == "" && b.PasswordFile == ""
}

// NewClient returns a new HTTP client.
func NewClient(cfg ClientConfig, name string) (*http.Client, error) {
	httpClientConfig := config_util.HTTPClientConfig{
		BearerToken:     config_util.Secret(cfg.BearerToken),
		BearerTokenFile: cfg.BearerTokenFile,
		TLSConfig: config_util.TLSConfig{
			CAFile:             cfg.TLSConfig.CAFile,
			CertFile:           cfg.TLSConfig.CertFile,
			KeyFile:            cfg.TLSConfig.KeyFile,
			ServerName:         cfg.TLSConfig.ServerName,
			InsecureSkipVerify: cfg.TLSConfig.InsecureSkipVerify,
		},
	}
	if cfg.ProxyURL != "" {
		var proxy config_util.URL
		err := yaml.Unmarshal([]byte(cfg.ProxyURL), &proxy)
		if err != nil {
			return nil, err
		}
		httpClientConfig.ProxyURL = proxy
	}
	if !cfg.BasicAuth.IsZero() {
		httpClientConfig.BasicAuth = &config_util.BasicAuth{
			Username:     cfg.BasicAuth.Username,
			Password:     config_util.Secret(cfg.BasicAuth.Password),
			PasswordFile: cfg.BasicAuth.PasswordFile,
		}
	}
	if err := httpClientConfig.Validate(); err != nil {
		return nil, err
	}

	client, err := config_util.NewClientFromConfig(httpClientConfig, name, false)
	if err != nil {
		return nil, err
	}
	client.Transport = &userAgentRoundTripper{name: userAgent, rt: client.Transport}
	return client, nil
}

var userAgent = fmt.Sprintf("Thanos/%s", version.Version)

type userAgentRoundTripper struct {
	name string
	rt   http.RoundTripper
}

// RoundTrip implements the http.RoundTripper interface.
func (u userAgentRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.UserAgent() == "" {
		// The specification of http.RoundTripper says that it shouldn't mutate
		// the request so make a copy of req.Header since this is all that is
		// modified.
		r2 := new(http.Request)
		*r2 = *r
		r2.Header = make(http.Header)
		for k, s := range r.Header {
			r2.Header[k] = s
		}
		r2.Header.Set("User-Agent", u.name)
		r = r2
	}
	return u.rt.RoundTrip(r)
}
