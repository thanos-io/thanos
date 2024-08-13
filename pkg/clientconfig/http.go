// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package clientconfig

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"

	"github.com/go-kit/log"
	"github.com/mwitkow/go-conntrack"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/file"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"golang.org/x/net/http2"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/discovery/cache"
)

// HTTPConfig is a structure that allows pointing to various HTTP endpoint, e.g ruler connecting to queriers.
type HTTPConfig struct {
	HTTPClientConfig HTTPClientConfig    `yaml:"http_config"`
	EndpointsConfig  HTTPEndpointsConfig `yaml:",inline"`
}

func (c *HTTPConfig) NotEmpty() bool {
	return len(c.EndpointsConfig.FileSDConfigs) > 0 || len(c.EndpointsConfig.StaticAddresses) > 0
}

// HTTPClientConfig configures an HTTP client.
type HTTPClientConfig struct {
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
	// TransportConfig for Client transport properties
	TransportConfig TransportConfig `yaml:"transport_config"`
	// ClientMetrics contains metrics that will be used to instrument
	// the client that will be created with this config.
	ClientMetrics *extpromhttp.ClientMetrics `yaml:"-"`
}

// TLSConfig configures TLS connections.
type TLSConfig struct {
	// The CA cert to use for the targets.
	CAFile string `yaml:"ca_file"`
	// The client cert file for the targets.
	CertFile string `yaml:"cert_file"`
	// The client key file for the targets.
	KeyFile string `yaml:"key_file"`
	// Used to verify the hostname for the targets. See https://tools.ietf.org/html/rfc4366#section-3.1
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

// TransportConfig configures client's transport properties.
type TransportConfig struct {
	MaxIdleConns          int   `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost   int   `yaml:"max_idle_conns_per_host"`
	IdleConnTimeout       int64 `yaml:"idle_conn_timeout"`
	ResponseHeaderTimeout int64 `yaml:"response_header_timeout"`
	ExpectContinueTimeout int64 `yaml:"expect_continue_timeout"`
	MaxConnsPerHost       int   `yaml:"max_conns_per_host"`
	DisableCompression    bool  `yaml:"disable_compression"`
	TLSHandshakeTimeout   int64 `yaml:"tls_handshake_timeout"`
	DialerTimeout         int64 `yaml:"dialer_timeout"`
}

var defaultTransportConfig TransportConfig = TransportConfig{
	MaxIdleConns:          100,
	MaxIdleConnsPerHost:   2,
	ResponseHeaderTimeout: 0,
	MaxConnsPerHost:       0,
	IdleConnTimeout:       int64(90 * time.Second),
	ExpectContinueTimeout: int64(10 * time.Second),
	DisableCompression:    false,
	TLSHandshakeTimeout:   int64(10 * time.Second),
	DialerTimeout:         int64(5 * time.Second),
}

func NewDefaultHTTPClientConfig() HTTPClientConfig {
	return HTTPClientConfig{TransportConfig: defaultTransportConfig}
}

func NewHTTPClientConfigFromYAML(cfg []byte) (*HTTPClientConfig, error) {
	conf := &HTTPClientConfig{TransportConfig: defaultTransportConfig}
	if err := yaml.Unmarshal(cfg, conf); err != nil {
		return nil, err
	}
	return conf, nil
}

// NewRoundTripperFromConfig returns a new HTTP RoundTripper configured for the
// given http.HTTPClientConfig and http.HTTPClientOption.
func NewRoundTripperFromConfig(cfg config_util.HTTPClientConfig, transportConfig TransportConfig, name string) (http.RoundTripper, error) {
	newRT := func(tlsConfig *tls.Config) (http.RoundTripper, error) {
		var rt http.RoundTripper = &http.Transport{
			Proxy:                 http.ProxyURL(cfg.ProxyURL.URL),
			MaxIdleConns:          transportConfig.MaxIdleConns,
			MaxIdleConnsPerHost:   transportConfig.MaxIdleConnsPerHost,
			MaxConnsPerHost:       transportConfig.MaxConnsPerHost,
			TLSClientConfig:       tlsConfig,
			DisableCompression:    transportConfig.DisableCompression,
			IdleConnTimeout:       time.Duration(transportConfig.IdleConnTimeout),
			ResponseHeaderTimeout: time.Duration(transportConfig.ResponseHeaderTimeout),
			ExpectContinueTimeout: time.Duration(transportConfig.ExpectContinueTimeout),
			TLSHandshakeTimeout:   time.Duration(transportConfig.TLSHandshakeTimeout),
			DialContext: conntrack.NewDialContextFunc(
				conntrack.DialWithDialer(&net.Dialer{
					Timeout: time.Duration(transportConfig.DialerTimeout),
				}),
				conntrack.DialWithTracing(),
				conntrack.DialWithName(name)),
		}

		// HTTP/2 support is golang has many problematic cornercases where
		// dead connections would be kept and used in connection pools.
		// https://github.com/golang/go/issues/32388
		// https://github.com/golang/go/issues/39337
		// https://github.com/golang/go/issues/39750
		// TODO: Re-Enable HTTP/2 once upstream issue is fixed.
		// TODO: use ForceAttemptHTTP2 when we move to Go 1.13+.
		err := http2.ConfigureTransport(rt.(*http.Transport))
		if err != nil {
			return nil, err
		}

		// If an authorization_credentials is provided, create a round tripper that will set the
		// Authorization header correctly on each request.
		if cfg.Authorization != nil && len(cfg.Authorization.Credentials) > 0 {
			rt = config_util.NewAuthorizationCredentialsRoundTripper(cfg.Authorization.Type, config_util.NewInlineSecret(string(cfg.Authorization.Credentials)), rt)
		} else if cfg.Authorization != nil && len(cfg.Authorization.CredentialsFile) > 0 {
			rt = config_util.NewAuthorizationCredentialsRoundTripper(cfg.Authorization.Type, config_util.NewFileSecret(cfg.Authorization.CredentialsFile), rt)
		}
		// Backwards compatibility, be nice with importers who would not have
		// called Validate().
		if len(cfg.BearerToken) > 0 {
			rt = config_util.NewAuthorizationCredentialsRoundTripper("Bearer", config_util.NewInlineSecret(string(cfg.BearerToken)), rt)
		} else if len(cfg.BearerTokenFile) > 0 {
			rt = config_util.NewAuthorizationCredentialsRoundTripper("Bearer", config_util.NewFileSecret(cfg.BearerTokenFile), rt)
		}

		if cfg.BasicAuth != nil {
			// TODO(yeya24): expose UsernameFile as a config.
			username := config_util.NewInlineSecret(cfg.BasicAuth.Username)
			var password config_util.SecretReader
			if len(cfg.BasicAuth.PasswordFile) > 0 {
				password = config_util.NewFileSecret(cfg.BasicAuth.PasswordFile)
			} else {
				password = config_util.NewInlineSecret(string(cfg.BasicAuth.Password))
			}
			rt = config_util.NewBasicAuthRoundTripper(username, password, rt)
		}
		// Return a new configured RoundTripper.
		return rt, nil
	}

	tlsConfig, err := config_util.NewTLSConfig(&cfg.TLSConfig)
	if err != nil {
		return nil, err
	}

	if len(cfg.TLSConfig.CAFile) == 0 {
		// No need for a RoundTripper that reloads the CA file automatically.
		return newRT(tlsConfig)
	}

	rtConfig := config_util.TLSRoundTripperSettings{
		Cert: config_util.NewFileSecret(cfg.TLSConfig.CAFile),
	}
	if len(cfg.TLSConfig.CertFile) > 0 {
		rtConfig.Cert = config_util.NewFileSecret(cfg.TLSConfig.CertFile)
	}

	if len(cfg.TLSConfig.KeyFile) > 0 {
		rtConfig.Key = config_util.NewFileSecret(cfg.TLSConfig.KeyFile)
	}

	return config_util.NewTLSRoundTripper(tlsConfig, rtConfig, newRT)
}

// NewHTTPClient returns a new HTTP client.
func NewHTTPClient(cfg HTTPClientConfig, name string) (*http.Client, error) {
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

	if cfg.BearerToken != "" {
		httpClientConfig.BearerToken = config_util.Secret(cfg.BearerToken)
	}

	if cfg.BearerTokenFile != "" {
		httpClientConfig.BearerTokenFile = cfg.BearerTokenFile
	}

	if err := httpClientConfig.Validate(); err != nil {
		return nil, err
	}

	rt, err := NewRoundTripperFromConfig(
		httpClientConfig,
		cfg.TransportConfig,
		name,
	)
	if err != nil {
		return nil, err
	}

	if cfg.ClientMetrics != nil {
		rt = extpromhttp.InstrumentedRoundTripper(rt, cfg.ClientMetrics)
	}

	rt = &userAgentRoundTripper{name: ThanosUserAgent, rt: rt}
	client := &http.Client{Transport: rt}

	return client, nil
}

var ThanosUserAgent = fmt.Sprintf("Thanos/%s", version.Version)

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

// HTTPEndpointsConfig configures a cluster of HTTP endpoints from static addresses and
// file service discovery.
type HTTPEndpointsConfig struct {
	// List of addresses with DNS prefixes.
	StaticAddresses []string `yaml:"static_configs"`
	// List of file  configurations (our FileSD supports different DNS lookups).
	FileSDConfigs []HTTPFileSDConfig `yaml:"file_sd_configs"`

	// The URL scheme to use when talking to targets.
	Scheme string `yaml:"scheme"`

	// Path prefix to add in front of the endpoint path.
	PathPrefix string `yaml:"path_prefix"`
}

// HTTPFileSDConfig represents a file service discovery configuration.
type HTTPFileSDConfig struct {
	Files           []string       `yaml:"files"`
	RefreshInterval model.Duration `yaml:"refresh_interval"`
}

func (c HTTPFileSDConfig) convert() (file.SDConfig, error) {
	var fileSDConfig file.SDConfig
	b, err := yaml.Marshal(c)
	if err != nil {
		return fileSDConfig, err
	}
	err = yaml.Unmarshal(b, &fileSDConfig)
	return fileSDConfig, err
}

type AddressProvider interface {
	Resolve(context.Context, []string) error
	Addresses() []string
}

// HTTPClient represents a client that can send requests to a cluster of HTTP-based endpoints.
type HTTPClient struct {
	logger log.Logger

	httpClient *http.Client
	scheme     string
	prefix     string

	staticAddresses []string
	fileSDCache     *cache.Cache
	fileDiscoverers []*file.Discovery

	provider AddressProvider
}

// NewClient returns a new Client.
func NewClient(logger log.Logger, cfg HTTPEndpointsConfig, client *http.Client, provider AddressProvider) (*HTTPClient, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	var discoverers []*file.Discovery
	for _, sdCfg := range cfg.FileSDConfigs {
		fileSDCfg, err := sdCfg.convert()
		if err != nil {
			return nil, err
		}
		// We provide an empty registry and ignore metrics for now.
		sdReg := prometheus.NewRegistry()
		fileSD, err := file.NewDiscovery(&fileSDCfg, logger, fileSDCfg.NewDiscovererMetrics(sdReg, discovery.NewRefreshMetrics(sdReg)))
		if err != nil {
			return nil, err
		}
		discoverers = append(discoverers, fileSD)
	}
	return &HTTPClient{
		logger:          logger,
		httpClient:      client,
		scheme:          cfg.Scheme,
		prefix:          cfg.PathPrefix,
		staticAddresses: cfg.StaticAddresses,
		fileSDCache:     cache.New(),
		fileDiscoverers: discoverers,
		provider:        provider,
	}, nil
}

// Do executes an HTTP request with the underlying HTTP client.
func (c *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	return c.httpClient.Do(req)
}

// Endpoints returns the list of known endpoints.
func (c *HTTPClient) Endpoints() []*url.URL {
	var urls []*url.URL
	for _, addr := range c.provider.Addresses() {
		urls = append(urls,
			&url.URL{
				Scheme: c.scheme,
				Host:   addr,
				Path:   path.Join("/", c.prefix),
			},
		)
	}
	return urls
}

// Discover runs the service to discover endpoints until the given context is done.
func (c *HTTPClient) Discover(ctx context.Context) {
	var wg sync.WaitGroup
	ch := make(chan []*targetgroup.Group)

	for _, d := range c.fileDiscoverers {
		wg.Add(1)
		go func(d *file.Discovery) {
			d.Run(ctx, ch)
			wg.Done()
		}(d)
	}

	func() {
		for {
			select {
			case update := <-ch:
				// Discoverers sometimes send nil updates so need to check for it to avoid panics.
				if update == nil {
					continue
				}
				c.fileSDCache.Update(update)
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()
}

// Resolve refreshes and resolves the list of targets.
func (c *HTTPClient) Resolve(ctx context.Context) error {
	return c.provider.Resolve(ctx, append(c.fileSDCache.Addresses(), c.staticAddresses...))
}
