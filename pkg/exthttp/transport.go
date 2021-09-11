// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exthttp

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/common/model"
)

var DefaultConfig = HTTPConfig{
	IdleConnTimeout:       model.Duration(90 * time.Second),
	ResponseHeaderTimeout: model.Duration(2 * time.Minute),
	TLSHandshakeTimeout:   model.Duration(10 * time.Second),
	ExpectContinueTimeout: model.Duration(1 * time.Second),
	MaxIdleConns:          100,
	MaxIdleConnsPerHost:   100,
	MaxConnsPerHost:       0,
}

// HTTPConfig stores the http.Transport configuration
type HTTPConfig struct {
	IdleConnTimeout       model.Duration `yaml:"idle_conn_timeout"`
	ResponseHeaderTimeout model.Duration `yaml:"response_header_timeout"`
	InsecureSkipVerify    bool           `yaml:"insecure_skip_verify"`

	TLSHandshakeTimeout   model.Duration `yaml:"tls_handshake_timeout"`
	ExpectContinueTimeout model.Duration `yaml:"expect_continue_timeout"`
	MaxIdleConns          int            `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost   int            `yaml:"max_idle_conns_per_host"`
	MaxConnsPerHost       int            `yaml:"max_conns_per_host"`
	DisableCompression    bool           `yaml:"disable_compression"`
	// Allow upstream callers to inject a round tripper
	Transport http.RoundTripper `yaml:"-"`
}

// NewTransport creates a new http.Transport with default settings.
func NewTransport() *http.Transport {
	transport := DefaultTransport(DefaultConfig)
	return transport
}

// Default Transport function that creates a http.Transport with specified settings.
func DefaultTransport(config HTTPConfig) *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,

		MaxIdleConns:          config.MaxIdleConns,
		MaxIdleConnsPerHost:   config.MaxIdleConnsPerHost,
		IdleConnTimeout:       time.Duration(config.IdleConnTimeout),
		MaxConnsPerHost:       config.MaxConnsPerHost,
		TLSHandshakeTimeout:   time.Duration(config.TLSHandshakeTimeout),
		ExpectContinueTimeout: time.Duration(config.ExpectContinueTimeout),
		ResponseHeaderTimeout: time.Duration(config.ResponseHeaderTimeout),
		DisableCompression:    config.DisableCompression,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: config.InsecureSkipVerify},
	}
}
