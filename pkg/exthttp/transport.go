// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exthttp

import (
	"net"
	"net/http"
	"time"

	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type HTTPConfig struct {
	IdleConnTimeout       model.Duration `yaml:"idle_conn_timeout"`
	ResponseHeaderTimeout model.Duration `yaml:"response_header_timeout"`
	InsecureSkipVerify    bool           `yaml:"insecure_skip_verify"`

	TLSHandshakeTimeout   model.Duration `yaml:"tls_handshake_timeout"`
	ExpectContinueTimeout model.Duration `yaml:"expect_continue_timeout"`
	MaxIdleConns          int            `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost   int            `yaml:"max_idle_conns_per_host"`
	MaxConnsPerHost       int            `yaml:"max_conns_per_host"`

	// Allow upstream callers to inject a round tripper
	Transport http.RoundTripper `yaml:"-"`

	TLSConfig objstore.TLSConfig `yaml:"tls_config"`
}

// NewTransport creates a new http.Transport with default settings.
func NewTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// DefaultTransport - this default transport is based on the Minio
// DefaultTransport up until the following commit:
// https://github.com/minio/minio-go/commit/008c7aa71fc17e11bf980c209a4f8c4d687fc884
// The values have since diverged.
func DefaultTransport(config HTTPConfig) (*http.Transport, error) {
	tlsConfig, err := objstore.NewTLSConfig(&config.TLSConfig)
	if err != nil {
		return nil, err
	}

	if config.InsecureSkipVerify {
		tlsConfig.InsecureSkipVerify = true
	}

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
		// A custom ResponseHeaderTimeout was introduced
		// to cover cases where the tcp connection works but
		// the server never answers. Defaults to 2 minutes.
		ResponseHeaderTimeout: time.Duration(config.ResponseHeaderTimeout),
		// Set this value so that the underlying transport round-tripper
		// doesn't try to auto decode the body of objects with
		// content-encoding set to `gzip`.
		//
		// Refer: https://golang.org/src/net/http/transport.go?h=roundTrip#L1843.
		DisableCompression: true,
		TLSClientConfig:    tlsConfig,
	}, nil
}
