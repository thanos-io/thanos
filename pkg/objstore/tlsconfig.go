// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objstore

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"

	"github.com/thanos-io/thanos/pkg/httpconfig"
)

// NewTLSConfig creates a new tls.Config from the given TLSConfig.
func NewTLSConfig(cfg *httpconfig.TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify}

	// If a CA cert is provided then let's read it in.
	if len(cfg.CAFile) > 0 {
		b, err := readCAFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		if !updateRootCA(tlsConfig, b) {
			return nil, fmt.Errorf("unable to use specified CA cert %s", cfg.CAFile)
		}
	}

	if len(cfg.ServerName) > 0 {
		tlsConfig.ServerName = cfg.ServerName
	}
	// If a client cert & key is provided then configure TLS config accordingly.
	if len(cfg.CertFile) > 0 && len(cfg.KeyFile) == 0 {
		return nil, fmt.Errorf("client cert file %q specified without client key file", cfg.CertFile)
	} else if len(cfg.KeyFile) > 0 && len(cfg.CertFile) == 0 {
		return nil, fmt.Errorf("client key file %q specified without client cert file", cfg.KeyFile)
	} else if len(cfg.CertFile) > 0 && len(cfg.KeyFile) > 0 {
		// Verify that client cert and key are valid.
		if _, err := cfg.GetClientCertificate(nil); err != nil {
			return nil, err
		}
		tlsConfig.GetClientCertificate = cfg.GetClientCertificate
	}

	return tlsConfig, nil
}

// readCAFile reads the CA cert file from disk.
func readCAFile(f string) ([]byte, error) {
	data, err := ioutil.ReadFile(f)
	if err != nil {
		return nil, fmt.Errorf("unable to load specified CA cert %s: %s", f, err)
	}
	return data, nil
}

// updateRootCA parses the given byte slice as a series of PEM encoded certificates and updates tls.Config.RootCAs.
func updateRootCA(cfg *tls.Config, b []byte) bool {
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(b) {
		return false
	}
	cfg.RootCAs = caCertPool
	return true
}
