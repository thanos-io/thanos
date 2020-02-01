// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tls

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// NewServerConfig provides new server TLS configuration.
func NewServerConfig(logger log.Logger, cert, key, clientCA string) (*tls.Config, error) {
	if key == "" && cert == "" {
		if clientCA != "" {
			return nil, errors.New("when a client CA is used a server key and certificate must also be provided")
		}

		level.Info(logger).Log("msg", "disabled TLS, key and cert must be set to enable")
		return nil, nil
	}

	level.Info(logger).Log("msg", "enabling server side TLS")

	if key == "" || cert == "" {
		return nil, errors.New("both server key and certificate must be provided")
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	tlsCert, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, errors.Wrap(err, "server credentials")
	}

	tlsCfg.Certificates = []tls.Certificate{tlsCert}

	if clientCA != "" {
		caPEM, err := ioutil.ReadFile(clientCA)
		if err != nil {
			return nil, errors.Wrap(err, "reading client CA")
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caPEM) {
			return nil, errors.Wrap(err, "building client CA")
		}
		tlsCfg.ClientCAs = certPool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert

		level.Info(logger).Log("msg", "server TLS client verification enabled")
	}

	return tlsCfg, nil
}

// NewClientConfig provides new client TLS configuration.
func NewClientConfig(logger log.Logger, cert, key, caCert, serverName string) (*tls.Config, error) {
	var certPool *x509.CertPool
	if caCert != "" {
		caPEM, err := ioutil.ReadFile(caCert)
		if err != nil {
			return nil, errors.Wrap(err, "reading client CA")
		}

		certPool = x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caPEM) {
			return nil, errors.Wrap(err, "building client CA")
		}
		level.Info(logger).Log("msg", "TLS client using provided certificate pool")
	} else {
		var err error
		certPool, err = x509.SystemCertPool()
		if err != nil {
			return nil, errors.Wrap(err, "reading system certificate pool")
		}
		level.Info(logger).Log("msg", "TLS client using system certificate pool")
	}

	tlsCfg := &tls.Config{
		RootCAs: certPool,
	}

	if serverName != "" {
		tlsCfg.ServerName = serverName
	}

	if (key != "") != (cert != "") {
		return nil, errors.New("both client key and certificate must be provided")
	}

	if cert != "" {
		cert, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, errors.Wrap(err, "client credentials")
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
		level.Info(logger).Log("msg", "TLS client authentication enabled")
	}
	return tlsCfg, nil
}
