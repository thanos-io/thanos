// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tls

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
)

// NewServerConfig provides new server TLS configuration.
func NewServerConfig(logger log.Logger, certPath, keyPath, clientCA string) (*tls.Config, error) {
	if keyPath == "" && certPath == "" {
		if clientCA != "" {
			return nil, errors.New("when a client CA is used a server key and certificate must also be provided")
		}

		level.Info(logger).Log("msg", "disabled TLS, key and cert must be set to enable")
		return nil, nil
	}

	level.Info(logger).Log("msg", "enabling server side TLS")

	if keyPath == "" || certPath == "" {
		return nil, errors.New("both server key and certificate must be provided")
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
	}
	// Certificate is loaded during server startup to check for any errors.
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, errors.Wrap(err, "server credentials")
	}

	mngr := &serverTLSManager{
		srvCertPath: certPath,
		srvKeyPath:  keyPath,
		srvCert:     &certificate,
	}

	tlsCfg.GetCertificate = mngr.getCertificate

	if clientCA != "" {
		caPEM, err := os.ReadFile(filepath.Clean(clientCA))
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

type serverTLSManager struct {
	srvCertPath string
	srvKeyPath  string

	mtx            sync.Mutex
	srvCert        *tls.Certificate
	srvCertModTime time.Time
	srvKeyModTime  time.Time
}

func (m *serverTLSManager) getCertificate(clientHello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	statCert, err := os.Stat(m.srvCertPath)
	if err != nil {
		return nil, err
	}
	statKey, err := os.Stat(m.srvKeyPath)
	if err != nil {
		return nil, err
	}

	if m.srvCert == nil || !statCert.ModTime().Equal(m.srvCertModTime) || !statKey.ModTime().Equal(m.srvKeyModTime) {
		cert, err := tls.LoadX509KeyPair(m.srvCertPath, m.srvKeyPath)
		if err != nil {
			return nil, errors.Wrap(err, "server credentials")
		}
		m.srvCertModTime = statCert.ModTime()
		m.srvKeyModTime = statKey.ModTime()
		m.srvCert = &cert
	}
	return m.srvCert, nil
}

// NewClientConfig provides new client TLS configuration.
func NewClientConfig(logger log.Logger, cert, key, caCert, serverName string, skipVerify bool) (*tls.Config, error) {
	var certPool *x509.CertPool
	if caCert != "" {
		caPEM, err := os.ReadFile(filepath.Clean(caCert))
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

	if skipVerify {
		tlsCfg.InsecureSkipVerify = true
	}

	if (key != "") != (cert != "") {
		return nil, errors.New("both client key and certificate must be provided")
	}

	if cert != "" {
		mngr := &clientTLSManager{
			certPath: cert,
			keyPath:  key,
		}
		tlsCfg.GetClientCertificate = mngr.getClientCertificate

		level.Info(logger).Log("msg", "TLS client authentication enabled")
	}
	return tlsCfg, nil
}

type clientTLSManager struct {
	certPath string
	keyPath  string

	mtx         sync.Mutex
	cert        *tls.Certificate
	certModTime time.Time
	keyModTime  time.Time
}

func (m *clientTLSManager) getClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	statCert, err := os.Stat(m.certPath)
	if err != nil {
		return nil, err
	}
	statKey, err := os.Stat(m.keyPath)
	if err != nil {
		return nil, err
	}

	if m.cert == nil || !statCert.ModTime().Equal(m.certModTime) || !statKey.ModTime().Equal(m.keyModTime) {
		cert, err := tls.LoadX509KeyPair(m.certPath, m.keyPath)
		if err != nil {
			return nil, errors.Wrap(err, "client credentials")
		}
		m.certModTime = statCert.ModTime()
		m.keyModTime = statKey.ModTime()
		m.cert = &cert
	}

	return m.cert, nil
}
