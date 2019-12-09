package tls

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/go-kit/kit/log"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// NewServerConfig returns a new server tls config with auto reloading certificates.
// It reload the key and certificates when the local mod time of any file has changed.
func NewServerConfig(logger log.Logger, srvCertPath, srvKeyPath, cltCApath string) (*tls.Config, error) {
	if srvCertPath == "" && srvKeyPath == "" {
		if cltCApath != "" {
			return nil, errors.New("when a client CA is used a server key and certificate must also be provided")
		}

		level.Info(logger).Log("msg", "disabled TLS, key and cert must be set to enable")
		return nil, nil
	}

	level.Info(logger).Log("msg", "enabling server side TLS")

	if srvCertPath == "" || srvKeyPath == "" {
		return nil, errors.New("both server key and certificate path must be provided")
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	mngr := &serverTLSManager{
		srvCertPath:  srvCertPath,
		srvKeyPath:   srvKeyPath,
		cltCApath:    cltCApath,
		cltTLSConfig: tlsCfg,
	}
	// TODO (krasi) use GetConfigForServer when add in the std.
	// https://github.com/golang/go/issues/22836
	// With the current implementation the CA's can't be auto reloaded without a restart.
	tlsCfg.GetCertificate = mngr.getCertificate
	level.Info(logger).Log("msg", "enabled server side TLS")

	if cltCApath != "" {
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		tlsCfg.GetConfigForClient = mngr.getConfigForClient
		level.Info(logger).Log("msg", "server TLS client verification enabled")
	}

	return tlsCfg, nil
}

type serverTLSManager struct {
	srvCertPath,
	srvKeyPath,
	cltCApath string

	mtxSrv         sync.Mutex
	srvCert        *tls.Certificate
	srvCertModTime time.Time
	srvKeyModTime  time.Time

	mtxClt       sync.Mutex
	cltTLSConfig *tls.Config
	cltCAmodTime time.Time
}

func (m *serverTLSManager) getCertificate(clientHello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	m.mtxSrv.Lock()
	defer m.mtxSrv.Unlock()

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
			return nil, errors.Wrap(err, "loading server cert and key")
		}
		m.srvCertModTime = statCert.ModTime()
		m.srvKeyModTime = statKey.ModTime()
		m.srvCert = &cert
	}

	return m.srvCert, nil
}

func (m *serverTLSManager) getConfigForClient(*tls.ClientHelloInfo) (*tls.Config, error) {
	m.mtxClt.Lock()
	defer m.mtxClt.Unlock()

	statCA, err := os.Stat(m.cltCApath)
	if err != nil {
		return nil, err
	}

	if m.cltTLSConfig.ClientCAs == nil || !statCA.ModTime().Equal(m.cltCAmodTime) {
		caPEM, err := ioutil.ReadFile(m.cltCApath)
		if err != nil {
			return nil, errors.Wrap(err, "reading client CA")
		}

		m.cltTLSConfig.ClientCAs = x509.NewCertPool()
		if !m.cltTLSConfig.ClientCAs.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("adding CA file to the client pool")
		}
		m.cltCAmodTime = statCA.ModTime()
	}

	return m.cltTLSConfig, nil
}

// NewClientConfig returns a new client tls config with auto reloading certificates.
// It reload the key and certificates when the local mod time of any file has changed.
func NewClientConfig(logger log.Logger, certPath, keyPath, caCertPath, serverName string) (*tls.Config, error) {
	if (keyPath != "") != (certPath != "") {
		return nil, errors.New("both client key and certificate must be provided")
	}

	var certPool *x509.CertPool
	if caCertPath != "" {
		caPEM, err := ioutil.ReadFile(caCertPath)
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

	if (keyPath != "") != (certPath != "") {
		return nil, errors.New("both client key and certificate must be provided or both should be empty")
	}

	if certPath != "" {
		mngr := &clientTLSManager{
			certPath: certPath,
			keyPath:  keyPath,
		}
		// TODO (krasi) implement CA rotation when added in the std.
		// With the current implementation the CA's can't be auto reloaded without a restart.
		tlsCfg.GetClientCertificate = mngr.getClientCertificate
		level.Info(logger).Log("msg", "TLS client authentication enabled")
	}
	return tlsCfg, nil
}

type clientTLSManager struct {
	certPath,
	keyPath string

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
			return nil, errors.Wrap(err, "loading server cert and key")
		}
		m.certModTime = statCert.ModTime()
		m.keyModTime = statKey.ModTime()
		m.cert = &cert
	}

	return m.cert, nil
}
