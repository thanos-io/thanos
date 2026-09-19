// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package clientconfig

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
)

func TestNewHTTPClientConfigFromYAML(t *testing.T) {
	for _, tc := range []struct {
		desc string
		cfg  HTTPClientConfig
		err  bool
	}{
		{
			desc: "empty string",
			cfg:  HTTPClientConfig{},
			err:  false,
		},
		{
			desc: "missing CA file",
			cfg: HTTPClientConfig{
				TLSConfig: TLSConfig{
					CAFile: "xxx",
				},
			},
			err: true,
		},
		{
			desc: "invalid CA file",
			cfg: HTTPClientConfig{
				TLSConfig: TLSConfig{
					CAFile: "testdata/invalid.pem",
				},
			},
			err: true,
		},
		{
			desc: "valid CA file",
			cfg: HTTPClientConfig{
				TLSConfig: TLSConfig{
					CAFile: "testdata/tls-ca-chain.pem",
				},
			},
			err: false,
		},
		{
			desc: "invalid cert file",
			cfg: HTTPClientConfig{
				TLSConfig: TLSConfig{
					CAFile:   "testdata/tls-ca-chain.pem",
					CertFile: "testdata/invalid.pem",
					KeyFile:  "testdata/self-signed-client.key",
				},
			},
			err: true,
		},
		{
			desc: "invalid key file",
			cfg: HTTPClientConfig{
				TLSConfig: TLSConfig{
					CAFile:   "testdata/tls-ca-chain.pem",
					CertFile: "testdata/self-signed-client.crt",
					KeyFile:  "testdata/invalid.pem",
				},
			},
			err: true,
		},
		{
			desc: "valid CA, cert and key files",
			cfg: HTTPClientConfig{
				TLSConfig: TLSConfig{
					CAFile:   "testdata/tls-ca-chain.pem",
					CertFile: "testdata/self-signed-client.crt",
					KeyFile:  "testdata/self-signed-client.key",
				},
			},
			err: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := NewHTTPClient(tc.cfg, "")
			if tc.err {
				t.Logf("err: %v", err)
				testutil.NotOk(t, err)
				return
			}

			testutil.Ok(t, err)
		})
	}
}

func generateTestCA(t *testing.T) (certPEM []byte, cert *x509.Certificate, key *rsa.PrivateKey) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	testutil.Ok(t, err)

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	testutil.Ok(t, err)

	cert, err = x509.ParseCertificate(der)
	testutil.Ok(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return certPEM, cert, key
}

func generateTestLeaf(t *testing.T, caCert *x509.Certificate, caKey *rsa.PrivateKey) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	testutil.Ok(t, err)

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, caCert, &key.PublicKey, caKey)
	testutil.Ok(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM
}

// TestNewHTTPClient_CertRotation checks that a client that specifies a cert, key, and CA
// continues working after its cert/key files are updated.
func TestNewHTTPClient_CertRotation(t *testing.T) {
	caCertPEM, caCert, caKey := generateTestCA(t)

	caPool := x509.NewCertPool()
	testutil.Assert(t, caPool.AppendCertsFromPEM(caCertPEM))

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	server.TLS = new(tlsConfigForTest(t, caPool))
	server.StartTLS()
	defer server.Close()

	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	certFile := filepath.Join(dir, "client.crt")
	keyFile := filepath.Join(dir, "client.key")

	testutil.Ok(t, os.WriteFile(caFile, caCertPEM, 0o600))

	certPEMv1, keyPEMv1 := generateTestLeaf(t, caCert, caKey)
	testutil.Ok(t, os.WriteFile(certFile, certPEMv1, 0o600))
	testutil.Ok(t, os.WriteFile(keyFile, keyPEMv1, 0o600))

	client, err := NewHTTPClient(HTTPClientConfig{
		TLSConfig: TLSConfig{
			CAFile:             caFile,
			CertFile:           certFile,
			KeyFile:            keyFile,
			InsecureSkipVerify: true,
		},
	}, "")
	testutil.Ok(t, err)

	resp, err := client.Get(server.URL)
	testutil.Ok(t, err)
	testutil.Equals(t, http.StatusOK, resp.StatusCode)
	testutil.Ok(t, resp.Body.Close())

	certPEMv2, keyPEMv2 := generateTestLeaf(t, caCert, caKey)
	testutil.Ok(t, os.WriteFile(certFile, certPEMv2, 0o600))
	testutil.Ok(t, os.WriteFile(keyFile, keyPEMv2, 0o600))

	resp, err = client.Get(server.URL)
	testutil.Ok(t, err)
	testutil.Equals(t, http.StatusOK, resp.StatusCode)
	testutil.Ok(t, resp.Body.Close())
}

func tlsConfigForTest(t *testing.T, clientCAs *x509.CertPool) tls.Config {
	t.Helper()

	_, serverCACert, serverCAKey := generateTestCA(t)
	serverCertPEM, serverKeyPEM := generateTestLeaf(t, serverCACert, serverCAKey)
	serverCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	testutil.Ok(t, err)

	return tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	}
}
