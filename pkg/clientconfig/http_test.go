// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package clientconfig

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	config_util "github.com/prometheus/common/config"
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
			desc: "reserved HTTP header",
			cfg: HTTPClientConfig{
				HTTPHeaders: config_util.Headers{Headers: map[string]config_util.Header{
					"Authorization": {Values: []string{"secret"}},
				}},
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
		{
			desc: "google_iam combined with basic_auth",
			cfg: HTTPClientConfig{
				BasicAuth: BasicAuth{Username: "u", Password: "p"},
				GoogleIAM: &GoogleIAMConfig{},
			},
			err: true,
		},
		{
			desc: "google_iam combined with bearer_token",
			cfg: HTTPClientConfig{
				BearerToken: "tok",
				GoogleIAM:   &GoogleIAMConfig{},
			},
			err: true,
		},
		{
			desc: "google_iam with missing credentials_file",
			cfg: HTTPClientConfig{
				GoogleIAM: &GoogleIAMConfig{
					CredentialsFile: "testdata/does-not-exist.json",
				},
			},
			err: true,
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

func TestNewHTTPClient_HTTPHeaders(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
	}))
	defer srv.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	testutil.Ok(t, os.WriteFile(tokenFile, []byte("from-file"), 0o600))

	cfgs, err := LoadConfigs([]byte(`
- http_config:
    http_headers:
      X-Scope-OrgID:
        values: [tenant-a]
      X-Custom:
        values: [a, b]
      X-Token:
        files: [` + tokenFile + `]
  static_configs: ["localhost:9090"]
`))
	testutil.Ok(t, err)
	client, err := NewHTTPClient(cfgs[0].HTTPConfig.HTTPClientConfig, "")
	testutil.Ok(t, err)

	resp, err := client.Get(srv.URL)
	testutil.Ok(t, err)
	testutil.Ok(t, resp.Body.Close())

	testutil.Equals(t, []string{"tenant-a"}, got.Values("X-Scope-OrgID"))
	testutil.Equals(t, []string{"a", "b"}, got.Values("X-Custom"))
	testutil.Equals(t, []string{"from-file"}, got.Values("X-Token"))
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

func TestNewHTTPClient_GoogleIAM(t *testing.T) {
	var tokenRequests int
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenRequests++
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"test-google-iam-token","token_type":"Bearer","expires_in":3600}`))
	}))
	t.Cleanup(tokenSrv.Close)

	var gotHeader http.Header
	targetSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(targetSrv.Close)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	testutil.Ok(t, err)
	pkcs8Key, err := x509.MarshalPKCS8PrivateKey(key)
	testutil.Ok(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8Key})

	credJSON, err := json.Marshal(map[string]string{
		"type":             "service_account",
		"project_id":       "test-project",
		"quota_project_id": "test-quota-project",
		"private_key_id":   "key-1",
		"private_key":      string(keyPEM),
		"client_email":     "test@test-project.iam.gserviceaccount.com",
		"client_id":        "123456",
		"token_uri":        tokenSrv.URL + "/token",
	})
	testutil.Ok(t, err)

	credFile := filepath.Join(t.TempDir(), "sa.json")
	testutil.Ok(t, os.WriteFile(credFile, credJSON, 0o600))

	t.Run("credentials_file with custom header", func(t *testing.T) {
		tokenRequests = 0
		cfgs, err := LoadConfigs([]byte(fmt.Sprintf(`
- http_config:
    google_iam:
      credentials_file: %s
    http_headers:
      X-Custom:
        values: [val]
  static_configs: ["localhost:9090"]
`, credFile)))
		testutil.Ok(t, err)

		client, err := NewHTTPClient(cfgs[0].HTTPConfig.HTTPClientConfig, "query")
		testutil.Ok(t, err)

		resp, err := client.Get(targetSrv.URL)
		testutil.Ok(t, err)
		testutil.Ok(t, resp.Body.Close())

		testutil.Equals(t, 1, tokenRequests)
		testutil.Equals(t, "Bearer test-google-iam-token", gotHeader.Get("Authorization"))
		testutil.Equals(t, "test-quota-project", gotHeader.Get("X-Goog-User-Project"))
		testutil.Equals(t, "val", gotHeader.Get("X-Custom"))
	})

	t.Run("application default credentials with custom scope", func(t *testing.T) {
		tokenRequests = 0
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)

		cfgs, err := LoadConfigs([]byte(`
- http_config:
    google_iam:
      scopes:
        - https://www.googleapis.com/auth/cloud-platform
  static_configs: ["localhost:9090"]
`))
		testutil.Ok(t, err)

		client, err := NewHTTPClient(cfgs[0].HTTPConfig.HTTPClientConfig, "query")
		testutil.Ok(t, err)

		resp, err := client.Get(targetSrv.URL)
		testutil.Ok(t, err)
		testutil.Ok(t, resp.Body.Close())

		testutil.Equals(t, 1, tokenRequests)
		testutil.Equals(t, "Bearer test-google-iam-token", gotHeader.Get("Authorization"))
	})
}
