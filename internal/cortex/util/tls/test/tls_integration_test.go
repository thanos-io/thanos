// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package test

import (
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/gogo/status"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/thanos-io/thanos/internal/cortex/integration/ca"
	"github.com/thanos-io/thanos/internal/cortex/util/grpcclient"
	"github.com/thanos-io/thanos/internal/cortex/util/tls"
)

type tcIntegrationClientServer struct {
	name            string
	tlsGrpcEnabled  bool
	tlsConfig       tls.ClientConfig
	httpExpectError func(*testing.T, error)
	grpcExpectError func(*testing.T, error)
}

type grpcHealthCheck struct {
	healthy bool
}

func (h *grpcHealthCheck) Check(_ context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if !h.healthy {
		return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING}, nil
	}

	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func (h *grpcHealthCheck) Watch(_ *grpc_health_v1.HealthCheckRequest, _ grpc_health_v1.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "Watching is not supported")
}

func getLocalHostPort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}

	if err := l.Close(); err != nil {
		return 0, err
	}
	return l.Addr().(*net.TCPAddr).Port, nil
}

func newIntegrationClientServer(
	t *testing.T,
	cfg server.Config,
	tcs []tcIntegrationClientServer,
) {
	// server registers some metrics to default registry
	savedRegistry := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	defer func() {
		prometheus.DefaultRegisterer = savedRegistry
	}()

	grpcPort, err := getLocalHostPort()
	require.NoError(t, err)
	httpPort, err := getLocalHostPort()
	require.NoError(t, err)

	cfg.HTTPListenPort = httpPort
	cfg.GRPCListenPort = grpcPort

	serv, err := server.New(cfg)
	require.NoError(t, err)

	serv.HTTP.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "OK")
	})

	grpc_health_v1.RegisterHealthServer(serv.GRPC, &grpcHealthCheck{true})

	go func() {
		err := serv.Run()
		require.NoError(t, err)
	}()

	httpURL := fmt.Sprintf("https://localhost:%d/hello", httpPort)
	grpcHost := fmt.Sprintf("localhost:%d", grpcPort)

	for _, tc := range tcs {
		tlsClientConfig, err := tc.tlsConfig.GetTLSConfig()
		require.NoError(t, err)

		// HTTP
		t.Run("HTTP/"+tc.name, func(t *testing.T) {
			transport := &http.Transport{TLSClientConfig: tlsClientConfig}
			client := &http.Client{Transport: transport}

			resp, err := client.Get(httpURL)
			if err == nil {
				defer resp.Body.Close()
			}
			if tc.httpExpectError != nil {
				tc.httpExpectError(t, err)
				return
			}
			if err != nil {
				assert.NoError(t, err, tc.name)
				return
			}
			body, err := ioutil.ReadAll(resp.Body)
			assert.NoError(t, err, tc.name)

			assert.Equal(t, []byte("OK"), body, tc.name)

		})

		// GRPC
		t.Run("GRPC/"+tc.name, func(t *testing.T) {
			clientConfig := grpcclient.Config{}
			clientConfig.RegisterFlags(flag.NewFlagSet("fake", flag.ContinueOnError))

			clientConfig.TLSEnabled = tc.tlsGrpcEnabled
			clientConfig.TLS = tc.tlsConfig

			dialOptions, err := clientConfig.DialOption(nil, nil)
			assert.NoError(t, err, tc.name)
			dialOptions = append([]grpc.DialOption{grpc.WithDefaultCallOptions(clientConfig.CallOptions()...)}, dialOptions...)

			conn, err := grpc.Dial(grpcHost, dialOptions...)
			assert.NoError(t, err, tc.name)
			require.NoError(t, err, tc.name)
			require.NoError(t, err, tc.name)

			client := grpc_health_v1.NewHealthClient(conn)

			// TODO: Investigate why the client doesn't really receive the
			// error about the bad certificate from the server side and just
			// see connection closed instead
			resp, err := client.Check(context.TODO(), &grpc_health_v1.HealthCheckRequest{})
			if tc.grpcExpectError != nil {
				tc.grpcExpectError(t, err)
				return
			}
			assert.NoError(t, err)
			if err == nil {
				assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)
			}
		})

	}

	serv.Shutdown()
}

func TestServerWithoutTlsEnabled(t *testing.T) {
	cfg := server.Config{}
	(&cfg).RegisterFlags(flag.NewFlagSet("fake", flag.ContinueOnError))

	newIntegrationClientServer(
		t,
		cfg,
		[]tcIntegrationClientServer{
			{
				name:            "no-config",
				tlsConfig:       tls.ClientConfig{},
				httpExpectError: errorContainsString("http: server gave HTTP response to HTTPS client"),
				grpcExpectError: nil,
			},
			{
				name:            "tls-enable",
				tlsGrpcEnabled:  true,
				tlsConfig:       tls.ClientConfig{},
				httpExpectError: errorContainsString("http: server gave HTTP response to HTTPS client"),
				grpcExpectError: errorContainsString("transport: authentication handshake failed: tls: first record does not look like a TLS handshake"),
			},
		},
	)
}

func TestServerWithLocalhostCertNoClientCertAuth(t *testing.T) {
	certs := setupCertificates(t)

	cfg := server.Config{}
	(&cfg).RegisterFlags(flag.NewFlagSet("fake", flag.ContinueOnError))

	unavailableDescErr := errorContainsString("rpc error: code = Unavailable desc =")

	cfg.HTTPTLSConfig.TLSCertPath = certs.serverCertFile
	cfg.HTTPTLSConfig.TLSKeyPath = certs.serverKeyFile
	cfg.GRPCTLSConfig.TLSCertPath = certs.serverCertFile
	cfg.GRPCTLSConfig.TLSKeyPath = certs.serverKeyFile

	// Test a TLS server with localhost cert without any client certificate enforcement
	newIntegrationClientServer(
		t,
		cfg,
		[]tcIntegrationClientServer{
			{
				name:            "no-config",
				tlsConfig:       tls.ClientConfig{},
				httpExpectError: errorContainsString("x509: certificate signed by unknown authority"),
				// For GRPC we expect this error as we try to connect without TLS to a TLS enabled server
				grpcExpectError: unavailableDescErr,
			},
			{
				name:            "grpc-tls-enabled",
				tlsGrpcEnabled:  true,
				tlsConfig:       tls.ClientConfig{},
				httpExpectError: errorContainsString("x509: certificate signed by unknown authority"),
				grpcExpectError: errorContainsString("x509: certificate signed by unknown authority"),
			},
			{
				name:           "tls-skip-verify",
				tlsGrpcEnabled: true,
				tlsConfig: tls.ClientConfig{
					InsecureSkipVerify: true,
				},
			},
			{
				name:           "tls-skip-verify-no-grpc-tls-enabled",
				tlsGrpcEnabled: false,
				tlsConfig: tls.ClientConfig{
					InsecureSkipVerify: true,
				},
				grpcExpectError: unavailableDescErr,
			},
			{
				name:           "ca-path-set",
				tlsGrpcEnabled: true,
				tlsConfig: tls.ClientConfig{
					CAPath: certs.caCertFile,
				},
			},
			{
				name:           "ca-path-no-grpc-tls-enabled",
				tlsGrpcEnabled: false,
				tlsConfig: tls.ClientConfig{
					CAPath: certs.caCertFile,
				},
				grpcExpectError: unavailableDescErr,
			},
		},
	)
}

func TestServerWithoutLocalhostCertNoClientCertAuth(t *testing.T) {
	certs := setupCertificates(t)

	cfg := server.Config{}
	(&cfg).RegisterFlags(flag.NewFlagSet("fake", flag.ContinueOnError))

	unavailableDescErr := errorContainsString("rpc error: code = Unavailable desc =")

	// Test a TLS server without localhost cert without any client certificate enforcement
	cfg.HTTPTLSConfig.TLSCertPath = certs.serverNoLocalhostCertFile
	cfg.HTTPTLSConfig.TLSKeyPath = certs.serverNoLocalhostKeyFile
	cfg.GRPCTLSConfig.TLSCertPath = certs.serverNoLocalhostCertFile
	cfg.GRPCTLSConfig.TLSKeyPath = certs.serverNoLocalhostKeyFile
	newIntegrationClientServer(
		t,
		cfg,
		[]tcIntegrationClientServer{
			{
				name:            "no-config",
				tlsConfig:       tls.ClientConfig{},
				httpExpectError: errorContainsString("x509: certificate is valid for my-other-name, not localhost"),
				// For GRPC we expect this error as we try to connect without TLS to a TLS enabled server
				grpcExpectError: unavailableDescErr,
			},
			{
				name:            "grpc-tls-enabled",
				tlsGrpcEnabled:  true,
				tlsConfig:       tls.ClientConfig{},
				httpExpectError: errorContainsString("x509: certificate is valid for my-other-name, not localhost"),
				grpcExpectError: errorContainsString("x509: certificate is valid for my-other-name, not localhost"),
			},
			{
				name:           "ca-path",
				tlsGrpcEnabled: true,
				tlsConfig: tls.ClientConfig{
					CAPath: certs.caCertFile,
				},
				httpExpectError: errorContainsString("x509: certificate is valid for my-other-name, not localhost"),
				grpcExpectError: errorContainsString("x509: certificate is valid for my-other-name, not localhost"),
			},
			{
				name:           "server-name",
				tlsGrpcEnabled: true,
				tlsConfig: tls.ClientConfig{
					CAPath:     certs.caCertFile,
					ServerName: "my-other-name",
				},
			},
			{
				name:           "tls-skip-verify",
				tlsGrpcEnabled: true,
				tlsConfig: tls.ClientConfig{
					InsecureSkipVerify: true,
				},
			},
		},
	)
}

func TestTLSServerWithLocalhostCertWithClientCertificateEnforcementUsingClientCA1(t *testing.T) {
	certs := setupCertificates(t)

	cfg := server.Config{}
	(&cfg).RegisterFlags(flag.NewFlagSet("fake", flag.ContinueOnError))

	unavailableDescErr := errorContainsString("rpc error: code = Unavailable desc =")

	// Test a TLS server with localhost cert with client certificate enforcement through client CA 1
	cfg.HTTPTLSConfig.TLSCertPath = certs.serverCertFile
	cfg.HTTPTLSConfig.TLSKeyPath = certs.serverKeyFile
	cfg.HTTPTLSConfig.ClientCAs = certs.clientCA1CertFile
	cfg.HTTPTLSConfig.ClientAuth = "RequireAndVerifyClientCert"
	cfg.GRPCTLSConfig.TLSCertPath = certs.serverCertFile
	cfg.GRPCTLSConfig.TLSKeyPath = certs.serverKeyFile
	cfg.GRPCTLSConfig.ClientCAs = certs.clientCA1CertFile
	cfg.GRPCTLSConfig.ClientAuth = "RequireAndVerifyClientCert"

	// TODO: Investigate why we don't really receive the error about the
	// bad certificate from the server side and just see connection
	// closed/reset instead
	badCertErr := errorContainsString("remote error: tls: bad certificate")
	newIntegrationClientServer(
		t,
		cfg,
		[]tcIntegrationClientServer{
			{
				name:           "tls-skip-verify",
				tlsGrpcEnabled: true,
				tlsConfig: tls.ClientConfig{
					InsecureSkipVerify: true,
				},
				httpExpectError: badCertErr,
				grpcExpectError: unavailableDescErr,
			},
			{
				name:           "ca-path",
				tlsGrpcEnabled: true,
				tlsConfig: tls.ClientConfig{
					CAPath: certs.caCertFile,
				},
				httpExpectError: badCertErr,
				grpcExpectError: unavailableDescErr,
			},
			{
				name:           "ca-path-and-client-cert-ca1",
				tlsGrpcEnabled: true,
				tlsConfig: tls.ClientConfig{
					CAPath:   certs.caCertFile,
					CertPath: certs.client1CertFile,
					KeyPath:  certs.client1KeyFile,
				},
			},
			{
				name:           "tls-skip-verify-and-client-cert-ca1",
				tlsGrpcEnabled: true,
				tlsConfig: tls.ClientConfig{
					InsecureSkipVerify: true,
					CertPath:           certs.client1CertFile,
					KeyPath:            certs.client1KeyFile,
				},
			},
			{
				name:           "ca-cert-and-client-cert-ca2",
				tlsGrpcEnabled: true,
				tlsConfig: tls.ClientConfig{
					CAPath:   certs.caCertFile,
					CertPath: certs.client2CertFile,
					KeyPath:  certs.client2KeyFile,
				},
				httpExpectError: badCertErr,
				grpcExpectError: unavailableDescErr,
			},
		},
	)
}

func TestTLSServerWithLocalhostCertWithClientCertificateEnforcementUsingClientCA2(t *testing.T) {
	certs := setupCertificates(t)

	cfg := server.Config{}
	(&cfg).RegisterFlags(flag.NewFlagSet("fake", flag.ContinueOnError))

	// Test a TLS server with localhost cert with client certificate enforcement through client CA 1
	cfg.HTTPTLSConfig.TLSCertPath = certs.serverCertFile
	cfg.HTTPTLSConfig.TLSKeyPath = certs.serverKeyFile
	cfg.HTTPTLSConfig.ClientCAs = certs.clientCABothCertFile
	cfg.HTTPTLSConfig.ClientAuth = "RequireAndVerifyClientCert"
	cfg.GRPCTLSConfig.TLSCertPath = certs.serverCertFile
	cfg.GRPCTLSConfig.TLSKeyPath = certs.serverKeyFile
	cfg.GRPCTLSConfig.ClientCAs = certs.clientCABothCertFile
	cfg.GRPCTLSConfig.ClientAuth = "RequireAndVerifyClientCert"

	newIntegrationClientServer(
		t,
		cfg,
		[]tcIntegrationClientServer{
			{
				name:           "ca-cert-and-client-cert-ca1",
				tlsGrpcEnabled: true,
				tlsConfig: tls.ClientConfig{
					CAPath:   certs.caCertFile,
					CertPath: certs.client1CertFile,
					KeyPath:  certs.client1KeyFile,
				},
			},
			{
				name:           "ca-cert-and-client-cert-ca2",
				tlsGrpcEnabled: true,
				tlsConfig: tls.ClientConfig{
					CAPath:   certs.caCertFile,
					CertPath: certs.client2CertFile,
					KeyPath:  certs.client2KeyFile,
				},
			},
		},
	)
}

func setupCertificates(t *testing.T) keyMaterial {
	testCADir := t.TempDir()

	// create server side CA

	testCA := ca.New("Cortex Test")
	caCertFile := filepath.Join(testCADir, "ca.crt")
	require.NoError(t, testCA.WriteCACertificate(caCertFile))

	serverCertFile := filepath.Join(testCADir, "server.crt")
	serverKeyFile := filepath.Join(testCADir, "server.key")
	require.NoError(t, testCA.WriteCertificate(
		&x509.Certificate{
			Subject:     pkix.Name{CommonName: "server"},
			DNSNames:    []string{"localhost", "my-other-name"},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		},
		serverCertFile,
		serverKeyFile,
	))

	serverNoLocalhostCertFile := filepath.Join(testCADir, "server-no-localhost.crt")
	serverNoLocalhostKeyFile := filepath.Join(testCADir, "server-no-localhost.key")
	require.NoError(t, testCA.WriteCertificate(
		&x509.Certificate{
			Subject:     pkix.Name{CommonName: "server-no-localhost"},
			DNSNames:    []string{"my-other-name"},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		},
		serverNoLocalhostCertFile,
		serverNoLocalhostKeyFile,
	))

	// create client CAs
	testClientCA1 := ca.New("Cortex Test Client CA 1")
	testClientCA2 := ca.New("Cortex Test Client CA 2")

	clientCA1CertFile := filepath.Join(testCADir, "ca-client-1.crt")
	require.NoError(t, testClientCA1.WriteCACertificate(clientCA1CertFile))
	clientCA2CertFile := filepath.Join(testCADir, "ca-client-2.crt")
	require.NoError(t, testClientCA2.WriteCACertificate(clientCA2CertFile))

	// create a ca file with both certs
	clientCABothCertFile := filepath.Join(testCADir, "ca-client-both.crt")
	func() {
		src1, err := os.Open(clientCA1CertFile)
		require.NoError(t, err)
		defer src1.Close()
		src2, err := os.Open(clientCA2CertFile)
		require.NoError(t, err)
		defer src2.Close()

		dst, err := os.Create(clientCABothCertFile)
		require.NoError(t, err)
		defer dst.Close()

		_, err = io.Copy(dst, src1)
		require.NoError(t, err)
		_, err = io.Copy(dst, src2)
		require.NoError(t, err)

	}()

	client1CertFile := filepath.Join(testCADir, "client-1.crt")
	client1KeyFile := filepath.Join(testCADir, "client-1.key")
	require.NoError(t, testClientCA1.WriteCertificate(
		&x509.Certificate{
			Subject:     pkix.Name{CommonName: "client-1"},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		},
		client1CertFile,
		client1KeyFile,
	))

	client2CertFile := filepath.Join(testCADir, "client-2.crt")
	client2KeyFile := filepath.Join(testCADir, "client-2.key")
	require.NoError(t, testClientCA2.WriteCertificate(
		&x509.Certificate{
			Subject:     pkix.Name{CommonName: "client-2"},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		},
		client2CertFile,
		client2KeyFile,
	))

	return keyMaterial{
		caCertFile:                caCertFile,
		serverCertFile:            serverCertFile,
		serverKeyFile:             serverKeyFile,
		serverNoLocalhostCertFile: serverNoLocalhostCertFile,
		serverNoLocalhostKeyFile:  serverNoLocalhostKeyFile,
		clientCA1CertFile:         clientCA1CertFile,
		clientCABothCertFile:      clientCABothCertFile,
		client1CertFile:           client1CertFile,
		client1KeyFile:            client1KeyFile,
		client2CertFile:           client2CertFile,
		client2KeyFile:            client2KeyFile,
	}
}

type keyMaterial struct {
	caCertFile                string
	serverCertFile            string
	serverKeyFile             string
	serverNoLocalhostCertFile string
	serverNoLocalhostKeyFile  string
	clientCA1CertFile         string
	clientCABothCertFile      string
	client1CertFile           string
	client1KeyFile            string
	client2CertFile           string
	client2KeyFile            string
}

func errorContainsString(str string) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		require.Error(t, err)
		assert.Contains(t, err.Error(), str)
	}
}
