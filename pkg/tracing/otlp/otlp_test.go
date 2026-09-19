// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package otlp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/thanos-io/thanos/pkg/exthttp"
	"github.com/thanos-io/thanos/pkg/tracing"
	"github.com/thanos-io/thanos/pkg/tracing/migration"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gopkg.in/yaml.v2"
)

// mockTraceServiceServer is a minimal OTLP trace gRPC service used for testing.
type mockTraceServiceServer struct {
	coltracepb.UnimplementedTraceServiceServer
}

func (m *mockTraceServiceServer) Export(_ context.Context, _ *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// generateTestCert creates a self-signed ECDSA certificate valid for 127.0.0.1,
// returning the PEM-encoded certificate and private key.
func generateTestCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	testutil.Ok(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	testutil.Ok(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-ca"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	testutil.Ok(t, err)
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	return
}

// startTLSGRPCServer starts a TLS gRPC server with a mock OTLP trace service
// and returns its address. The server is stopped when the test ends.
func startTLSGRPCServer(t *testing.T, certPEM, keyPEM []byte) string {
	t.Helper()

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	testutil.Ok(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	testutil.Ok(t, err)

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{tlsCert},
	})))
	coltracepb.RegisterTraceServiceServer(srv, &mockTraceServiceServer{})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(srv.GracefulStop)

	return lis.Addr().String()
}

// TestNewTracerProvider_GRPCTLSConfigApplied verifies that tls_config.ca_file is
// correctly applied to the gRPC exporter, enabling connections to a server whose
// certificate is signed by a private CA.
func TestNewTracerProvider_GRPCTLSConfigApplied(t *testing.T) {
	certPEM, keyPEM := generateTestCert(t)

	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.crt")
	testutil.Ok(t, os.WriteFile(caFile, certPEM, 0600))

	addr := startTLSGRPCServer(t, certPEM, keyPEM)

	cfg := Config{
		ClientType:  TracingClientGRPC,
		ServiceName: "test",
		Endpoint:    addr,
		Insecure:    false,
		TLSConfig:   exthttp.TLSConfig{CAFile: caFile},
		SamplerType: AlwaysSample,
	}
	confBytes, err := yaml.Marshal(cfg)
	testutil.Ok(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tp, err := NewTracerProvider(ctx, log.NewNopLogger(), confBytes)
	testutil.Ok(t, err)

	// Create and finish a span to trigger the actual gRPC export and TLS handshake.
	_, span := tp.Tracer("test").Start(ctx, "test-span")
	span.End()

	// Shutdown flushes all pending spans; a TLS failure would surface here.
	testutil.Ok(t, tp.Shutdown(ctx))
}

// This test creates an OTLP tracer, starts a span and checks whether it is logged in the exporter.
func TestContextTracing_ClientEnablesTracing(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()

	tracerOtel := newTraceProvider(context.Background(), tracesdk.NewSimpleSpanProcessor(exp), log.NewNopLogger(), "thanos", nil, tracesdk.AlwaysSample())
	tracer, _ := migration.Bridge(tracerOtel, log.NewNopLogger())
	clientRoot, _ := tracing.StartSpan(tracing.ContextWithTracer(context.Background(), tracer), "a")

	testutil.Equals(t, 0, len(exp.GetSpans()))

	clientRoot.Finish()
	testutil.Equals(t, 1, len(exp.GetSpans()))
	testutil.Equals(t, 1, tracing.CountSampledSpans(exp.GetSpans()))
}
