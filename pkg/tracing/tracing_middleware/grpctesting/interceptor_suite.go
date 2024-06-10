// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

/*
This was copied over from https://github.com/grpc-ecosystem/go-grpc-middleware/tree/v2.0.0-rc.3
and modified to support tracing in Thanos till migration to Otel is supported.
*/

package grpctesting

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"math/big"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc/credentials/insecure"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/thanos-io/thanos/pkg/tracing/tracing_middleware/grpctesting/testpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	flagTls = flag.Bool("use_tls", true, "whether all gRPC middleware tests should use tls")
	certPEM []byte
	keyPEM  []byte
)

// InterceptorTestSuite is a testify/Suite that starts a gRPC PingService server and a client.
type InterceptorTestSuite struct {
	suite.Suite

	TestService testpb.TestServiceServer
	ServerOpts  []grpc.ServerOption
	ClientOpts  []grpc.DialOption

	serverAddr     string
	ServerListener net.Listener
	Server         *grpc.Server
	clientConn     *grpc.ClientConn
	Client         testpb.TestServiceClient

	restartServerWithDelayedStart chan time.Duration
	serverRunning                 chan bool

	cancels []context.CancelFunc
}

func (s *InterceptorTestSuite) SetupSuite() {
	s.restartServerWithDelayedStart = make(chan time.Duration)
	s.serverRunning = make(chan bool)

	s.serverAddr = "127.0.0.1:0"
	var err error
	certPEM, keyPEM, err = generateCertAndKey([]string{"localhost", "example.com"})
	require.NoError(s.T(), err, "unable to generate test certificate/key")

	go func() {
		for {
			var err error
			s.ServerListener, err = net.Listen("tcp", s.serverAddr)
			s.serverAddr = s.ServerListener.Addr().String()
			require.NoError(s.T(), err, "must be able to allocate a port for serverListener")
			if *flagTls {
				cert, err := tls.X509KeyPair(certPEM, keyPEM)
				require.NoError(s.T(), err, "unable to load test TLS certificate")
				creds := credentials.NewServerTLSFromCert(&cert)
				s.ServerOpts = append(s.ServerOpts, grpc.Creds(creds))
			}
			// This is the point where we hook up the interceptor.
			s.Server = grpc.NewServer(s.ServerOpts...)
			// Create a service if the instantiator hasn't provided one.
			if s.TestService == nil {
				s.TestService = &TestPingService{T: s.T()}
			}
			testpb.RegisterTestServiceServer(s.Server, s.TestService)

			w := sync.WaitGroup{}
			w.Add(1)
			go func() {
				_ = s.Server.Serve(s.ServerListener)
				w.Done()
			}()
			if s.Client == nil {
				s.Client = s.NewClient(s.ClientOpts...)
			}

			s.serverRunning <- true

			d := <-s.restartServerWithDelayedStart
			s.Server.Stop()
			time.Sleep(d)
			w.Wait()
		}
	}()

	select {
	case <-s.serverRunning:
	case <-time.After(2 * time.Second):
		s.T().Fatal("server failed to start before deadline")
	}
}

func (s *InterceptorTestSuite) RestartServer(delayedStart time.Duration) <-chan bool {
	s.restartServerWithDelayedStart <- delayedStart
	time.Sleep(10 * time.Millisecond)
	return s.serverRunning
}

func (s *InterceptorTestSuite) NewClient(dialOpts ...grpc.DialOption) testpb.TestServiceClient {
	var err error
	if *flagTls {
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM(certPEM) {
			s.T().Fatal("failed to append certificate")
		}
		creds := credentials.NewTLS(&tls.Config{ServerName: "localhost", RootCAs: cp})
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	s.clientConn, err = grpc.NewClient(s.ServerAddr(), dialOpts...)
	require.NoError(s.T(), err, "must not error on client Dial")
	return testpb.NewTestServiceClient(s.clientConn)
}

func (s *InterceptorTestSuite) ServerAddr() string {
	return s.serverAddr
}

func (s *InterceptorTestSuite) SimpleCtx() context.Context {
	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
	s.cancels = append(s.cancels, cancel)
	return ctx
}

func (s *InterceptorTestSuite) DeadlineCtx(deadline time.Time) context.Context {
	ctx, cancel := context.WithDeadline(context.TODO(), deadline)
	s.cancels = append(s.cancels, cancel)
	return ctx
}

func (s *InterceptorTestSuite) TearDownSuite() {
	time.Sleep(10 * time.Millisecond)
	if s.ServerListener != nil {
		s.Server.GracefulStop()
		s.T().Logf("stopped grpc.Server at: %v", s.ServerAddr())
		_ = s.ServerListener.Close()
	}
	if s.clientConn != nil {
		_ = s.clientConn.Close()
	}
	for _, c := range s.cancels {
		c()
	}
}

// generateCertAndKey copied from https://github.com/johanbrandhorst/certify/blob/master/issuers/vault/vault_suite_test.go#L255
// with minor modifications.
func generateCertAndKey(san []string) ([]byte, []byte, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	notBefore := time.Now()
	notAfter := notBefore.Add(time.Hour)
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, err
	}
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: "example.com",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              san,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, priv.Public(), priv)
	if err != nil {
		return nil, nil, err
	}
	certOut := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: derBytes,
	})
	keyOut := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(priv),
	})

	return certOut, keyOut, nil
}
