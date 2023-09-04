// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

/*
This was copied over from https://github.com/grpc-ecosystem/go-grpc-middleware/tree/v2.0.0-rc.3
and modified to support tracing in Thanos till migration to Otel is supported.
*/

package grpctesting

import (
	"context"
	"flag"
	"net"
	"path"
	"runtime"
	"sync"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/thanos-io/thanos/pkg/tracing/tracing_middleware/grpctesting/testpb"
)

var (
	flagTls = flag.Bool("use_tls", true, "whether all gRPC middleware tests should use tls")
)

func getTestingCertsPath() string {
	_, callerPath, _, _ := runtime.Caller(0)
	return path.Join(path.Dir(callerPath), "certs")
}

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

	go func() {
		for {
			var err error
			s.ServerListener, err = net.Listen("tcp", s.serverAddr)
			s.serverAddr = s.ServerListener.Addr().String()
			require.NoError(s.T(), err, "must be able to allocate a port for serverListener")
			if *flagTls {
				creds, err := credentials.NewServerTLSFromFile(
					path.Join(getTestingCertsPath(), "localhost.crt"),
					path.Join(getTestingCertsPath(), "localhost.key"),
				)
				require.NoError(s.T(), err, "failed reading server credentials for localhost.crt")
				s.ServerOpts = append(s.ServerOpts, grpc.Creds(creds))
			}
			// This is the point where we hook up the interceptor
			s.Server = grpc.NewServer(s.ServerOpts...)
			// Crete a service of the instantiator hasn't provided one.
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

	<-s.serverRunning
}

func (s *InterceptorTestSuite) RestartServer(delayedStart time.Duration) <-chan bool {
	s.restartServerWithDelayedStart <- delayedStart
	time.Sleep(10 * time.Millisecond)
	return s.serverRunning
}

func (s *InterceptorTestSuite) NewClient(dialOpts ...grpc.DialOption) testpb.TestServiceClient {
	newDialOpts := append(dialOpts, grpc.WithBlock())
	if *flagTls {
		creds, err := credentials.NewClientTLSFromFile(
			path.Join(getTestingCertsPath(), "localhost.crt"), "localhost")
		require.NoError(s.T(), err, "failed reading client credentials for localhost.crt")
		newDialOpts = append(newDialOpts, grpc.WithTransportCredentials(creds))
	} else {
		newDialOpts = append(newDialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	clientConn, err := grpc.DialContext(ctx, s.ServerAddr(), newDialOpts...)
	require.NoError(s.T(), err, "must not error on client Dial")
	return testpb.NewTestServiceClient(clientConn)
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
