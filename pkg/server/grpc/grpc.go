// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package grpc

import (
	"context"
	"math"
	"net"
	"runtime/debug"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grpc-ecosystem/go-grpc-middleware/providers/kit/v2"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	grpc_health "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// A Server defines parameters to serve RPC requests, a wrapper around grpc.Server.
type Server struct {
	logger log.Logger
	comp   component.Component

	srv      *grpc.Server
	listener net.Listener

	opts options
}

// New creates a new gRPC Store API.
// If rulesSrv is not nil, it also registers Rules API to the returned server.
func New(logger log.Logger, reg prometheus.Registerer, tracer opentracing.Tracer, logOpts []grpc_logging.Option, comp component.Component, probe *prober.GRPCProbe, opts ...Option) *Server {
	logger = log.With(logger, "service", "gRPC/server", "component", comp.String())
	options := options{
		network: "tcp",
	}
	for _, o := range opts {
		o.apply(&options)
	}

	met := grpc_prometheus.NewServerMetrics()
	met.EnableHandlingTimeHistogram(
		grpc_prometheus.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}),
	)
	panicsTotal := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "grpc_req_panics_recovered_total",
		Help: "Total number of gRPC requests recovered from internal panic.",
	})

	grpcPanicRecoveryHandler := func(p interface{}) (err error) {
		panicsTotal.Inc()
		level.Error(logger).Log("msg", "recovered from panic", "panic", p, "stack", debug.Stack())
		return status.Errorf(codes.Internal, "%s", p)
	}

	options.grpcOpts = append(options.grpcOpts, []grpc.ServerOption{
		// NOTE: It is recommended for gRPC messages to not go over 1MB, yet it is typical for remote write requests and store API responses to go over 4MB.
		// Remove limits and allow users to use histogram message sizes to detect those situations.
		// TODO(bwplotka): https://github.com/grpc-ecosystem/go-grpc-middleware/issues/462
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc_middleware.WithUnaryServerChain(
			grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
			met.UnaryServerInterceptor(),
			tracing.UnaryServerInterceptor(tracer),
			grpc_logging.UnaryServerInterceptor(kit.InterceptorLogger(logger), logOpts...),
		),
		grpc_middleware.WithStreamServerChain(
			grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
			met.StreamServerInterceptor(),
			tracing.StreamServerInterceptor(tracer),
			grpc_logging.StreamServerInterceptor(kit.InterceptorLogger(logger), logOpts...),
		),
	}...)

	if options.tlsConfig != nil {
		options.grpcOpts = append(options.grpcOpts, grpc.Creds(credentials.NewTLS(options.tlsConfig)))
	}
	if options.maxConnAge > 0 {
		options.grpcOpts = append(options.grpcOpts, grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: options.maxConnAge}))
	}
	s := grpc.NewServer(options.grpcOpts...)

	// Register all configured servers.
	for _, f := range options.registerServerFuncs {
		f(s)
	}

	met.InitializeMetrics(s)
	reg.MustRegister(met)

	grpc_health.RegisterHealthServer(s, probe.HealthServer())
	reflection.Register(s)

	return &Server{
		logger: logger,
		comp:   comp,
		srv:    s,
		opts:   options,
	}
}

// ListenAndServe listens on the TCP network address and handles requests on incoming connections.
func (s *Server) ListenAndServe() error {
	l, err := net.Listen(s.opts.network, s.opts.listen)
	if err != nil {
		return errors.Wrapf(err, "listen gRPC on address %s", s.opts.listen)
	}
	s.listener = l

	level.Info(s.logger).Log("msg", "listening for serving gRPC", "address", s.opts.listen)
	return errors.Wrap(s.srv.Serve(s.listener), "serve gRPC")
}

// Shutdown gracefully shuts down the server by waiting,
// for specified amount of time (by gracePeriod) for connections to return to idle and then shut down.
func (s *Server) Shutdown(err error) {
	level.Info(s.logger).Log("msg", "internal server is shutting down", "err", err)

	if s.opts.gracePeriod == 0 {
		s.srv.Stop()
		level.Info(s.logger).Log("msg", "internal server is shutdown", "err", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.opts.gracePeriod)
	defer cancel()

	stopped := make(chan struct{})
	go func() {
		level.Info(s.logger).Log("msg", "gracefully stopping internal server")
		s.srv.GracefulStop() // Also closes s.listener.
		close(stopped)
	}()

	select {
	case <-ctx.Done():
		level.Info(s.logger).Log("msg", "grace period exceeded enforcing shutdown")
		s.srv.Stop()
		return
	case <-stopped:
		cancel()
	}
	level.Info(s.logger).Log("msg", "internal server is shutdown gracefully", "err", err)
}
