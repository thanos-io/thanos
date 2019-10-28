package grpc

import (
	"context"
	"math"
	"net"
	"runtime/debug"
	"time"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type Server struct {
	logger log.Logger
	comp   component.Component

	srv      *grpc.Server
	listener net.Listener

	opts options
}

func New(logger log.Logger, reg prometheus.Registerer, tracer opentracing.Tracer, comp component.Component, storeSrv storepb.StoreServer, opts ...Option) *Server {
	options := options{
		gracePeriod: 5 * time.Second,
	}

	for _, o := range opts {
		o.apply(&options)
	}

	met := grpc_prometheus.NewServerMetrics()
	met.EnableHandlingTimeHistogram(
		grpc_prometheus.WithHistogramBuckets([]float64{
			0.001, 0.01, 0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4,
		}),
	)
	panicsTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_grpc_req_panics_recovered_total",
		Help: "Total number of gRPC requests recovered from internal panic.",
	})
	reg.MustRegister(met, panicsTotal)

	grpcPanicRecoveryHandler := func(p interface{}) (err error) {
		panicsTotal.Inc()
		level.Error(logger).Log("msg", "recovered from panic", "panic", p, "stack", debug.Stack())
		return status.Errorf(codes.Internal, "%s", p)
	}

	grpcOpts := []grpc.ServerOption{}
	grpcOpts = append(grpcOpts,
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc_middleware.WithUnaryServerChain(
			met.UnaryServerInterceptor(),
			tracing.UnaryServerInterceptor(tracer),
			grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
		),
		grpc_middleware.WithStreamServerChain(
			met.StreamServerInterceptor(),
			tracing.StreamServerInterceptor(tracer),
			grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
		),
	)

	if options.tlsConfig != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(options.tlsConfig)))
	}
	s := grpc.NewServer(grpcOpts...)

	storepb.RegisterStoreServer(s, storeSrv)
	met.InitializeMetrics(s)

	return &Server{
		logger: log.With(logger, "service", "gRPC/server", "component", comp.String()),
		comp:   comp,
		srv:    s,
		opts:   options,
	}
}

func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", s.opts.listen)
	if err != nil {
		return errors.Wrap(err, "listen gRPC on address")
	}
	s.listener = l

	level.Info(s.logger).Log("msg", "Listening for StoreAPI gRPC", "address", s.opts.listen)
	return errors.Wrap(s.srv.Serve(s.listener), "serve gRPC")
}

func (s *Server) Shutdown(err error) {
	defer level.Info(s.logger).Log("msg", "server shut down internal server", "err", err)

	if s.opts.gracePeriod == 0 {
		s.srv.Stop()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.opts.gracePeriod)
	defer cancel()

	stopped := make(chan struct{})
	go func() {
		level.Info(s.logger).Log("msg", "gracefully stoping server")
		s.srv.GracefulStop() // Also closes s.listener.
		close(stopped)
	}()

	select {
	case <-ctx.Done():
		level.Info(s.logger).Log("msg", "timeout, grace period exceeded")
		s.srv.Stop()
	case <-stopped:
		cancel()
	}
}
