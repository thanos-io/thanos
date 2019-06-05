package register

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime/debug"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type SetupFunc func(*run.Group, log.Logger, *prometheus.Registry, opentracing.Tracer, bool) error

// metricHTTPListenGroup is a run.Group that servers HTTP endpoint with only Prometheus metrics.
func metricHTTPListenGroup(g *run.Group, logger log.Logger, reg *prometheus.Registry, httpBindAddr string) error {
	mux := http.NewServeMux()
	registerMetrics(mux, reg)
	registerProfile(mux)

	l, err := net.Listen("tcp", httpBindAddr)
	if err != nil {
		return errors.Wrap(err, "listen metrics address")
	}

	g.Add(func() error {
		level.Info(logger).Log("msg", "Listening for metrics", "address", httpBindAddr)
		return errors.Wrap(http.Serve(l, mux), "serve metrics")
	}, func(error) {
		runutil.CloseWithLogOnErr(logger, l, "metric listener")
	})
	return nil
}


func registerMetrics(mux *http.ServeMux, g prometheus.Gatherer) {
	mux.Handle("/metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{}))
}

func registerProfile(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
}

// defaultGRPCServerOpts returns default gRPC server opts that includes:
// - request histogram
// - tracing
// - panic recovery with panic counter
func defaultGRPCServerOpts(logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, cert, key, clientCA string) ([]grpc.ServerOption, error) {
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
	grpcPanicRecoveryHandler := func(p interface{}) (err error) {
		panicsTotal.Inc()
		level.Error(logger).Log("msg", "recovered from panic", "panic", p, "stack", debug.Stack())
		return status.Errorf(codes.Internal, "%s", p)
	}
	reg.MustRegister(met, panicsTotal)
	opts := []grpc.ServerOption{
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
	}

	if key == "" && cert == "" {
		if clientCA != "" {
			return nil, errors.New("when a client CA is used a server key and certificate must also be provided")
		}

		level.Info(logger).Log("msg", "disabled TLS, key and cert must be set to enable")
		return opts, nil
	}

	if key == "" || cert == "" {
		return nil, errors.New("both server key and certificate must be provided")
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	tlsCert, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, errors.Wrap(err, "server credentials")
	}

	level.Info(logger).Log("msg", "enabled gRPC server side TLS")

	tlsCfg.Certificates = []tls.Certificate{tlsCert}

	if clientCA != "" {
		caPEM, err := ioutil.ReadFile(clientCA)
		if err != nil {
			return nil, errors.Wrap(err, "reading client CA")
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caPEM) {
			return nil, errors.Wrap(err, "building client CA")
		}
		tlsCfg.ClientCAs = certPool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert

		level.Info(logger).Log("msg", "gRPC server TLS client verification enabled")
	}

	return append(opts, grpc.Creds(credentials.NewTLS(tlsCfg))), nil
}

