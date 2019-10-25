package http

import (
	"context"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/prober"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type HTTPServer struct {
	logger log.Logger
	comp   component.Component
	prober *prober.Prober

	mux *http.ServeMux
	srv *http.Server

	opts options
}

func New(logger log.Logger, reg *prometheus.Registry, comp component.Component, prober *prober.Prober, opts ...Option) *HTTPServer {
	options := options{
		gracePeriod: 5 * time.Second,
		listen:      "0.0.0.0:10902",
	}

	for _, o := range opts {
		o.apply(&options)
	}

	mux := http.NewServeMux()
	registerMetrics(mux, reg)
	registerProfiler(mux)
	prober.RegisterInMux(mux)

	return &HTTPServer{
		logger: log.With(logger, "service", "http/server", "component", comp.String()),
		comp:   comp,
		prober: prober,
		mux:    mux,
		srv:    &http.Server{Addr: options.listen, Handler: mux},
		opts:   options,
	}
}

func (s *HTTPServer) ListenAndServe() error {
	s.prober.SetHealthy()
	level.Info(s.logger).Log("msg", "listening for requests and metrics", "address", s.opts.listen)
	return errors.Wrap(s.srv.ListenAndServe(), "serve HTTP and metrics")
}

func (s *HTTPServer) Shutdown(err error) {
	s.prober.SetNotReady(err)
	defer s.prober.SetNotHealthy(err)

	if err == http.ErrServerClosed {
		level.Warn(s.logger).Log("msg", "internal server closed unexpectedly")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.opts.gracePeriod)
	defer cancel()

	level.Info(s.logger).Log("msg", "server shut down internal server")

	if err := s.srv.Shutdown(ctx); err != nil {
		level.Error(s.logger).Log("msg", "server shut down failed", "err", err)
	}
}

func (s *HTTPServer) Handle(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, handler)
}

func registerProfiler(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

func registerMetrics(mux *http.ServeMux, g prometheus.Gatherer) {
	mux.Handle("/metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{}))
}
