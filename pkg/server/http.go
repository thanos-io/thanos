package server

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

type Server struct {
	logger log.Logger
	comp   component.Component
	prober *prober.Prober

	mux *http.ServeMux
	srv *http.Server

	opts options
}

func NewHTTP(logger log.Logger, reg *prometheus.Registry, comp component.Component, prober *prober.Prober, opts ...Option) Server {
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

	return Server{
		logger: log.With(logger, "service", "http/server"),
		comp:   comp,
		prober: prober,
		mux:    mux,
		srv:    &http.Server{Addr: options.listen, Handler: mux},
		opts:   options,
	}
}

func (s *Server) ListenAndServe() error {
	s.prober.SetHealthy()
	level.Info(s.logger).Log("msg", "listening for requests and metrics", "component", s.comp.String(), "address", s.opts.listen)
	return errors.Wrapf(s.srv.ListenAndServe(), "serve %s and metrics", s.comp.String())
}

func (s *Server) Shutdown(err error) {
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
		level.Error(s.logger).Log("msg", "server shut down failed", "err", err, "component", s.comp.String())
	}
}

func (s *Server) Handle(pattern string, handler http.Handler) {
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
