// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package http

import (
	"context"
	"net/http"
	"net/http/pprof"

	"github.com/felixge/fgprof"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/prober"
)

// A Server defines parameters for serve HTTP requests, a wrapper around http.Server.
type Server struct {
	logger log.Logger
	comp   component.Component
	prober *prober.HTTPProbe

	mux *http.ServeMux
	srv *http.Server

	opts options
}

// New creates a new Server.
func New(logger log.Logger, reg *prometheus.Registry, comp component.Component, prober *prober.HTTPProbe, opts ...Option) *Server {
	options := options{}
	for _, o := range opts {
		o.apply(&options)
	}

	mux := http.NewServeMux()
	registerMetrics(mux, reg)
	registerProbes(mux, prober, logger)
	registerProfiler(mux)

	return &Server{
		logger: log.With(logger, "service", "http/server", "component", comp.String()),
		comp:   comp,
		prober: prober,
		mux:    mux,
		srv:    &http.Server{Addr: options.listen, Handler: mux},
		opts:   options,
	}
}

// ListenAndServe listens on the TCP network address and handles requests on incoming connections.
func (s *Server) ListenAndServe() error {
	level.Info(s.logger).Log("msg", "listening for requests and metrics", "address", s.opts.listen)
	return errors.Wrap(s.srv.ListenAndServe(), "serve HTTP and metrics")
}

// Shutdown gracefully shuts down the server by waiting,
// for specified amount of time (by gracePeriod) for connections to return to idle and then shut down.
func (s *Server) Shutdown(err error) {
	level.Info(s.logger).Log("msg", "internal server is shutting down", "err", err)
	if err == http.ErrServerClosed {
		level.Warn(s.logger).Log("msg", "internal server closed unexpectedly")
		return
	}

	if s.opts.gracePeriod == 0 {
		s.srv.Close()
		level.Info(s.logger).Log("msg", "internal server is shutdown", "err", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.opts.gracePeriod)
	defer cancel()

	if err := s.srv.Shutdown(ctx); err != nil {
		level.Error(s.logger).Log("msg", "internal server shut down failed", "err", err)
		return
	}
	level.Info(s.logger).Log("msg", "internal server is shutdown gracefully", "err", err)
}

// Handle registers the handler for the given pattern.
func (s *Server) Handle(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, handler)
}

func registerProfiler(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/fgprof", fgprof.Handler())
}

func registerMetrics(mux *http.ServeMux, g prometheus.Gatherer) {
	if g != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{}))
	}
}

func registerProbes(mux *http.ServeMux, p *prober.HTTPProbe, logger log.Logger) {
	if p != nil {
		mux.Handle("/-/healthy", p.HealthyHandler(logger))
		mux.Handle("/-/ready", p.ReadyHandler(logger))
	}
}
