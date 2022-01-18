// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package http

import (
	"net/http"
	"time"
)

type options struct {
	gracePeriod   time.Duration
	listen        string
	tlsConfigPath string
	mux           *http.ServeMux
	enableH2C     bool
}

// Option overrides behavior of Server.
type Option interface {
	apply(*options)
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) {
	f(o)
}

// WithGracePeriod sets shutdown grace period for HTTP server.
// Server waits connections to drain for specified amount of time.
func WithGracePeriod(t time.Duration) Option {
	return optionFunc(func(o *options) {
		o.gracePeriod = t
	})
}

// WithListen sets address to listen for HTTP server.
// Server accepts incoming TCP connections on given address.
func WithListen(s string) Option {
	return optionFunc(func(o *options) {
		o.listen = s
	})
}

func WithTLSConfig(tls string) Option {
	return optionFunc(func(o *options) {
		o.tlsConfigPath = tls
	})
}

func WithEnableH2C(enableH2C bool) Option {
	return optionFunc(func(o *options) {
		o.enableH2C = enableH2C
	})
}

// WithMux overrides the the server's default mux.
func WithMux(mux *http.ServeMux) Option {
	return optionFunc(func(o *options) {
		o.mux = mux
	})
}
