package grpc

import (
	"crypto/tls"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"time"
)

type options struct {
	gracePeriod time.Duration
	listen      string

	tlsConfig *tls.Config

	tracingMethodFilter grpc_opentracing.FilterFunc
}

// Option overrides behavior of Server.
type Option interface {
	apply(*options)
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) {
	f(o)
}

// WithGracePeriod sets shutdown grace period for gRPC server.
// Server waits connections to drain for specified amount of time.
func WithGracePeriod(t time.Duration) Option {
	return optionFunc(func(o *options) {
		o.gracePeriod = t
	})
}

// WithListen sets address to listen for gRPC server.
// Server accepts incoming connections on given address.
func WithListen(s string) Option {
	return optionFunc(func(o *options) {
		o.listen = s
	})
}

// WithTLSConfig sets TLS configuration for gRPC server.
func WithTLSConfig(cfg *tls.Config) Option {
	return optionFunc(func(o *options) {
		o.tlsConfig = cfg
	})
}

// WithTracingMethodFilter sets method filter to use for tracing interceptors.
func WithTracingMethodFilter(f grpc_opentracing.FilterFunc) Option {
	return optionFunc(func(o *options) {
		o.tracingMethodFilter = f
	})
}

