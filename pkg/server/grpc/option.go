// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package grpc

import (
	"crypto/tls"
	"time"

	"google.golang.org/grpc"
)

type options struct {
	registerServerFuncs []registerServerFunc

	gracePeriod time.Duration
	maxConnAge  time.Duration
	listen      string
	network     string

	tlsConfig *tls.Config

	grpcOpts []grpc.ServerOption
}

// Option overrides behavior of Server.
type Option interface {
	apply(*options)
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) {
	f(o)
}

type registerServerFunc func(s *grpc.Server)

// WithServer calls the passed gRPC registration functions on the created
// grpc.Server.
func WithServer(f registerServerFunc) Option {
	return optionFunc(func(o *options) {
		o.registerServerFuncs = append(o.registerServerFuncs, f)
	})
}

// WithGRPCServerOption allows adding raw grpc.ServerOption's to the
// instantiated gRPC server.
func WithGRPCServerOption(opt grpc.ServerOption) Option {
	return optionFunc(func(o *options) {
		o.grpcOpts = append(o.grpcOpts, opt)
	})
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

// WithNetwork sets network to listen for gRPC server e.g tcp, udp or unix.
func WithNetwork(s string) Option {
	return optionFunc(func(o *options) {
		o.network = s
	})
}

// WithTLSConfig sets TLS configuration for gRPC server.
func WithTLSConfig(cfg *tls.Config) Option {
	return optionFunc(func(o *options) {
		o.tlsConfig = cfg
	})
}

// WithMaxConnAge sets the maximum connection age for gRPC server.
func WithMaxConnAge(t time.Duration) Option {
	return optionFunc(func(o *options) {
		o.maxConnAge = t
	})
}
