package grpc

import (
	"crypto/tls"
	"time"
)

type options struct {
	gracePeriod time.Duration
	listen      string

	tlsConfig *tls.Config
}

// Option overrides behavior of Server.
type Option interface {
	apply(*options)
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) {
	f(o)
}

func WithGracePeriod(t time.Duration) Option {
	return optionFunc(func(o *options) {
		o.gracePeriod = t
	})
}

func WithListen(s string) Option {
	return optionFunc(func(o *options) {
		o.listen = s
	})
}

func WithTLSConfig(cfg *tls.Config) Option {
	return optionFunc(func(o *options) {
		o.tlsConfig = cfg
	})
}
