package server

import (
	"time"
)

type options struct {
	gracePeriod time.Duration
	listen      string
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
