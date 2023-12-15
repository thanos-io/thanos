// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

type StoreOption[T BucketStore | PrometheusStore | TSDBStore | ProxyStore] func(*T)

// WithLogger sets the logger to the one you pass.
func WithLogger[T BucketStore | PrometheusStore | TSDBStore | ProxyStore](logger log.Logger) StoreOption[T] {
	return func(s *T) {
		switch s := any(s).(type) {
		case *BucketStore:
			s.logger = logger
		case *PrometheusStore:
			s.logger = logger
		case *TSDBStore:
			s.logger = logger
		case *ProxyStore:
			s.logger = logger
		}
	}
}

// WithDebugLogging enables debug logging.
func WithDebugLogging[T BucketStore | ProxyStore]() StoreOption[T] {
	return func(s *T) {
		switch s := any(s).(type) {
		case *BucketStore:
			s.debugLogging = true
		case *ProxyStore:
			s.debugLogging = true
		}
	}
}

// WithRegistry sets a registry that the store uses to register metrics with.
func WithRegistry[T BucketStore | PrometheusStore | ProxyStore](reg prometheus.Registerer) StoreOption[T] {
	return func(s *T) {
		switch s := any(s).(type) {
		case *BucketStore:
			s.reg = reg
		case *PrometheusStore:
			s.reg = reg
		case *ProxyStore:
			s.reg = reg
		}
	}
}
