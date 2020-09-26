// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

type ChunksLimiter interface {
	// Reserve num chunks or bytes out of the total number of chunks or bytes enforced by the limiter.
	// Returns an error if the limit has been exceeded. This function must be goroutine safe.
	Reserve(num uint64) error

	// NewWithFailedCounterFrom creates a new chunks limiter from existing with failed counter from the argument.
	NewWithFailedCounterFrom(l ChunksLimiter) (ChunksLimiter, error)
}

// ChunksLimiterFactory is used to create a new ChunksLimiter. The factory is useful for
// projects depending on Thanos (eg. Cortex) which have dynamic limits.
type ChunksLimiterFactory func(failedCounter prometheus.Counter) ChunksLimiter

// Limiter is a simple mechanism for checking if something has passed a certain threshold.
type Limiter struct {
	limit    uint64
	reserved atomic.Uint64

	// Counter metric which we will increase if limit is exceeded.
	failedCounter prometheus.Counter
	failedOnce    *sync.Once
}

// NewLimiter returns a new limiter with a specified limit. 0 disables the limit.
func NewLimiter(limit uint64, ctr prometheus.Counter) *Limiter {
	return &Limiter{limit: limit, failedCounter: ctr, failedOnce: &sync.Once{}}
}

// Reserve implements ChunksLimiter.
func (l *Limiter) Reserve(num uint64) error {
	if l.limit == 0 {
		return nil
	}
	if reserved := l.reserved.Add(num); reserved > l.limit {
		// We need to protect from the counter being incremented twice due to concurrency
		// while calling Reserve().
		l.failedOnce.Do(l.failedCounter.Inc)
		return errors.Errorf("limit %v violated (got %v)", l.limit, reserved)
	}
	return nil
}

// NewWithFailedCounterFrom creates a new limiter with failed counter from the argument.
func (l *Limiter) NewWithFailedCounterFrom(l2 ChunksLimiter) (ChunksLimiter, error) {
	from, ok := l2.(*Limiter)
	if !ok || from == nil {
		return &Limiter{}, errors.Errorf("failed to share counter from %#v", l2)
	}

	return &Limiter{
		limit:         l.limit,
		reserved:      l.reserved,
		failedCounter: from.failedCounter,
		failedOnce:    from.failedOnce,
	}, nil
}

// NewChunksLimiterFactory makes a new ChunksLimiterFactory with a static limit.
func NewChunksLimiterFactory(limit uint64) ChunksLimiterFactory {
	return func(failedCounter prometheus.Counter) ChunksLimiter {
		return NewLimiter(limit, failedCounter)
	}
}
