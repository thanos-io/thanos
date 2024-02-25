// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/sony/gobreaker"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/thanos/pkg/gate"
)

var (
	errCircuitBreakerConsecutiveFailuresNotPositive = errors.New("circuit breaker: consecutive failures must be greater than 0")
	errCircuitBreakerFailurePercentInvalid          = errors.New("circuit breaker: failure percent must be in range (0,1]")

	defaultCircuitBreakerConfig = CircuitBreakerConfig{
		Enabled:             false,
		HalfOpenMaxRequests: 10,
		OpenDuration:        5 * time.Second,
		MinRequests:         50,
		ConsecutiveFailures: 5,
		FailurePercent:      0.05,
	}
)

// doWithBatch do func with batch and gate. batchSize==0 means one batch. gate==nil means no gate.
func doWithBatch(ctx context.Context, totalSize int, batchSize int, ga gate.Gate, f func(startIndex, endIndex int) error) error {
	if totalSize == 0 {
		return nil
	}
	if batchSize <= 0 {
		return f(0, totalSize)
	}
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < totalSize; i += batchSize {
		j := i + batchSize
		if j > totalSize {
			j = totalSize
		}
		if ga != nil {
			if err := ga.Start(ctx); err != nil {
				return nil
			}
		}
		startIndex, endIndex := i, j
		g.Go(func() error {
			if ga != nil {
				defer ga.Done()
			}
			return f(startIndex, endIndex)
		})
	}
	return g.Wait()
}

// CircuitBreaker implements the circuit breaker pattern https://en.wikipedia.org/wiki/Circuit_breaker_design_pattern.
type CircuitBreaker interface {
	Execute(func() error) error
}

// CircuitBreakerConfig is the config for the circuite breaker.
type CircuitBreakerConfig struct {
	// Enabled enables circuite breaker.
	Enabled bool `yaml:"enabled"`

	// HalfOpenMaxRequests is the maximum number of requests allowed to pass through
	// when the circuit breaker is half-open.
	// If set to 0, the circuit breaker allows only 1 request.
	HalfOpenMaxRequests uint32 `yaml:"half_open_max_requests"`
	// OpenDuration is the period of the open state after which the state of the circuit breaker becomes half-open.
	// If set to 0, the circuit breaker utilizes the default value of 60 seconds.
	OpenDuration time.Duration `yaml:"open_duration"`
	// MinRequests is minimal requests to trigger the circuit breaker.
	MinRequests uint32 `yaml:"min_requests"`
	// ConsecutiveFailures represents consecutive failures based on CircuitBreakerMinRequests to determine if the circuit breaker should open.
	ConsecutiveFailures uint32 `yaml:"consecutive_failures"`
	// FailurePercent represents the failure percentage, which is based on CircuitBreakerMinRequests, to determine if the circuit breaker should open.
	FailurePercent float64 `yaml:"failure_percent"`
}

func (c CircuitBreakerConfig) validate() error {
	if !c.Enabled {
		return nil
	}
	if c.ConsecutiveFailures == 0 {
		return errCircuitBreakerConsecutiveFailuresNotPositive
	}
	if c.FailurePercent <= 0 || c.FailurePercent > 1 {
		return errCircuitBreakerFailurePercentInvalid
	}
	return nil
}

type noopCircuitBreaker struct{}

func (noopCircuitBreaker) Execute(f func() error) error { return f() }

type gobreakerCircuitBreaker struct {
	*gobreaker.CircuitBreaker
}

func (cb gobreakerCircuitBreaker) Execute(f func() error) error {
	_, err := cb.CircuitBreaker.Execute(func() (any, error) {
		return nil, f()
	})
	return err
}

func newCircuitBreaker(name string, config CircuitBreakerConfig) CircuitBreaker {
	if !config.Enabled {
		return noopCircuitBreaker{}
	}
	return gobreakerCircuitBreaker{gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        name,
		MaxRequests: config.HalfOpenMaxRequests,
		Interval:    10 * time.Second,
		Timeout:     config.OpenDuration,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.Requests >= config.MinRequests &&
				(counts.ConsecutiveFailures >= uint32(config.ConsecutiveFailures) ||
					float64(counts.TotalFailures)/float64(counts.Requests) >= config.FailurePercent)
		},
	})}
}
