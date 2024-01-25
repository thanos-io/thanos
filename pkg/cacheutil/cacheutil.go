// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/sony/gobreaker"

	"github.com/thanos-io/thanos/pkg/gate"
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
