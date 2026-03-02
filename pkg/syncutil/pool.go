// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package syncutil

import "sync"

// Pool is a generic wrapper around sync.Pool that provides type safety,
// an optional reset function, and a defer-friendly Return mechanism.
// Usage:
//
//	p := NewPool(func() *bytes.Buffer {
//		return new(bytes.Buffer)
//	}).WithReset(func(b *bytes.Buffer) bool {
//		if b.Cap() <= maxPooledCompressedCap {
//			b.Reset()
//			return true // return buffer to the pool.
//		}
//		return false // discard the buffer that is too large.
//	}).Build()
//
//	buf, ret := p.Get()
//	defer ret(buf)
//	buf.WriteString("hello")
type Pool[T any] struct {
	disabled bool

	inner sync.Pool
	reset func(T) bool
}

// PoolBuilder accumulates configuration for a Pool and produces one via Build.
type PoolBuilder[T any] struct {
	disabled    bool
	constructor func() T
	reset       func(T) bool
}

// NewPool begins building a Pool. The constructor function is called whenever
// the underlying sync.Pool needs to allocate a fresh object. Call Build to
// obtain the final Pool.
func NewPool[T any](constructor func() T) *PoolBuilder[T] {
	return &PoolBuilder[T]{constructor: constructor}
}

// WithReset registers a function that is called on every object returned to the
// pool via Put, allowing callers to clear or reinitialise the object before
// it is reused.
// If this function returns true the object will be returned to the pool, otherwise it will be discarded.
func (b *PoolBuilder[T]) WithReset(fn func(T) bool) *PoolBuilder[T] {
	b.reset = fn
	return b
}

// WithDisabled registers a flag that indicates whether the pool is disabled.
// If the pool is disabled, the pool will not return any objects to the pool,
// rendering the pool effectively a no-op.
// This is useful to gate usage of pools via flags.
func (b *PoolBuilder[T]) WithDisabled(disabled bool) *PoolBuilder[T] {
	b.disabled = disabled
	return b
}

// Build creates the Pool from the accumulated builder configuration.
func (b *PoolBuilder[T]) Build() *Pool[T] {
	p := &Pool[T]{reset: b.reset, disabled: b.disabled}
	p.inner.New = func() any { return b.constructor() }
	return p
}

// Get retrieves an object from the pool and a Return function. The Return
// function puts the object back into the pool (applying the reset function if
// one was configured). Typical usage:
//
//	obj, ret := pool.Get()
//	defer ret(obj)
func (p *Pool[T]) Get() (obj T, Return func(obj T)) {
	obj = p.inner.Get().(T)
	return obj, p.Put
}

// Put returns a T to the pool.
// This is exposed in the off-chance that the caller wants to manually return a T to the pool.
// In most cases, the Return function returned by Get should be used instead.
func (p *Pool[T]) Put(obj T) {
	if p.disabled {
		return
	}
	shouldPut := true
	if p.reset != nil {
		shouldPut = p.reset(obj)
	}
	if shouldPut {
		p.inner.Put(obj)
	}
}
