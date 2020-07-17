// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import "time"

// Limiter implements the Limits interface in Cortex frontend
// https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/limits.go#L17.
type Limiter struct {
	maxQueryLength      time.Duration
	maxCacheFreshness   time.Duration
	maxQueryParallelism int
}

func NewLimiter(
	maxQueryParallelism int,
	maxQueryLength time.Duration,
	maxCacheFreshness time.Duration,
) *Limiter {
	return &Limiter{
		maxQueryLength:      maxQueryLength,
		maxCacheFreshness:   maxCacheFreshness,
		maxQueryParallelism: maxQueryParallelism,
	}
}

func (l *Limiter) MaxQueryLength(_ string) time.Duration {
	return l.maxQueryLength
}

func (l *Limiter) MaxQueryParallelism(_ string) int {
	return l.maxQueryParallelism
}

func (l *Limiter) MaxCacheFreshness(_ string) time.Duration {
	return l.maxCacheFreshness
}
