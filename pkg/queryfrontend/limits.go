// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import "time"

// Limits implements the Limits interface in Cortex frontend
// https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/limits.go#L17.
type Limits struct {
	maxQueryLength      time.Duration
	maxCacheFreshness   time.Duration
	maxQueryParallelism int
}

func NewLimits(
	maxQueryParallelism int,
	maxQueryLength time.Duration,
	maxCacheFreshness time.Duration,
) *Limits {
	return &Limits{
		maxQueryLength:      maxQueryLength,
		maxCacheFreshness:   maxCacheFreshness,
		maxQueryParallelism: maxQueryParallelism,
	}
}

func (l *Limits) MaxQueryLength(_ string) time.Duration {
	return l.maxQueryLength
}

func (l *Limits) MaxQueryParallelism(_ string) int {
	return l.maxQueryParallelism
}

func (l *Limits) MaxCacheFreshness(_ string) time.Duration {
	return l.maxCacheFreshness
}
