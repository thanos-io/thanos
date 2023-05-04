// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"testing"

	"github.com/go-kit/log/level"
)

func BenchmarkDisallowedLogLevels(b *testing.B) {
	logger := NewLogger("warn", "logfmt", "benchmark")

	for i := 0; i < b.N; i++ {
		level.Info(logger).Log("hello", "world", "number", i)
		level.Debug(logger).Log("hello", "world", "number", i)
	}
}
