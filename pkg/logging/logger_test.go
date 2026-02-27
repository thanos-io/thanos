// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"testing"

	"github.com/go-kit/log/level"
)

func BenchmarkDisallowedLogLevels(b *testing.B) {
	logger, err := NewLogger("warn", "logfmt", "benchmark")
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; b.Loop(); i++ {
		level.Info(logger).Log("hello", "world", "number", i)
		level.Debug(logger).Log("hello", "world", "number", i)
	}
}
