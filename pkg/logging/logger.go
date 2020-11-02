// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"fmt"
	"os"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

const (
	LogFormatLogfmt = "logfmt"
	LogFormatJSON   = "json"
)

// NewLogger returns a log.Logger that prints in the provided format with a UTC
// timestamp and the caller of the log entry.
func NewLogger(logFormat, debugName string) log.Logger {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	if logFormat == LogFormatJSON {
		logger = log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
	}

	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	if debugName != "" {
		logger = log.With(logger, "name", debugName)
	}

	return logger
}

// WithLogLevel returns the passed logger with the appropriate log level
// filter. If an unknown log level is passed an error is returned alongside the
// original logger.
func WithLogLevel(logger log.Logger, logLevel string) (log.Logger, error) {
	var lvl level.Option
	switch logLevel {
	case "error":
		lvl = level.AllowError()
	case "warn":
		lvl = level.AllowWarn()
	case "info":
		lvl = level.AllowInfo()
	case "debug":
		lvl = level.AllowDebug()
	default:
		return logger, fmt.Errorf("unexpected log level: %s (expected error, warn, info or debug)", logLevel)
	}

	return level.NewFilter(logger, lvl), nil
}
