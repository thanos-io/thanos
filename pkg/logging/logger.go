// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"os"

	"github.com/coreos/go-systemd/v22/journal"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
)

const (
	LogFormatLogfmt   = "logfmt"
	LogFormatJSON     = "json"
	LogFormatJournald = "journald"
)

type LevelLogger struct {
	log.Logger
	LogLevel string
}

// NewLogger returns a log.Logger that prints in the provided format at the
// provided level with a UTC timestamp and the caller of the log entry. If non
// empty, the debug name is also appended as a field to all log lines. Panics
// if the log level is not error, warn, info or debug. Log level is expected to
// be validated before passed to this function.
func NewLogger(logLevel, logFormat, debugName string) (log.Logger, error) {
	var (
		logger log.Logger
		lvl    level.Option
	)

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
		// This enum is already checked and enforced by flag validations, so
		// this should never happen.
		panic("unexpected log level")
	}

	logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	switch logFormat {
	case LogFormatJSON:
		logger = log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
	case LogFormatJournald:
		if journal.Enabled() {
			logger = newJournaldLogger()
		} else {
			return nil, errors.New("journald log format requested but systemd journal is not available")
		}
	}

	// Sort the logger chain to avoid expensive log.Valuer evaluation for disallowed level.
	// Ref: https://github.com/go-kit/log/issues/14#issuecomment-945038252
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.Caller(5))
	logger = level.NewFilter(logger, lvl)

	if debugName != "" {
		logger = log.With(logger, "name", debugName)
	}

	return LevelLogger{
		Logger:   logger,
		LogLevel: logLevel,
	}, nil
}
