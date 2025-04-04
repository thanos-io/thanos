// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logutil

import (
	"log/slog"

	"github.com/go-kit/log"
	"github.com/thanos-io/thanos/pkg/logging"
	sloggk "github.com/tjhop/slog-gokit"
)

// GoKitLogToSlog convert go-kit/log to slog.
func GoKitLogToSlog(logger log.Logger) *slog.Logger {
	levelVar := slog.LevelVar{}
	levelLogger, ok := logger.(logging.LevelLogger)
	if !ok {
		levelVar.Set(slog.LevelDebug)
	} else {
		switch levelLogger.LogLevel {
		case "debug":
			levelVar.Set(slog.LevelDebug)
		case "info":
			levelVar.Set(slog.LevelInfo)
		case "warn":
			levelVar.Set(slog.LevelWarn)
		case "error":
			levelVar.Set(slog.LevelError)
		}
	}
	return slog.New(sloggk.NewGoKitHandler(logger, &levelVar))
}
