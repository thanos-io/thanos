// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logutil

import (
	"context"
	"log/slog"
	"testing"

	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/logging"
)

func Test_GoKitLogToSlog(t *testing.T) {
	ctx := context.Background()
	logLevels := []string{"debug", "info", "warn", "error"}
	slogLevels := []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError}

	for _, logFormat := range []string{"logfmt", "json"} {
		for i, lv := range logLevels {
			logger, err := logging.NewLogger(lv, logFormat, "test")
			require.NoError(t, err)

			slog := GoKitLogToSlog(logger)
			for j, slogLv := range slogLevels {
				if i <= j {
					t.Logf("[logFormat: %v, go-kit log level: %v, slog level: %v] slog should be enabled", logFormat, lv, slogLv)
					require.True(t, slog.Enabled(ctx, slogLv))
				} else {
					t.Logf("[logFormat: %v, go-kit log level: %v, slog level: %v] slog should be disabled", logFormat, lv, slogLv)
					require.False(t, slog.Enabled(ctx, slogLv))
				}

				switch lv {
				case "debug":
					level.Debug(logger).Log("msg", "message", "debug", lv)
					slog.Debug("message", "debug", lv)
				case "info":
					level.Info(logger).Log("msg", "message", "info", lv)
					slog.Info("message", "info", lv)
				case "warn":
					level.Warn(logger).Log("msg", "message", "warn", lv)
					slog.Warn("message", "warn", lv)
				case "error":
					level.Error(logger).Log("msg", "message", "error", lv)
					slog.Error("message", "error", lv)
				}
			}
		}
	}
}
