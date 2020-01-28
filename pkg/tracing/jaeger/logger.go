// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package jaeger

import (
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

type jaegerLogger struct {
	logger log.Logger
}

func (l *jaegerLogger) Infof(format string, args ...interface{}) {
	level.Info(l.logger).Log("msg", fmt.Sprintf(format, args...))
}

func (l *jaegerLogger) Error(msg string) {
	level.Error(l.logger).Log("msg", msg)
}
