// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package runutil

import (
	"fmt"
	"os"
	"testing"

	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/assert"
)

func TestCloseWithLogOnErr(t *testing.T) {
	t.Run("With non-close error", func(t *testing.T) {
		closer := fakeCloser{err: fmt.Errorf("an error")}
		logger := fakeLogger{}

		CloseWithLogOnErr(&logger, closer, "closing failed")

		assert.Equal(t, []interface{}{
			"level", level.WarnValue(), "msg", "detected close error", "err", "closing failed: an error",
		}, logger.keyvals)
	})

	t.Run("With no error", func(t *testing.T) {
		closer := fakeCloser{}
		logger := fakeLogger{}

		CloseWithLogOnErr(&logger, closer, "closing failed")

		assert.Empty(t, logger.keyvals)
	})

	t.Run("With closed error", func(t *testing.T) {
		closer := fakeCloser{err: os.ErrClosed}
		logger := fakeLogger{}

		CloseWithLogOnErr(&logger, closer, "closing failed")

		assert.Empty(t, logger.keyvals)
	})
}

type fakeCloser struct {
	err error
}

func (c fakeCloser) Close() error {
	return c.err
}

type fakeLogger struct {
	keyvals []interface{}
}

func (l *fakeLogger) Log(keyvals ...interface{}) error {
	l.keyvals = keyvals
	return nil
}
