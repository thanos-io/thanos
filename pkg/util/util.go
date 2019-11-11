// Package util houses various utilities that do not fit anywhere else.
package util

import (
	"runtime/debug"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// RecoverGoroutine is a wrapper to be used for calling new goroutines / using run.Group
// which handles panics gracefully - prints the info out to the error log
// and returns an error.
func RecoverGoroutine(logger log.Logger, f func() error, extrafields ...interface{}) func() error {
	return func() (err error) {
		defer func() {
			if p := recover(); p != nil {
				level.Error(logger).Log("msg", "recovered from panic", "panic", p, "stack", debug.Stack())
				level.Error(logger).Log("msg", "extra info")
				for _, f := range extrafields {
					level.Error(logger).Log("extra", f)
				}
				level.Error(logger).Log("msg", "please fill a new issue with this message in https://github.com/thanos-io/thanos")
				err = errors.Errorf("goroutine encountered %s", p)
			}
		}()
		err = f()
		return
	}
}
