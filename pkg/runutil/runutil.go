// Package runutil provides helpers to advanced function scheduling control like repeat or retry.
//
// While meeting a scenario to do something every fixed intervals or be retried automatically,
// it's a common thinking to use time.Ticker or for-loop. Here we provide easy ways to implement those by closure function.
//
// For repeat executes, use Repeat:
//
// 	err := runutil.Repeat(10*time.Second, stopc, func() error {
// 		// ...
// 	})
//
// Retry starts executing closure function f until no error is returned from f:
//
// 	err := runutil.Retry(10*time.Second, stopc, func() error {
// 		// ...
// 	})
//
// For logging an error on each f error, use RetryWithLog:
//
// 	err := runutil.RetryWithLog(logger, 10*time.Second, stopc, func() error {
// 		// ...
// 	})
//
// As we all know, we should close all implements of io.Closer in Golang, such as *os.File. Commonly we will use:
//
// 	defer closer.Close()
//
// The Close() will return error if it has been already closed, sometimes we will ignore it. Thanos provides utility functions to log every error like those:
//
// 	defer runutil.CloseWithLogOnErr(logger, closer, "log format message")
//
// For capturing error, use CloseWithErrCapture:
//
// 	var err error
// 	defer runutil.CloseWithErrCapture(logger, &err, closer, "log format message")
//
// 	// ...
//
// If Close() returns error, err will capture it and return by argument.
package runutil

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// Repeat executes f every interval seconds until stopc is closed.
// It executes f once right after being called.
func Repeat(interval time.Duration, stopc <-chan struct{}, f func() error) error {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		if err := f(); err != nil {
			return err
		}
		select {
		case <-stopc:
			return nil
		case <-tick.C:
		}
	}
}

// Retry executes f every interval seconds until timeout or no error is returned from f.
func Retry(interval time.Duration, stopc <-chan struct{}, f func() error) error {
	return RetryWithLog(log.NewNopLogger(), interval, stopc, f)
}

// RetryWithLog executes f every interval seconds until timeout or no error is returned from f. It logs an error on each f error.
func RetryWithLog(logger log.Logger, interval time.Duration, stopc <-chan struct{}, f func() error) error {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	var err error
	for {
		if err = f(); err == nil {
			return nil
		}
		level.Error(logger).Log("msg", "function failed. Retrying in next tick", "err", err)
		select {
		case <-stopc:
			return err
		case <-tick.C:
		}
	}
}

// CloseWithLogOnErr is making sure we log every error, even those from best effort tiny closers.
func CloseWithLogOnErr(logger log.Logger, closer io.Closer, format string, a ...interface{}) {
	err := closer.Close()
	if err == nil {
		return
	}

	if logger == nil {
		logger = log.NewLogfmtLogger(os.Stderr)
	}

	level.Warn(logger).Log("msg", "detected close error", "err", errors.Wrap(err, fmt.Sprintf(format, a...)))
}

// CloseWithErrCapture runs function and on error tries to return error by argument.
// If error is already there we assume that error has higher priority and we just log the function error.
func CloseWithErrCapture(logger log.Logger, err *error, closer io.Closer, format string, a ...interface{}) {
	closeErr := closer.Close()
	if closeErr == nil {
		return
	}

	if *err == nil {
		err = &closeErr
		return
	}

	// There is already an error, let's log this one.
	if logger == nil {
		logger = log.NewLogfmtLogger(os.Stderr)
	}

	level.Warn(logger).Log(
		"msg", "detected best effort close error that was preempted from the more important one",
		"err", errors.Wrap(closeErr, fmt.Sprintf(format, a...)),
	)
}
