// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package runutil provides helpers to advanced function scheduling control like repeat or retry.
//
// It's very often the case when you need to executes some code every fixed intervals or have it retried automatically.
// To make it reliably with proper timeout, you need to carefully arrange some boilerplate for this.
// Below function does it for you.
//
// For repeat executes, use Repeat:
//
//	err := runutil.Repeat(10*time.Second, stopc, func() error {
//		// ...
//	})
//err := runutil.RepeatWithJitter(30*time.Second, ctx, 0.05, func() error {
// Your code here
//})

// Retry starts executing closure function f until no error is returned from f:
//
//	err := runutil.Retry(10*time.Second, stopc, func() error {
//		// ...
//	})
//
// For logging an error on each f error, use RetryWithLog:
//
//	err := runutil.RetryWithLog(logger, 10*time.Second, stopc, func() error {
//		// ...
//	})
//
// Another use case for runutil package is when you want to close a `Closer` interface. As we all know, we should close all implements of `Closer`, such as *os.File. Commonly we will use:
//
//	defer closer.Close()
//
// The problem is that Close() usually can return important error e.g for os.File the actual file flush might happen (and fail) on `Close` method. It's important to *always* check error. Thanos provides utility functions to log every error like those, allowing to put them in convenient `defer`:
//
//	defer runutil.CloseWithLogOnErr(logger, closer, "log format message")
//
// For capturing error, use CloseWithErrCapture:
//
//	var err error
//	defer runutil.CloseWithErrCapture(&err, closer, "log format message")
//
//	// ...
//
// If Close() returns error, err will capture it and return by argument.
//
// The rununtil.Exhaust* family of functions provide the same functionality but
// they take an io.ReadCloser and they exhaust the whole reader before closing
// them. They are useful when trying to use http keep-alive connections because
// for the same connection to be reused the whole response body needs to be
// exhausted.
package runutil

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/errutil"
)

// RepeatInfinitely executes f every interval seconds until stopc is closed or f returns an error.
func RepeatInfinitely(logger log.Logger, interval time.Duration, stopc <-chan struct{}, f func() error) {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		if err := f(); err != nil {
			level.Error(logger).Log("msg", "function failed. Retrying in next tick", "err", err)
		}
		select {
		case <-stopc:
			return
		case <-tick.C:
		}
	}
}

// Repeat executes f every interval seconds until stopc is closed or f returns an error.
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

// RepeatWithJitter executes f with a random jitter added to the interval between each execution.
// It continues until ctx is done or f returns an error.
// The jitter factor should be between 0 and 1, where 0 means no jitter and 1 means the interval can vary from 0 to 2 times the original interval.
func RepeatWithJitter(ctx context.Context, interval time.Duration, jitterFactor float64, f func() error) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := f(); err != nil {
				return err
			}

			jitter := time.Duration(float64(interval) * jitterFactor)

			jitteredInterval := interval + time.Duration(rand.Float64()*float64(jitter))

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(jitteredInterval):
			}
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
func CloseWithLogOnErr(logger log.Logger, closer io.Closer, format string, a ...any) {
	err := closer.Close()
	if err == nil {
		return
	}

	// Not a problem if it has been closed already.
	if errors.Is(err, os.ErrClosed) {
		return
	}

	if logger == nil {
		logger = log.NewLogfmtLogger(os.Stderr)
	}

	level.Warn(logger).Log("msg", "detected close error", "err", errors.Wrap(err, fmt.Sprintf(format, a...)))
}

// ExhaustCloseWithLogOnErr closes the io.ReadCloser with a log message on error but exhausts the reader before.
func ExhaustCloseWithLogOnErr(logger log.Logger, r io.ReadCloser, format string, a ...any) {
	_, err := io.Copy(io.Discard, r)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to exhaust reader, performance may be impeded", "err", err)
	}

	CloseWithLogOnErr(logger, r, format, a...)
}

// CloseWithErrCapture closes closer, wraps any error with message from fmt and args, and stores this in err.
func CloseWithErrCapture(err *error, closer io.Closer, format string, a ...any) {
	merr := errutil.MultiError{}

	merr.Add(*err)
	merr.Add(errors.Wrapf(closer.Close(), format, a...))

	*err = merr.Err()
}

// ExhaustCloseWithErrCapture closes the io.ReadCloser with error capture but exhausts the reader before.
func ExhaustCloseWithErrCapture(err *error, r io.ReadCloser, format string, a ...any) {
	_, copyErr := io.Copy(io.Discard, r)

	CloseWithErrCapture(err, r, format, a...)

	// Prepend the io.Copy error.
	merr := errutil.MultiError{}
	merr.Add(copyErr)
	merr.Add(*err)

	*err = merr.Err()
}

// DeleteAll deletes all files and directories inside the given
// dir except for the ignoreDirs directories.
// NOTE: DeleteAll is not idempotent.
func DeleteAll(dir string, ignoreDirs ...string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "read dir")
	}
	var groupErrs errutil.MultiError

	var matchingIgnores []string
	for _, d := range entries {
		if !d.IsDir() {
			if err := os.RemoveAll(filepath.Join(dir, d.Name())); err != nil {
				groupErrs.Add(err)
			}
			continue
		}

		// ignoreDirs might be multi-directory paths.
		matchingIgnores = matchingIgnores[:0]
		ignore := false
		for _, ignoreDir := range ignoreDirs {
			id := strings.Split(ignoreDir, "/")
			if id[0] == d.Name() {
				if len(id) == 1 {
					ignore = true
					break
				}
				matchingIgnores = append(matchingIgnores, filepath.Join(id[1:]...))
			}
		}

		if ignore {
			continue
		}

		if len(matchingIgnores) == 0 {
			if err := os.RemoveAll(filepath.Join(dir, d.Name())); err != nil {
				groupErrs.Add(err)
			}
			continue
		}
		if err := DeleteAll(filepath.Join(dir, d.Name()), matchingIgnores...); err != nil {
			groupErrs.Add(err)
		}
	}

	if groupErrs.Err() != nil {
		return errors.Wrap(groupErrs.Err(), "delete file/dir")
	}
	return nil
}
