package runutil

import (
	"os"
	"time"

	"io"

	"fmt"

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
