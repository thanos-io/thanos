package runutil_test

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/improbable-eng/thanos/pkg/runutil"

	"github.com/go-kit/kit/log"
)

func ExampleRepeat() {
	stopc := make(chan struct{})

	// It will stop Repeat 11 seconds later.
	go func() {
		select {
		case <-time.After(11 * time.Second):
			close(stopc)
		}
	}()

	// It will print out "Repeat" every 5 seconds.
	err := runutil.Repeat(5*time.Second, stopc, func() error {
		fmt.Println("Repeat")
		return nil
	})

	if err != nil {
		fmt.Println(err)
	}
}

func ExampleRetry() {
	stopc := make(chan struct{})

	// It will stop Retry 11 seconds later.
	go func() {
		select {
		case <-time.After(11 * time.Second):
			close(stopc)
		}
	}()
	// It will print out "Retry" every 5 seconds.
	err := runutil.Retry(5*time.Second, stopc, func() error {
		fmt.Println("Retry")
		return errors.New("Try to retry.")
	})

	if err != nil {
		fmt.Println(err)
	}
}

func ExampleRetryWithLog() {
	logger := log.NewLogfmtLogger(os.Stdout)
	stopc := make(chan struct{})

	// It will stop RetryWithLog 11 seconds later.
	go func() {
		select {
		case <-time.After(11 * time.Second):
			close(stopc)
		}
	}()

	// It will print out "RetryWithLog" every 5 seconds.
	err := runutil.RetryWithLog(logger, 5*time.Second, stopc, func() error {
		fmt.Println("RetryWithLog")
		return errors.New("Try to retry.")
	})

	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// RetryWithLog
	// level=error msg="function failed. Retrying in next tick" err="Try to retry."
	// RetryWithLog
	// level=error msg="function failed. Retrying in next tick" err="Try to retry."
	// RetryWithLog
	// level=error msg="function failed. Retrying in next tick" err="Try to retry."
	// Try to retry.
}
