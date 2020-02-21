// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package runutil_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/runutil"
)

func ExampleRepeat() {
	// It will stop Repeat 10 seconds later.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// It will print out "Repeat" every 5 seconds.
	err := runutil.Repeat(5*time.Second, ctx.Done(), func() error {
		fmt.Println("Repeat")
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleRetry() {
	// It will stop Retry 10 seconds later.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// It will print out "Retry" every 5 seconds.
	err := runutil.Retry(5*time.Second, ctx.Done(), func() error {
		fmt.Println("Retry")
		return errors.New("Try to retry")
	})
	if err != nil {
		log.Fatal(err)
	}
}
