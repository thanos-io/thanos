// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"sync"
	"testing"

	"github.com/efficientgo/core/testutil"
)

// Ensure that the processor does not stop if there are still operations waiting in the queue.
func TestAsyncOp(t *testing.T) {
	for i := 0; i < 1000; i++ {
		runTest(t)
	}
}

func runTest(t *testing.T) {
	p := NewAsyncOperationProcessor(100, 10)
	mtx := sync.Mutex{}
	var acc int = 0

	for i := 0; i < 100; i++ {
		err := p.EnqueueAsync(func() {
			mtx.Lock()
			defer mtx.Unlock()
			acc += 1
		})
		testutil.Ok(t, err)
	}

	p.Stop()
	testutil.Equals(t, 100, acc)
}
