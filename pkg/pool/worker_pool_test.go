// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pool

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGo(t *testing.T) {
	var expectedWorksDone uint32
	var workerPoolSize uint
	var mu sync.Mutex
	workerPoolSize = 5
	p := NewWorkerPool(workerPoolSize)
	p.Init()
	defer p.Close()

	var wg sync.WaitGroup
	for i := 0; i < int(workerPoolSize*3); i++ {
		wg.Add(1)
		err := p.Go(context.Background(), func() {
			mu.Lock()
			defer mu.Unlock()
			expectedWorksDone++
			wg.Done()
		})
		require.NoError(t, err)
	}
	wg.Wait()
	require.Equal(t, uint32(workerPoolSize*3), expectedWorksDone)
	require.Equal(t, int(workerPoolSize), p.Size())
}

func TestGo_ContextCanceled(t *testing.T) {
	p := NewWorkerPool(1)
	p.Init()
	defer p.Close()

	// Fill the pool's buffer and worker with blocking work.
	blocked := make(chan struct{})
	defer close(blocked)

	// Fill the worker (1 worker running this).
	err := p.Go(context.Background(), func() { <-blocked })
	require.NoError(t, err)

	// Fill the buffer (capacity 1).
	err = p.Go(context.Background(), func() {})
	require.NoError(t, err)

	// Now the pool is saturated. A canceled context should return immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = p.Go(ctx, func() { t.Fatal("work should not execute") })
	require.ErrorIs(t, err, context.Canceled)
}
