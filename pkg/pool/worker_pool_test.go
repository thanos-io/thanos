// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pool

import (
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
		p.Go(func() {
			mu.Lock()
			defer mu.Unlock()
			expectedWorksDone++
			wg.Done()
		})
	}
	wg.Wait()
	require.Equal(t, uint32(workerPoolSize*3), expectedWorksDone)
	require.Equal(t, int(workerPoolSize), p.Size())
}
