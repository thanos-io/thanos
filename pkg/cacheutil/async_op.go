// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"sync"

	"github.com/pkg/errors"
)

type asyncOperationProcessor struct {
	// Channel used to notify internal goroutines when they should quit.
	stop chan struct{}

	// Channel used to enqueue async operations.
	asyncQueue chan func()

	// Wait group used to wait all workers on stopping.
	workers sync.WaitGroup
}

func newAsyncOperationProcessor(bufferSize, concurrency int) *asyncOperationProcessor {
	p := &asyncOperationProcessor{
		stop:       make(chan struct{}, 1),
		asyncQueue: make(chan func(), bufferSize),
	}

	p.workers.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go p.asyncQueueProcessLoop()
	}

	return p
}

func (p *asyncOperationProcessor) Stop() {
	close(p.stop)

	// Wait until all workers have terminated.
	p.workers.Wait()
}

func (p *asyncOperationProcessor) asyncQueueProcessLoop() {
	defer p.workers.Done()

	for {
		select {
		case op := <-p.asyncQueue:
			op()
		case <-p.stop:
			return
		}
	}
}

var errAsyncBufferFull = errors.New("the async buffer is full")

func (p *asyncOperationProcessor) enqueueAsync(op func()) error {
	select {
	case p.asyncQueue <- op:
		return nil
	default:
		return errAsyncBufferFull
	}
}
