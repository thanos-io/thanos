// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrAsyncBufferFull = errors.New("the async buffer is full")
)

type AsyncOperationProcessor struct {
	// Channel used to notify internal goroutines when they should quit.
	stop chan struct{}

	// Channel used to enqueue async operations.
	asyncQueue chan func()

	// Wait group used to wait all workers on stopping.
	workers sync.WaitGroup
}

// NewAsyncOperationProcessor creates an async processor with given bufferSize and concurrency.
func NewAsyncOperationProcessor(bufferSize, concurrency int) *AsyncOperationProcessor {
	p := &AsyncOperationProcessor{
		stop:       make(chan struct{}, 1),
		asyncQueue: make(chan func(), bufferSize),
	}

	p.workers.Add(concurrency)
	for range concurrency {
		go p.asyncQueueProcessLoop()
	}

	return p
}

func (p *AsyncOperationProcessor) Stop() {
	close(p.stop)

	// Wait until all workers have terminated.
	p.workers.Wait()
	close(p.asyncQueue)
}

func (p *AsyncOperationProcessor) asyncQueueProcessLoop() {
	defer p.workers.Done()

	for {
		select {
		case op := <-p.asyncQueue:
			op()
		case <-p.stop:
			// Run all remaining operations before stopping
			select {
			case op := <-p.asyncQueue:
				op()
				continue
			default:
				return
			}
		}
	}
}

// EnqueueAsync enqueues op to async queue. If enqueue failed, ErrAsyncBufferFull is returned.
func (p *AsyncOperationProcessor) EnqueueAsync(op func()) error {
	select {
	case p.asyncQueue <- op:
		return nil
	default:
		return ErrAsyncBufferFull
	}
}

// AsyncQueue exposes the async channel to caller directly. This is useful in unit test
// to drain the queue to make sure deterministic results.
func (p *AsyncOperationProcessor) AsyncQueue() <-chan func() {
	return p.asyncQueue
}
