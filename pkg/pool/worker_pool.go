// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pool

import (
	"context"
	"sync"
)

// Work is a unit of item to be worked on, like Java Runnable.
type Work func()

// WorkerPool is a pool of goroutines that are reusable, similar to Java ThreadPool.
type WorkerPool interface {
	// Init initializes the worker pool.
	Init()

	// Go waits until the next worker becomes available and executes the given work.
	Go(work Work)

	// Close cancels all workers and waits for them to finish.
	Close()

	// Size returns the number of workers in the pool.
	Size() int
}

type workerPool struct {
	sync.Once
	workCh chan Work
	cancel context.CancelFunc
}

func NewWorkerPool(workers uint) WorkerPool {
	return &workerPool{
		workCh: make(chan Work, workers),
	}
}

func (p *workerPool) Init() {
	p.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		p.cancel = cancel

		for i := 0; i < cap(p.workCh); i++ {
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case work := <-p.workCh:
						work()
					}
				}
			}()
		}
	})
}

func (p *workerPool) Go(work Work) {
	p.Init()
	p.workCh <- work
}

func (p *workerPool) Close() {
	p.cancel()
}

func (p *workerPool) Size() int {
	return cap(p.workCh)
}
