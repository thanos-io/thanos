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

	// Go waits until the next worker becomes available or the context is canceled.
	// Returns ctx.Err() if the context is canceled before a worker slot is available.
	Go(ctx context.Context, work Work) error

	// Close cancels all workers and waits for them to finish.
	Close()

	// Size returns the number of workers in the pool.
	Size() int
}

type workerPool struct {
	sync.Once
	ctx    context.Context
	workCh chan Work
	cancel context.CancelFunc
}

func NewWorkerPool(workers uint) WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &workerPool{
		ctx:    ctx,
		cancel: cancel,
		workCh: make(chan Work, workers),
	}
}

func (p *workerPool) Init() {
	p.Do(func() {
		for i := 0; i < p.Size(); i++ {
			go func() {
				for {
					select {
					case <-p.ctx.Done():
						// TODO: exhaust workCh before exit
						return
					case work := <-p.workCh:
						work()
					}
				}
			}()
		}
	})
}

func (p *workerPool) Go(ctx context.Context, work Work) error {
	p.Init()
	select {
	case p.workCh <- work:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *workerPool) Close() {
	p.cancel()
}

func (p *workerPool) Size() int {
	return cap(p.workCh)
}
