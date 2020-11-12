// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type ReaderPool struct {
	lazyReaderEnabled     bool
	lazyReaderIdleTimeout time.Duration
	lazyReaderMetrics     *LazyBinaryReaderMetrics
	logger                log.Logger

	// Channel used to signal once the pool is closing.
	close chan struct{}

	// Keep track of all readers managed by the pool.
	readersMx sync.Mutex
	readers   map[*readerTracker]struct{}
}

func NewReaderPool(logger log.Logger, lazyReaderEnabled bool, lazyReaderIdleTimeout time.Duration, reg prometheus.Registerer) *ReaderPool {
	p := &ReaderPool{
		logger:                logger,
		lazyReaderEnabled:     lazyReaderEnabled,
		lazyReaderIdleTimeout: lazyReaderIdleTimeout,
		lazyReaderMetrics:     NewLazyBinaryReaderMetrics(extprom.WrapRegistererWithPrefix("indexheader_pool_", reg)),
		readers:               make(map[*readerTracker]struct{}),
		close:                 make(chan struct{}),
	}

	// Start a goroutine to close idle readers (only if required).
	if p.lazyReaderEnabled && p.lazyReaderIdleTimeout > 0 {
		checkFreq := p.lazyReaderIdleTimeout / 10

		go func() {
			for {
				select {
				case <-p.close:
					return
				case <-time.After(checkFreq):
					p.closeIdleReaders()
				}
			}
		}()
	}

	return p
}

func (p *ReaderPool) NewBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int) (Reader, error) {
	var reader Reader
	var err error

	if p.lazyReaderEnabled {
		reader, err = NewLazyBinaryReader(ctx, logger, bkt, dir, id, postingOffsetsInMemSampling, p.lazyReaderMetrics)
	} else {
		reader, err = NewBinaryReader(ctx, logger, bkt, dir, id, postingOffsetsInMemSampling)
	}

	if err != nil {
		return nil, err
	}

	// Keep track of lazy readers only if required.
	if p.lazyReaderEnabled && p.lazyReaderIdleTimeout > 0 {
		reader = &readerTracker{
			reader: reader,
			pool:   p,
			usedAt: atomic.NewInt64(time.Now().UnixNano()),
		}

		p.readersMx.Lock()
		p.readers[reader.(*readerTracker)] = struct{}{}
		p.readersMx.Unlock()
	}

	return reader, err
}

// Close the pool and stop checking for idle readers. No reader tracked by this pool
// will be closed. It's the caller responsibility to close readers.
func (p *ReaderPool) Close() {
	close(p.close)
}

func (p *ReaderPool) closeIdleReaders() {
	for _, r := range p.getIdleReaders() {
		// Closing an already closed reader is a no-op, so we close it and just update
		// the last timestamp on success. If it will be still be idle the next time this
		// function is called, we'll try to close it again and will just be a no-op.
		//
		// Due to concurrency, the current implementation may close a reader which was
		// use between when the list of idle readers has been computed and now. This is
		// an edge case we're willing to accept, to not further complicate the logic.
		if err := r.reader.Close(); err != nil {
			level.Warn(p.logger).Log("msg", "failed to close idle index-header reader", "err", err)
		}

		// Update the used timestamp so that we'll not call Close() again until the next
		// idle timeout is hit.
		r.usedAt.Store(time.Now().UnixNano())
	}
}

func (p *ReaderPool) getIdleReaders() []*readerTracker {
	p.readersMx.Lock()
	defer p.readersMx.Unlock()

	var idle []*readerTracker
	threshold := time.Now().Add(-p.lazyReaderIdleTimeout).UnixNano()

	for r := range p.readers {
		if r.usedAt.Load() < threshold {
			idle = append(idle, r)
		}
	}

	return idle
}

func (p *ReaderPool) isTracking(r *readerTracker) bool {
	p.readersMx.Lock()
	defer p.readersMx.Unlock()

	_, ok := p.readers[r]
	return ok
}

func (p *ReaderPool) onReaderClosed(r *readerTracker) {
	p.readersMx.Lock()
	defer p.readersMx.Unlock()

	// When this function is called, it means the reader has been closed NOT because was idle
	// but because the consumer closed it. By contract, a reader closed by the consumer can't
	// be used anymore, so we can automatically remove it from the pool.
	delete(p.readers, r)
}

type readerTracker struct {
	reader Reader
	pool   *ReaderPool
	usedAt *atomic.Int64
}

func (r *readerTracker) Close() error {
	r.pool.onReaderClosed(r)
	return r.reader.Close()
}

func (r *readerTracker) IndexVersion() (int, error) {
	r.usedAt.Store(time.Now().UnixNano())
	return r.reader.IndexVersion()
}

func (r *readerTracker) PostingsOffset(name string, value string) (index.Range, error) {
	r.usedAt.Store(time.Now().UnixNano())
	return r.reader.PostingsOffset(name, value)
}

func (r *readerTracker) LookupSymbol(o uint32) (string, error) {
	r.usedAt.Store(time.Now().UnixNano())
	return r.reader.LookupSymbol(o)
}

func (r *readerTracker) LabelValues(name string) ([]string, error) {
	r.usedAt.Store(time.Now().UnixNano())
	return r.reader.LabelValues(name)
}

func (r *readerTracker) LabelNames() ([]string, error) {
	r.usedAt.Store(time.Now().UnixNano())
	return r.reader.LabelNames()
}
