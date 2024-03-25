// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// ReaderPoolMetrics holds metrics tracked by ReaderPool.
type ReaderPoolMetrics struct {
	lazyReader   *LazyBinaryReaderMetrics
	binaryReader *BinaryReaderMetrics
}

// NewReaderPoolMetrics makes new ReaderPoolMetrics.
func NewReaderPoolMetrics(reg prometheus.Registerer) *ReaderPoolMetrics {
	return &ReaderPoolMetrics{
		lazyReader:   NewLazyBinaryReaderMetrics(reg),
		binaryReader: NewBinaryReaderMetrics(reg),
	}
}

// ReaderPool is used to istantiate new index-header readers and keep track of them.
// When the lazy reader is enabled, the pool keeps track of all instantiated readers
// and automatically close them once the idle timeout is reached. A closed lazy reader
// will be automatically re-opened upon next usage.
type ReaderPool struct {
	lazyReaderEnabled     bool
	lazyReaderIdleTimeout time.Duration
	logger                log.Logger
	metrics               *ReaderPoolMetrics

	// Channel used to signal once the pool is closing.
	close chan struct{}

	// Keep track of all readers managed by the pool.
	lazyReadersMx sync.Mutex
	lazyReaders   map[*LazyBinaryReader]struct{}

	lazyDownloadFunc LazyDownloadIndexHeaderFunc
}

// IndexHeaderLazyDownloadStrategy specifies how to download index headers
// lazily. Only used when lazy mmap is enabled.
type IndexHeaderLazyDownloadStrategy string

const (
	// EagerDownloadStrategy always disables lazy downloading index headers.
	EagerDownloadStrategy IndexHeaderLazyDownloadStrategy = "eager"
	// LazyDownloadStrategy always lazily download index headers.
	LazyDownloadStrategy IndexHeaderLazyDownloadStrategy = "lazy"
)

func (s IndexHeaderLazyDownloadStrategy) StrategyToDownloadFunc() LazyDownloadIndexHeaderFunc {
	switch s {
	case LazyDownloadStrategy:
		return AlwaysLazyDownloadIndexHeader
	default:
		// Always fallback to eager download index header.
		return AlwaysEagerDownloadIndexHeader
	}
}

// LazyDownloadIndexHeaderFunc is used to determinte whether to download the index header lazily
// or not by checking its block metadata. Usecase can be by time or by index file size.
type LazyDownloadIndexHeaderFunc func(meta *metadata.Meta) bool

// AlwaysEagerDownloadIndexHeader always eagerly download index header.
func AlwaysEagerDownloadIndexHeader(meta *metadata.Meta) bool {
	return false
}

// AlwaysLazyDownloadIndexHeader always lazily download index header.
func AlwaysLazyDownloadIndexHeader(meta *metadata.Meta) bool {
	return true
}

// NewReaderPool makes a new ReaderPool.
func NewReaderPool(logger log.Logger, lazyReaderEnabled bool, lazyReaderIdleTimeout time.Duration, metrics *ReaderPoolMetrics, lazyDownloadFunc LazyDownloadIndexHeaderFunc) *ReaderPool {
	p := &ReaderPool{
		logger:                logger,
		metrics:               metrics,
		lazyReaderEnabled:     lazyReaderEnabled,
		lazyReaderIdleTimeout: lazyReaderIdleTimeout,
		lazyReaders:           make(map[*LazyBinaryReader]struct{}),
		close:                 make(chan struct{}),
		lazyDownloadFunc:      lazyDownloadFunc,
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

// NewBinaryReader creates and returns a new binary reader. If the pool has been configured
// with lazy reader enabled, this function will return a lazy reader. The returned lazy reader
// is tracked by the pool and automatically closed once the idle timeout expires.
func (p *ReaderPool) NewBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int, meta *metadata.Meta) (Reader, error) {
	var reader Reader
	var err error

	if p.lazyReaderEnabled {
		reader, err = NewLazyBinaryReader(ctx, logger, bkt, dir, id, postingOffsetsInMemSampling, p.metrics.lazyReader, p.metrics.binaryReader, p.onLazyReaderClosed, p.lazyDownloadFunc(meta))
	} else {
		reader, err = NewBinaryReader(ctx, logger, bkt, dir, id, postingOffsetsInMemSampling, p.metrics.binaryReader)
	}

	if err != nil {
		return nil, err
	}

	// Keep track of lazy readers only if required.
	if p.lazyReaderEnabled && p.lazyReaderIdleTimeout > 0 {
		p.lazyReadersMx.Lock()
		p.lazyReaders[reader.(*LazyBinaryReader)] = struct{}{}
		p.lazyReadersMx.Unlock()
	}

	return reader, err
}

// Close the pool and stop checking for idle readers. No reader tracked by this pool
// will be closed. It's the caller responsibility to close readers.
func (p *ReaderPool) Close() {
	close(p.close)
}

func (p *ReaderPool) closeIdleReaders() {
	idleTimeoutAgo := time.Now().Add(-p.lazyReaderIdleTimeout).UnixNano()

	for _, r := range p.getIdleReadersSince(idleTimeoutAgo) {
		if err := r.unloadIfIdleSince(idleTimeoutAgo); err != nil && !errors.Is(err, errNotIdle) {
			level.Warn(p.logger).Log("msg", "failed to close idle index-header reader", "err", err)
		}
	}
}

func (p *ReaderPool) getIdleReadersSince(ts int64) []*LazyBinaryReader {
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	var idle []*LazyBinaryReader
	for r := range p.lazyReaders {
		if r.isIdleSince(ts) {
			idle = append(idle, r)
		}
	}

	return idle
}

func (p *ReaderPool) isTracking(r *LazyBinaryReader) bool {
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	_, ok := p.lazyReaders[r]
	return ok
}

func (p *ReaderPool) onLazyReaderClosed(r *LazyBinaryReader) {
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	// When this function is called, it means the reader has been closed NOT because was idle
	// but because the consumer closed it. By contract, a reader closed by the consumer can't
	// be used anymore, so we can automatically remove it from the pool.
	delete(p.lazyReaders, r)
}
