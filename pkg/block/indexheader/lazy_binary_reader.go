// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type LazyBinaryReaderMetrics struct {
	loadCount         prometheus.Counter
	loadFailedCount   prometheus.Counter
	unloadCount       prometheus.Counter
	unloadFailedCount prometheus.Counter
	loadDuration      prometheus.Histogram
}

func NewLazyBinaryReaderMetrics(reg prometheus.Registerer) *LazyBinaryReaderMetrics {
	return &LazyBinaryReaderMetrics{
		loadCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "indexheader_lazy_load_total",
			Help: "Total number of index-header lazy load operations.",
		}),
		loadFailedCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "indexheader_lazy_load_failed_total",
			Help: "Total number of failed index-header lazy load operations.",
		}),
		unloadCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "indexheader_lazy_unload_total",
			Help: "Total number of index-header lazy unload operations.",
		}),
		unloadFailedCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "indexheader_lazy_unload_failed_total",
			Help: "Total number of failed index-header lazy unload operations.",
		}),
		loadDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "indexheader_lazy_load_duration_seconds",
			Help:    "Duration of the index-header lazy loading in seconds.",
			Buckets: []float64{0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5},
		}),
	}
}

type LazyBinaryReader struct {
	ctx                         context.Context
	logger                      log.Logger
	bkt                         objstore.BucketReader
	dir                         string
	filepath                    string
	id                          ulid.ULID
	postingOffsetsInMemSampling int
	metrics                     *LazyBinaryReaderMetrics

	readerMx  sync.RWMutex
	reader    *BinaryReader
	readerErr error
}

func NewLazyBinaryReader(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.BucketReader,
	dir string,
	id ulid.ULID,
	postingOffsetsInMemSampling int,
	metrics *LazyBinaryReaderMetrics,
) (*LazyBinaryReader, error) {
	filepath := filepath.Join(dir, id.String(), block.IndexHeaderFilename)

	// If the index-header doesn't exist we should download it.
	if _, err := os.Stat(filepath); err != nil {
		if !os.IsNotExist(err) {
			return nil, errors.Wrap(err, "read index header")
		}

		level.Debug(logger).Log("msg", "the index-header doesn't exist on disk; recreating", "path", filepath)

		start := time.Now()
		if err := WriteBinary(ctx, bkt, id, filepath); err != nil {
			return nil, errors.Wrap(err, "write index header")
		}

		level.Debug(logger).Log("msg", "built index-header file", "path", filepath, "elapsed", time.Since(start))
	}

	return &LazyBinaryReader{
		ctx:                         ctx,
		logger:                      logger,
		bkt:                         bkt,
		dir:                         dir,
		filepath:                    filepath,
		id:                          id,
		postingOffsetsInMemSampling: postingOffsetsInMemSampling,
		metrics:                     metrics,
	}, nil
}

func (r *LazyBinaryReader) Close() error {
	r.readerMx.Lock()
	defer r.readerMx.Unlock()

	if r.reader == nil {
		return nil
	}

	r.metrics.unloadCount.Inc()
	if err := r.reader.Close(); err != nil {
		r.metrics.unloadFailedCount.Inc()
		return err
	}

	r.reader = nil

	return nil
}

func (r *LazyBinaryReader) IndexVersion() (int, error) {
	r.readerMx.RLock()
	defer r.readerMx.RUnlock()

	if err := r.open(); err != nil {
		return 0, err
	}

	return r.reader.IndexVersion()
}

func (r *LazyBinaryReader) PostingsOffset(name string, value string) (index.Range, error) {
	r.readerMx.RLock()
	defer r.readerMx.RUnlock()

	if err := r.open(); err != nil {
		return index.Range{}, err
	}

	return r.reader.PostingsOffset(name, value)
}

func (r *LazyBinaryReader) LookupSymbol(o uint32) (string, error) {
	r.readerMx.RLock()
	defer r.readerMx.RUnlock()

	if err := r.open(); err != nil {
		return "", err
	}

	return r.reader.LookupSymbol(o)
}

func (r *LazyBinaryReader) LabelValues(name string) ([]string, error) {
	r.readerMx.RLock()
	defer r.readerMx.RUnlock()

	if err := r.open(); err != nil {
		return nil, err
	}

	return r.reader.LabelValues(name)
}

func (r *LazyBinaryReader) LabelNames() ([]string, error) {
	r.readerMx.RLock()
	defer r.readerMx.RUnlock()

	if err := r.open(); err != nil {
		return nil, err
	}

	return r.reader.LabelNames()
}

// open ensure the underlying binary index-header reader has been successfully opened. Returns
// an error on failure. This function MUST be called with the read lock already acquired.
func (r *LazyBinaryReader) open() error {
	// Nothing to do if we already tried opening it.
	if r.reader != nil {
		return nil
	} else if r.readerErr != nil {
		return r.readerErr
	}

	// Take the write lock to ensure we'll try to open it only once. Take again
	// the read lock once done.
	r.readerMx.RUnlock()
	r.readerMx.Lock()
	defer r.readerMx.RLock()
	defer r.readerMx.Unlock()

	// Ensure none else tried to open it in the meanwhile.
	if r.reader != nil {
		return nil
	} else if r.readerErr != nil {
		return r.readerErr
	}

	level.Debug(r.logger).Log("msg", "lazy loading index-header file", "path", r.filepath)
	r.metrics.loadCount.Inc()
	startTime := time.Now()

	reader, err := NewBinaryReader(r.ctx, r.logger, r.bkt, r.dir, r.id, r.postingOffsetsInMemSampling)
	if err != nil {
		level.Error(r.logger).Log("msg", "failed to lazy load index-header file", "path", r.filepath, "err", err)
		r.metrics.loadFailedCount.Inc()
		r.readerErr = err
		return err
	}

	r.reader = reader
	level.Info(r.logger).Log("msg", "lazy loaded index-header file", "path", r.filepath, "elapsed", time.Since(startTime))
	r.metrics.loadDuration.Observe(time.Since(startTime).Seconds())

	return nil
}
