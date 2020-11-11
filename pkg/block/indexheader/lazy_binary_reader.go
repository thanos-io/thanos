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
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type LazyBinaryReader struct {
	ctx                         context.Context
	logger                      log.Logger
	bkt                         objstore.BucketReader
	dir                         string
	filepath                    string
	id                          ulid.ULID
	postingOffsetsInMemSampling int

	readerMx  sync.RWMutex
	reader    *BinaryReader
	readerErr error
}

func NewLazyBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int) (*LazyBinaryReader, error) {
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
	}, nil
}

func (r *LazyBinaryReader) Close() error {
	r.readerMx.Lock()
	defer r.readerMx.Unlock()

	if r.reader == nil {
		return nil
	}

	if err := r.reader.Close(); err != nil {
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
	// Nothing to do if it's already opened.
	if r.reader != nil {
		return nil
	}

	// If we already tried to open it and it failed, we don't retry again
	// and we always return the same error.
	if r.readerErr != nil {
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
	startTime := time.Now()

	reader, err := NewBinaryReader(r.ctx, r.logger, r.bkt, r.dir, r.id, r.postingOffsetsInMemSampling)
	if err != nil {
		level.Error(r.logger).Log("msg", "failed to lazy load index-header file", "path", r.filepath, "err", err)
		r.readerErr = err
		return err
	}

	r.reader = reader
	level.Info(r.logger).Log("msg", "lazy loaded index-header file", "path", r.filepath, "elapsed", time.Since(startTime))

	return nil
}
