// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"golang.org/x/sync/errgroup"
)

// partitionSize is used for splitting range reads for index-header.
const partitionSize = 16 * 1024 * 1024 // 16 MiB

type parallelBucketReader struct {
	bkt           objstore.BucketReader
	tmpDir        string
	partitionSize int64
}

func WrapWithParallel(b objstore.BucketReader, tmpDir string) objstore.BucketReader {
	return &parallelBucketReader{
		bkt:           b,
		tmpDir:        tmpDir,
		partitionSize: partitionSize,
	}
}

// GetRange implements ParallelBucket.
func (b *parallelBucketReader) GetRange(ctx context.Context, name string, off int64, length int64) (io.ReadCloser, error) {
	// id
	partFilePrefix := uuid.New().String()
	g := errgroup.Group{}

	numParts := length / b.partitionSize
	if length%b.partitionSize > 0 {
		// A partial partition is remaining
		numParts += 1
	}

	parts := make([]Part, 0, numParts)

	i := 0
	for o := off; o < off+length; o += b.partitionSize {
		l := b.partitionSize
		if o+l > off+length {
			l = length - (int64(i) * b.partitionSize)
		}

		partOff := o
		partLength := l
		partId := i
		part, err := b.createPart(partFilePrefix, partId, int(partLength))
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)

		g.Go(func() error {
			rc, err := b.bkt.GetRange(ctx, name, partOff, partLength)
			defer runutil.CloseWithErrCapture(&err, rc, "close object")
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("getRangePartitioned %v", partId))
			}
			buf := make([]byte, 32*1024)
			if _, err := io.CopyBuffer(part, rc, buf); err != nil {
				return errors.Wrap(err, fmt.Sprintf("getRangePartitioned %v", partId))
			}
			part.Flush()
			return part.Sync()
		})
		i += 1
	}

	// Wait until all parts complete.
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return newPartMerger(parts)
}

func (b *parallelBucketReader) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	return b.bkt.Attributes(ctx, name)
}

func (b *parallelBucketReader) Exists(ctx context.Context, name string) (bool, error) {
	return b.bkt.Exists(ctx, name)
}

func (b *parallelBucketReader) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.bkt.Get(ctx, name)
}

func (b *parallelBucketReader) IsAccessDeniedErr(err error) bool {
	return b.bkt.IsAccessDeniedErr(err)
}

func (b *parallelBucketReader) IsObjNotFoundErr(err error) bool {
	return b.bkt.IsObjNotFoundErr(err)
}

func (b *parallelBucketReader) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	return b.bkt.Iter(ctx, dir, f, options...)
}

func (b *parallelBucketReader) createPart(partFilePrefix string, partId int, size int) (Part, error) {
	if b.tmpDir == "" {
		// We're buffering in memory.
		return newPartBuffer(size), nil
	}

	partName := fmt.Sprintf("%s.part-%d", partFilePrefix, partId)
	filename := filepath.Join(b.tmpDir, partName)
	return newPartFile(filename)
}

type partMerger struct {
	closers     []io.Closer
	multiReader io.Reader
}

func newPartMerger(rcs []Part) (*partMerger, error) {
	readers := make([]io.Reader, 0, len(rcs))
	closers := make([]io.Closer, 0, len(rcs))
	for _, rc := range rcs {
		if _, err := rc.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		readers = append(readers, rc.(io.Reader))
		closers = append(closers, rc.(io.Closer))
	}
	return &partMerger{
		closers:     closers,
		multiReader: io.MultiReader(readers...),
	}, nil
}

func (m *partMerger) Read(p []byte) (n int, err error) {
	n, err = m.multiReader.Read(p)
	return
}

func (m *partMerger) Close() (err error) {
	var firstErr error
	for _, c := range m.closers {
		if err := c.Close(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if firstErr != nil {
		return firstErr
	}
	return nil
}

type Part interface {
	Read(buf []byte) (int, error)
	Write(buf []byte) (int, error)
	Seek(offset int64, whence int) (int64, error)
	Flush() error
	Sync() error
	Close() error
}

func newPartFile(filename string) (*partFile, error) {
	dir := filepath.Dir(filename)
	df, err := fileutil.OpenDir(dir)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, err
		}
		df, err = fileutil.OpenDir(dir)
	}
	if err != nil {
		return nil, err
	}

	if err := df.Sync(); err != nil {
		return nil, errors.Wrap(err, "sync dir")
	}

	if err := os.RemoveAll(filename); err != nil {
		return nil, errors.Wrap(err, "remove existing file")
	}
	f, err := os.OpenFile(filepath.Clean(filename), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	return &partFile{
		file:       f,
		fileWriter: bufio.NewWriterSize(f, 32*1024),
		fileReader: bufio.NewReaderSize(f, 32*1024),
	}, nil
}

type partFile struct {
	file       *os.File
	fileWriter *bufio.Writer
	fileReader *bufio.Reader
}

func (p *partFile) Close() error {
	if err := p.file.Close(); err != nil {
		return err
	}
	return os.Remove(p.file.Name())
}

func (p *partFile) Flush() error {
	return p.fileWriter.Flush()
}

func (p *partFile) Read(buf []byte) (int, error) {
	return p.fileReader.Read(buf)
}

func (p *partFile) Seek(offset int64, whence int) (int64, error) {
	return p.file.Seek(offset, whence)
}

func (p *partFile) Sync() error {
	return p.file.Sync()
}

func (p *partFile) Write(buf []byte) (int, error) {
	return p.fileWriter.Write(buf)
}

func newPartBuffer(size int) *partBuffer {
	return &partBuffer{
		buf: bytes.NewBuffer(make([]byte, 0, size)),
	}
}

type partBuffer struct {
	buf *bytes.Buffer
}

func (p *partBuffer) Close() error {
	return nil
}

func (p *partBuffer) Flush() error {
	return nil
}

func (p *partBuffer) Read(b []byte) (int, error) {
	return p.buf.Read(b)
}

func (p *partBuffer) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (p *partBuffer) Sync() error {
	return nil
}

func (p *partBuffer) Write(b []byte) (int, error) {
	return p.buf.Write(b)
}
