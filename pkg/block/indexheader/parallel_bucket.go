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
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// partitionSize is used for splitting range reads.
const partitionSize = 16 * 1024 * 1024 // 16 MiB

type parallelBucketReader struct {
	objstore.BucketReader
	tmpDir        string
	partitionSize int64
}

func WrapWithParallel(b objstore.BucketReader, tmpDir string) objstore.BucketReader {
	return &parallelBucketReader{
		BucketReader:  b,
		tmpDir:        tmpDir,
		partitionSize: partitionSize,
	}
}

// GetRange reads the range in parallel.
func (b *parallelBucketReader) GetRange(ctx context.Context, name string, off int64, length int64) (io.ReadCloser, error) {
	partFilePrefix := uuid.New().String()
	g, gctx := errgroup.WithContext(ctx)

	numParts := length / b.partitionSize
	if length%b.partitionSize > 0 {
		// A partial partition is remaining
		numParts += 1
	}

	parts := make([]Part, 0, numParts)

	partId := 0
	for o := off; o < off+length; o += b.partitionSize {
		l := b.partitionSize
		if o+l > off+length {
			// Partial partition
			l = length - (int64(partId) * b.partitionSize)
		}

		partOff := o
		partLength := l
		part, err := b.createPart(partFilePrefix, partId, int(partLength))
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)

		// Assign partId to another variable to avoid modifying
		// `partId` concurrently in multiple goroutines.
		idx := partId
		g.Go(func() error {
			rc, err := b.BucketReader.GetRange(gctx, name, partOff, partLength)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("get range part %v", idx))
			}
			defer runutil.CloseWithErrCapture(&err, rc, "close object")
			if _, err := io.Copy(part, rc); err != nil {
				return errors.Wrap(err, fmt.Sprintf("get range part %v", idx))
			}
			return part.Flush()
		})
		partId += 1
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return newPartMerger(parts), nil
}

func (b *parallelBucketReader) createPart(partFilePrefix string, partId int, size int) (Part, error) {
	if b.tmpDir == "" {
		// Parts stored in memory
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

func newPartMerger(parts []Part) *partMerger {
	readers := make([]io.Reader, 0, len(parts))
	closers := make([]io.Closer, 0, len(parts))
	for _, p := range parts {
		readers = append(readers, p.(io.Reader))
		closers = append(closers, p.(io.Closer))
	}
	return &partMerger{
		closers:     closers,
		multiReader: io.MultiReader(readers...),
	}
}

func (m *partMerger) Read(b []byte) (n int, err error) {
	n, err = m.multiReader.Read(b)
	return
}

func (m *partMerger) Close() (err error) {
	var firstErr error = nil
	for _, c := range m.closers {
		if err := c.Close(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

type Part interface {
	Read(buf []byte) (int, error)
	Write(buf []byte) (int, error)
	Flush() error
}

// partFile stores parts in temporary files.
type partFile struct {
	file       *os.File
	fileWriter *bufio.Writer
	fileReader *bufio.Reader
}

func newPartFile(filename string) (*partFile, error) {
	dir := filepath.Dir(filename)
	df, err := fileutil.OpenDir(dir)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, errors.Wrap(err, "create temp dir")
		}
		df, err = fileutil.OpenDir(dir)
	}
	if err != nil {
		return nil, errors.Wrap(err, "open temp dir")
	}

	if err := df.Sync(); err != nil {
		return nil, errors.Wrap(err, "sync dir")
	}

	if err := os.RemoveAll(filename); err != nil {
		return nil, errors.Wrap(err, "remove existing file")
	}
	f, err := os.OpenFile(filepath.Clean(filename), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.Wrap(err, "open temp file")
	}
	return &partFile{
		file:       f,
		fileWriter: bufio.NewWriterSize(f, 32*1024),
		fileReader: bufio.NewReaderSize(f, 32*1024),
	}, nil
}

func (p *partFile) Close() error {
	if err := p.file.Close(); err != nil {
		return err
	}
	return os.Remove(p.file.Name())
}

func (p *partFile) Flush() error {
	if err := p.fileWriter.Flush(); err != nil {
		return err
	}
	if err := p.file.Sync(); err != nil {
		return err
	}
	// Seek is necessary because the part was just written to.
	_, err := p.file.Seek(0, io.SeekStart)
	return err
}

func (p *partFile) Read(buf []byte) (int, error) {
	return p.fileReader.Read(buf)
}

func (p *partFile) Write(buf []byte) (int, error) {
	return p.fileWriter.Write(buf)
}

// partBuffer stores parts in memory.
type partBuffer struct {
	buf *bytes.Buffer
}

func newPartBuffer(size int) *partBuffer {
	return &partBuffer{
		buf: bytes.NewBuffer(make([]byte, 0, size)),
	}
}

func (p *partBuffer) Close() error {
	return nil
}

func (p *partBuffer) Read(b []byte) (int, error) {
	return p.buf.Read(b)
}

func (p *partBuffer) Write(b []byte) (int, error) {
	return p.buf.Write(b)
}

func (p *partBuffer) Flush() error {
	return nil
}
