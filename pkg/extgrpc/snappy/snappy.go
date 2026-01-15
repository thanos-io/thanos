// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package snappy

import (
	"io"
	"sync"

	"github.com/klauspost/compress/snappy"
	"google.golang.org/grpc/encoding"
)

// Name is the name registered for the snappy compressor.
const Name = "snappy"

var Compressor *compressor = newCompressor()

func init() {
	encoding.RegisterCompressor(Compressor)
}

type compressor struct {
	writersPool sync.Pool
	readersPool sync.Pool
}

func newCompressor() *compressor {
	c := &compressor{}
	c.readersPool = sync.Pool{
		New: func() any {
			return snappy.NewReader(nil)
		},
	}
	c.writersPool = sync.Pool{
		New: func() any {
			return snappy.NewBufferedWriter(nil)
		},
	}
	return c
}

func (c *compressor) Name() string {
	return Name
}

func (c *compressor) Compress(w io.Writer) (io.WriteCloser, error) {
	wr := c.writersPool.Get().(*snappy.Writer)
	wr.Reset(w)
	return writeCloser{wr, &c.writersPool}, nil
}

var decompressBuf = sync.Pool{}

func (c *compressor) Decompress(r io.Reader) (io.Reader, error) {
	var readBuf []byte

	dbuf := decompressBuf.Get()
	if dbuf == nil {
		buf := make([]byte, 512)
		readBuf = buf
	} else {
		readBuf = *dbuf.(*[]byte)
	}

	return &reader{r, &c.readersPool, readBuf}, nil
}

var _ io.WriterTo = (*reader)(nil)

func (r *reader) Read(p []byte) (n int, err error) {
	panic("not implemented, use WriteTo instead")
}

func (r *reader) WriteTo(w io.Writer) (int64, error) {
	defer decompressBuf.Put(&r.readBuf)

	snappyReader := r.snappyReaderPool.Get().(*snappy.Reader)
	snappyReader.Reset(r.reader)
	defer r.snappyReaderPool.Put(snappyReader)

	return io.CopyBuffer(w, snappyReader, r.readBuf)
}

type writeCloser struct {
	writer *snappy.Writer
	pool   *sync.Pool
}

func (w writeCloser) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}

func (w writeCloser) Close() error {
	defer func() {
		w.writer.Reset(nil)
		w.pool.Put(w.writer)
	}()

	if w.writer != nil {
		return w.writer.Close()
	}
	return nil
}

type reader struct {
	reader           io.Reader
	snappyReaderPool *sync.Pool

	readBuf []byte
}
