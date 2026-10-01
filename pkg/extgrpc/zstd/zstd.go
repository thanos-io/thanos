// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package zstd

import (
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/encoding"
)

// Name is the name registered for the zstd compressor.
const Name = "zstd"

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
			zr, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
			if err != nil {
				panic(err)
			}
			return zr
		},
	}
	c.writersPool = sync.Pool{
		New: func() any {
			zw, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
			if err != nil {
				panic(err)
			}
			return zw
		},
	}
	return c
}

func (c *compressor) Name() string {
	return Name
}

func (c *compressor) Compress(w io.Writer) (io.WriteCloser, error) {
	wr := c.writersPool.Get().(*zstd.Encoder)
	wr.Reset(w)
	return writeCloser{wr, &c.writersPool}, nil
}

func (c *compressor) Decompress(r io.Reader) (io.Reader, error) {
	dr := c.readersPool.Get().(*zstd.Decoder)
	if err := dr.Reset(r); err != nil {
		c.readersPool.Put(dr)
		return nil, err
	}
	return reader{dr, &c.readersPool}, nil
}

type writeCloser struct {
	writer *zstd.Encoder
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
	reader *zstd.Decoder
	pool   *sync.Pool
}

func (r reader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if err == io.EOF {
		_ = r.reader.Reset(nil)
		r.pool.Put(r.reader)
	}
	return n, err
}
