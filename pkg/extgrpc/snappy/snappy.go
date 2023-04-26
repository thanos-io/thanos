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
		New: func() interface{} {
			return snappy.NewReader(nil)
		},
	}
	c.writersPool = sync.Pool{
		New: func() interface{} {
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

func (c *compressor) DecompressByteReader(r io.Reader) (io.ByteReader, error) {
	dr := c.readersPool.Get().(*snappy.Reader)
	dr.Reset(r)
	return reader{dr, &c.readersPool}, nil
}

func (c *compressor) Decompress(r io.Reader) (io.Reader, error) {
	dr := c.readersPool.Get().(*snappy.Reader)
	dr.Reset(r)
	return reader{dr, &c.readersPool}, nil
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
	reader *snappy.Reader
	pool   *sync.Pool
}

func (r reader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if err == io.EOF {
		r.reader.Reset(nil)
		r.pool.Put(r.reader)
	}
	return n, err
}

func (r reader) ReadByte() (n byte, err error) {
	n, err = r.reader.ReadByte()
	if err == io.EOF {
		r.reader.Reset(nil)
		r.pool.Put(r.reader)
	}
	return n, err

}
