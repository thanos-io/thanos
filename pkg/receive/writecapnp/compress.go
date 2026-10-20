// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
)

type Compression string

const (
	CompressionNone Compression = "none"
	CompressionZstd Compression = "zstd"
)

var zstdPreface = [4]byte{'T', 'Z', 'S', 1}

const maxFrameSize = 64 << 20

var (
	zstdEncoder = sync.OnceValue(func() *zstd.Encoder {
		e, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(4),
			zstd.WithWindowSize(1<<20),
		)
		if err != nil {
			panic(err)
		}
		return e
	})
	zstdDecoder = sync.OnceValue(func() *zstd.Decoder {
		d, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxFrameSize))
		if err != nil {
			panic(err)
		}
		return d
	})
)

func NewClientTransport(conn net.Conn, c Compression) (rpc.Transport, error) {
	if c != CompressionZstd {
		return rpc.NewPackedStreamTransport(conn), nil
	}
	if _, err := conn.Write(zstdPreface[:]); err != nil {
		return nil, errors.Wrap(err, "write compression preface")
	}
	return rpc.NewTransport(newZstdCodec(bufio.NewReader(conn), conn)), nil
}

func NewServerTransport(conn net.Conn) (rpc.Transport, error) {
	br := bufio.NewReader(conn)
	head, err := br.Peek(len(zstdPreface))
	if err != nil {
		return nil, errors.Wrap(err, "read connection preface")
	}
	if bytes.Equal(head, zstdPreface[:]) {
		_, _ = br.Discard(len(zstdPreface))
		return rpc.NewTransport(newZstdCodec(br, conn)), nil
	}
	return rpc.NewPackedStreamTransport(&bufferedConn{Reader: br, Conn: conn}), nil
}

type bufferedConn struct {
	*bufio.Reader
	net.Conn
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.Reader.Read(p) }

type zstdCodec struct {
	r *bufio.Reader
	c io.WriteCloser

	encMu   sync.Mutex
	raw     bytes.Buffer
	enc     *capnp.Encoder
	out     []byte
	lenBuf  [binary.MaxVarintLen64]byte
	in      []byte
	plain   []byte
	plainRd bytes.Reader
	dec     *capnp.Decoder
}

func newZstdCodec(r *bufio.Reader, c io.WriteCloser) *zstdCodec {
	z := &zstdCodec{r: r, c: c}
	z.enc = capnp.NewEncoder(&z.raw)
	z.dec = capnp.NewDecoder(&z.plainRd)
	z.dec.MaxMessageSize = maxFrameSize
	return z
}

func (z *zstdCodec) Encode(m *capnp.Message) error {
	z.encMu.Lock()
	defer z.encMu.Unlock()

	z.raw.Reset()
	if err := z.enc.Encode(m); err != nil {
		return err
	}
	z.out = zstdEncoder().EncodeAll(z.raw.Bytes(), z.out[:0])
	n := binary.PutUvarint(z.lenBuf[:], uint64(len(z.out)))
	bufs := net.Buffers{z.lenBuf[:n], z.out}
	_, err := bufs.WriteTo(z.c)
	return err
}

func (z *zstdCodec) Decode() (*capnp.Message, error) {
	size, err := binary.ReadUvarint(z.r)
	if err != nil {
		return nil, err
	}
	if size > maxFrameSize {
		return nil, errors.Errorf("compressed message of %d bytes exceeds %d", size, maxFrameSize)
	}
	if uint64(cap(z.in)) < size {
		z.in = make([]byte, size)
	}
	z.in = z.in[:size]
	if _, err := io.ReadFull(z.r, z.in); err != nil {
		return nil, err
	}
	if z.plain, err = zstdDecoder().DecodeAll(z.in, z.plain[:0]); err != nil {
		return nil, errors.Wrap(err, "decompress message")
	}
	z.plainRd.Reset(z.plain)
	return z.dec.Decode()
}

func (z *zstdCodec) Close() error { return z.c.Close() }
