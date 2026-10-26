// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"sync"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/packed"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
)

// Wire format. The client picks the format, the server detects it from the
// first bytes, so one port serves both kinds of peers:
//
//	none: <packed capnp message> <packed capnp message> ...
//	zstd: "TZS\x01" <frame> <frame> ...
//	      frame := <uvarint payload length> <zstd frame of one standard capnp message>
//
// Each frame is compressed independently. A packed stream cannot start with the
// preface: decoded as a segment table it means far more segments than allowed.

// Compression selects how a client encodes replicated messages.
type Compression string

const (
	CompressionNone Compression = "none"
	CompressionZstd Compression = "zstd"

	// DefaultCompressionConcurrency is the default number of zstd encoder
	// states shared by all connections of a process.
	DefaultCompressionConcurrency = 4
)

var zstdPreface = [4]byte{'T', 'Z', 'S', 1}

const (
	// maxMessageSize bounds a message both compressed and decompressed. It equals
	// the default capnp decoder limit used by the packed stream.
	maxMessageSize = 64 << 20
	// Scratch buffers above this size are dropped after use, so one oversized
	// message does not pin memory for the life of the connection.
	maxRetainedBufferSize = 4 << 20
	zstdWindowSize        = 1 << 20
)

var zstdDecoder = func() *zstd.Decoder {
	d, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxMessageSize))
	if err != nil {
		panic(err)
	}
	return d
}()

// zstdEncoders holds one shared encoder per concurrency value.
var zstdEncoders struct {
	sync.Mutex
	byConcurrency map[int]*zstd.Encoder
}

// zstdEncoderFor returns the shared encoder for the given concurrency; 0 means GOMAXPROCS.
func zstdEncoderFor(concurrency int) (*zstd.Encoder, error) {
	if concurrency < 0 {
		return nil, errors.Errorf("zstd encoder concurrency must not be negative, got %d", concurrency)
	}
	if concurrency == 0 {
		concurrency = runtime.GOMAXPROCS(0)
	}

	zstdEncoders.Lock()
	defer zstdEncoders.Unlock()
	if e, ok := zstdEncoders.byConcurrency[concurrency]; ok {
		return e, nil
	}
	e, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(concurrency),
		zstd.WithWindowSize(zstdWindowSize),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create zstd encoder")
	}
	if zstdEncoders.byConcurrency == nil {
		zstdEncoders.byConcurrency = map[int]*zstd.Encoder{}
	}
	zstdEncoders.byConcurrency[concurrency] = e
	return e, nil
}

// NewClientTransport wraps a dialed connection in the transport matching c and
// writes the preface for CompressionZstd. encoderConcurrency 0 means GOMAXPROCS.
func NewClientTransport(conn net.Conn, c Compression, encoderConcurrency int) (rpc.Transport, error) {
	if c != CompressionZstd {
		return rpc.NewPackedStreamTransport(conn), nil
	}
	enc, err := zstdEncoderFor(encoderConcurrency)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(zstdPreface[:]); err != nil {
		return nil, errors.Wrap(err, "write compression preface")
	}
	return rpc.NewTransport(newZstdCodec(bufio.NewReader(conn), conn, enc)), nil
}

// NewServerTransport returns the transport matching what the client sends. It
// blocks until the client has sent at least len(zstdPreface) bytes.
func NewServerTransport(conn net.Conn, encoderConcurrency int) (rpc.Transport, error) {
	br := bufio.NewReader(conn)
	head, err := br.Peek(len(zstdPreface))
	if err != nil {
		return nil, errors.Wrap(err, "read connection preface")
	}
	if !bytes.Equal(head, zstdPreface[:]) {
		return rpc.NewTransport(packedCodec{
			Decoder: capnp.NewDecoder(packed.NewReader(br)),
			Encoder: capnp.NewPackedEncoder(conn),
			Closer:  conn,
		}), nil
	}
	enc, err := zstdEncoderFor(encoderConcurrency)
	if err != nil {
		return nil, err
	}
	if _, err := br.Discard(len(zstdPreface)); err != nil {
		return nil, errors.Wrap(err, "discard connection preface")
	}
	return rpc.NewTransport(newZstdCodec(br, conn, enc)), nil
}

// packedCodec is the stock packed codec over a reader that already holds the peeked bytes.
type packedCodec struct {
	*capnp.Decoder
	*capnp.Encoder
	io.Closer
}

// zstdCodec is an rpc.Codec that sends each capnp message as one zstd frame.
// The rpc layer calls Encode from one goroutine and Decode from another, so the
// two directions keep separate state; encMu only guards against misuse.
type zstdCodec struct {
	r   *bufio.Reader
	c   io.WriteCloser
	enc *zstd.Encoder

	encMu  sync.Mutex
	raw    bytes.Buffer
	capEnc *capnp.Encoder
	out    []byte
	lenBuf [binary.MaxVarintLen64]byte

	in      []byte
	plain   []byte
	plainRd bytes.Reader
	capDec  *capnp.Decoder
}

func newZstdCodec(r *bufio.Reader, c io.WriteCloser, enc *zstd.Encoder) *zstdCodec {
	z := &zstdCodec{r: r, c: c, enc: enc}
	z.capEnc = capnp.NewEncoder(&z.raw)
	z.capDec = capnp.NewDecoder(&z.plainRd)
	z.capDec.MaxMessageSize = maxMessageSize
	return z
}

func (z *zstdCodec) Encode(m *capnp.Message) error {
	z.encMu.Lock()
	defer z.encMu.Unlock()
	defer z.releaseEncodeBuffers()

	z.raw.Reset()
	if err := z.capEnc.Encode(m); err != nil {
		return err
	}
	z.out = z.enc.EncodeAll(z.raw.Bytes(), z.out[:0])
	n := binary.PutUvarint(z.lenBuf[:], uint64(len(z.out)))
	bufs := net.Buffers{z.lenBuf[:n], z.out}
	_, err := bufs.WriteTo(z.c)
	return err
}

func (z *zstdCodec) Decode() (*capnp.Message, error) {
	defer z.releaseDecodeBuffers()

	size, err := binary.ReadUvarint(z.r)
	if err != nil {
		return nil, err
	}
	if size > maxMessageSize {
		return nil, errors.Errorf("compressed message of %d bytes exceeds %d", size, maxMessageSize)
	}
	if uint64(cap(z.in)) < size {
		z.in = make([]byte, size)
	}
	z.in = z.in[:size]
	if _, err := io.ReadFull(z.r, z.in); err != nil {
		return nil, err
	}
	if z.plain, err = zstdDecoder.DecodeAll(z.in, z.plain[:0]); err != nil {
		return nil, errors.Wrap(err, "decompress message")
	}
	z.plainRd.Reset(z.plain)
	// The capnp decoder copies the message out of plainRd, so plain may be
	// reused or dropped as soon as Decode returns.
	return z.capDec.Decode()
}

func (z *zstdCodec) Close() error { return z.c.Close() }

func (z *zstdCodec) releaseEncodeBuffers() {
	if cap(z.out) > maxRetainedBufferSize {
		z.out = nil
	}
	if z.raw.Cap() > maxRetainedBufferSize {
		// capEnc writes through &z.raw, so replacing the value keeps it valid.
		z.raw = bytes.Buffer{}
	}
}

func (z *zstdCodec) releaseDecodeBuffers() {
	if cap(z.in) > maxRetainedBufferSize {
		z.in = nil
	}
	if cap(z.plain) > maxRetainedBufferSize {
		z.plain = nil
		z.plainRd.Reset(nil)
	}
}
