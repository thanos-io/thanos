// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"slices"
	"sync"
	"time"

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
//	zstd: "TZS\x02" <frame> <frame> ...
//	      frame := <uvarint payload length> <zstd frame of one standard capnp message>
//
// Only the client to server direction is compressed. Replies are small RPC
// returns that zstd cannot shrink, so the server always answers with a packed
// stream and never needs an encoder. Version 1 of the preface compressed both
// directions and is not accepted.
//
// Each frame is compressed independently. A packed stream cannot start with
// "TZS": decoded as a segment table any 4-byte "TZS?" prefix means far more
// segments than allowed.

// Compression selects how a client encodes replicated messages.
type Compression string

const (
	CompressionNone Compression = "none"
	CompressionZstd Compression = "zstd"

	// DefaultCompressionConcurrency is the default number of zstd encoder
	// states shared by all connections of a process.
	DefaultCompressionConcurrency = 4

	// prefaceTimeout bounds how long the server waits for the first bytes of a
	// connection, so stalled or half-open connections do not pin a goroutine.
	prefaceTimeout = 10 * time.Second
)

var zstdPreface = [4]byte{'T', 'Z', 'S', 2}

// errMessageTooLarge is returned when a message is too large for the peer to
// accept. Sending it again cannot succeed, so it is not a peer failure.
var errMessageTooLarge = errors.New("message exceeds the maximum size")

const (
	// maxMessageSize bounds a message both compressed and decompressed. It equals
	// the default capnp decoder limit used by the packed stream.
	maxMessageSize = 64 << 20
	// Scratch buffers above this size are dropped after use, so one oversized
	// message does not pin memory for the life of the connection.
	maxRetainedBufferSize = 4 << 20
	zstdWindowSize        = 1 << 20
	// minReadChunk is the first read size for a compressed message; later reads
	// double it up to the declared size.
	minReadChunk = 64 << 10
)

// zstdDecoder is shared by all connections and created on first use, so
// processes that never receive compressed traffic do not pay for it. Its
// concurrency is GOMAXPROCS, not the library default of 4, so decompression
// does not cap ingestion throughput.
var zstdDecoder = sync.OnceValues(func() (*zstd.Decoder, error) {
	d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(maxMessageSize))
	if err != nil {
		return nil, errors.Wrap(err, "create zstd decoder")
	}
	return d, nil
})

// zstdEncoders holds one shared encoder per concurrency value.
var zstdEncoders struct {
	sync.Mutex
	byConcurrency map[int]*zstd.Encoder
}

// zstdEncoderFor returns the shared encoder for the given concurrency; 0 means
// DefaultCompressionConcurrency.
func zstdEncoderFor(concurrency int) (*zstd.Encoder, error) {
	if concurrency < 0 {
		return nil, errors.Errorf("zstd encoder concurrency must not be negative, got %d", concurrency)
	}
	if concurrency == 0 {
		concurrency = DefaultCompressionConcurrency
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
// writes the preface for CompressionZstd. encoderConcurrency 0 means
// DefaultCompressionConcurrency.
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
	return rpc.NewTransport(codec{
		messageEncoder: newZstdFrameEncoder(conn, enc),
		messageDecoder: capnp.NewPackedDecoder(conn),
		Closer:         conn,
	}), nil
}

// NewServerTransport returns the transport matching what the client sends. It
// waits up to prefaceTimeout for the client to send len(zstdPreface) bytes.
func NewServerTransport(conn net.Conn) (rpc.Transport, error) {
	br := bufio.NewReader(conn)
	if err := conn.SetReadDeadline(time.Now().Add(prefaceTimeout)); err != nil {
		return nil, errors.Wrap(err, "set preface read deadline")
	}
	head, err := br.Peek(len(zstdPreface))
	if err != nil {
		return nil, errors.Wrap(err, "read connection preface")
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return nil, errors.Wrap(err, "clear preface read deadline")
	}
	if !bytes.HasPrefix(head, zstdPreface[:3]) {
		return rpc.NewTransport(codec{
			messageDecoder: capnp.NewDecoder(packed.NewReader(br)),
			messageEncoder: capnp.NewPackedEncoder(conn),
			Closer:         conn,
		}), nil
	}
	if head[3] != zstdPreface[3] {
		return nil, errors.Errorf("unsupported compression preface version %d", head[3])
	}
	if _, err := br.Discard(len(zstdPreface)); err != nil {
		return nil, errors.Wrap(err, "discard connection preface")
	}
	return rpc.NewTransport(codec{
		messageDecoder: newZstdFrameDecoder(br),
		messageEncoder: capnp.NewPackedEncoder(conn),
		Closer:         conn,
	}), nil
}

type messageEncoder interface {
	Encode(*capnp.Message) error
}

type messageDecoder interface {
	Decode() (*capnp.Message, error)
}

// codec is an rpc.Codec assembled from independent directions.
type codec struct {
	messageEncoder
	messageDecoder
	io.Closer
}

// zstdFrameEncoder sends each capnp message as one zstd frame. The rpc layer
// calls Encode from a single goroutine; mu only guards against misuse.
type zstdFrameEncoder struct {
	w   io.Writer
	enc *zstd.Encoder

	mu     sync.Mutex
	raw    bytes.Buffer
	capEnc *capnp.Encoder
	out    []byte
	lenBuf [binary.MaxVarintLen64]byte
}

func newZstdFrameEncoder(w io.Writer, enc *zstd.Encoder) *zstdFrameEncoder {
	z := &zstdFrameEncoder{w: w, enc: enc}
	z.capEnc = capnp.NewEncoder(&z.raw)
	return z
}

func (z *zstdFrameEncoder) Encode(m *capnp.Message) error {
	z.mu.Lock()
	defer z.mu.Unlock()
	defer z.releaseBuffers()

	rawSize, err := m.TotalSize()
	if err != nil {
		return err
	}
	if rawSize > maxMessageSize {
		return errors.Wrapf(errMessageTooLarge, "%d bytes, limit %d", rawSize, maxMessageSize)
	}

	z.raw.Reset()
	z.raw.Grow(int(rawSize))
	if err := z.capEnc.Encode(m); err != nil {
		return err
	}
	z.out = z.enc.EncodeAll(z.raw.Bytes(), z.out[:0])
	if len(z.out) > maxMessageSize {
		return errors.Wrapf(errMessageTooLarge, "compressed to %d bytes, limit %d", len(z.out), maxMessageSize)
	}
	n := binary.PutUvarint(z.lenBuf[:], uint64(len(z.out)))
	bufs := net.Buffers{z.lenBuf[:n], z.out}
	_, err = bufs.WriteTo(z.w)
	return err
}

func (z *zstdFrameEncoder) releaseBuffers() {
	if cap(z.out) > maxRetainedBufferSize {
		z.out = nil
	}
	if z.raw.Cap() > maxRetainedBufferSize {
		// capEnc writes through &z.raw, so replacing the value keeps it valid.
		z.raw = bytes.Buffer{}
	}
}

// zstdFrameDecoder reads messages written by zstdFrameEncoder.
type zstdFrameDecoder struct {
	r *bufio.Reader

	in      []byte
	plain   []byte
	plainRd bytes.Reader
	capDec  *capnp.Decoder
}

func newZstdFrameDecoder(r *bufio.Reader) *zstdFrameDecoder {
	z := &zstdFrameDecoder{r: r}
	z.capDec = capnp.NewDecoder(&z.plainRd)
	z.capDec.MaxMessageSize = maxMessageSize
	return z
}

func (z *zstdFrameDecoder) Decode() (*capnp.Message, error) {
	defer z.releaseBuffers()

	dec, err := zstdDecoder()
	if err != nil {
		return nil, err
	}
	size, err := binary.ReadUvarint(z.r)
	if err != nil {
		return nil, err
	}
	if size > maxMessageSize {
		return nil, errors.Errorf("compressed message of %d bytes exceeds %d", size, maxMessageSize)
	}
	// Grow the buffer as bytes arrive, so a peer has to send what it declares.
	// DecodeAll and the capnp decoder still size their output from headers, up
	// to maxMessageSize, like the packed stream does.
	z.in = z.in[:0]
	for uint64(len(z.in)) < size {
		n := len(z.in)
		chunk := int(min(size-uint64(n), uint64(max(n, minReadChunk))))
		z.in = slices.Grow(z.in, chunk)[:n+chunk]
		if _, err := io.ReadFull(z.r, z.in[n:]); err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
	if z.plain, err = dec.DecodeAll(z.in, z.plain[:0]); err != nil {
		return nil, errors.Wrap(err, "decompress message")
	}
	z.plainRd.Reset(z.plain)
	// The capnp decoder copies the message out of plainRd, so plain may be
	// reused or dropped as soon as Decode returns.
	msg, err := z.capDec.Decode()
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if z.plainRd.Len() != 0 {
		msg.Release()
		return nil, errors.Errorf("trailing %d bytes in compressed frame", z.plainRd.Len())
	}
	return msg, nil
}

func (z *zstdFrameDecoder) releaseBuffers() {
	if cap(z.in) > maxRetainedBufferSize {
		z.in = nil
	}
	if cap(z.plain) > maxRetainedBufferSize {
		z.plain = nil
		z.plainRd.Reset(nil)
	}
}
