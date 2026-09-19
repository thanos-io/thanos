// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/s2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"

	extsnappy "github.com/thanos-io/thanos/pkg/extgrpc/snappy"
	"github.com/thanos-io/thanos/pkg/pool"
)

// This file implements encoding and decoding of postings using diff (or delta) + varint
// number encoding. On top of that, we apply Snappy compression.
//
// On its own, Snappy compressing raw postings doesn't really help, because there is no
// repetition in raw data. Using diff (delta) between postings entries makes values small,
// and Varint is very efficient at encoding small values (values < 128 are encoded as
// single byte, values < 16384 are encoded as two bytes). Diff + varint reduces postings size
// significantly (to about 20% of original), snappy then halves it to ~10% of the original.

const (
	codecHeaderSnappy         = "dvs" // As in "diff+varint+snappy".
	codecHeaderStreamedSnappy = "dss" // As in "diffvarint+streamed snappy".
)

func decodePostings(input []byte) (closeablePostings, error) {
	var df func([]byte, bool) (closeablePostings, error)

	switch {
	case isDiffVarintSnappyEncodedPostings(input):
		df = diffVarintSnappyDecode
	case isDiffVarintSnappyStreamedEncodedPostings(input):
		df = diffVarintSnappyStreamedDecode
	default:
		return nil, fmt.Errorf("unrecognize postings format")
	}

	return df(input, false)
}

// isDiffVarintSnappyEncodedPostings returns true, if input looks like it has been encoded by diff+varint+snappy codec.
func isDiffVarintSnappyEncodedPostings(input []byte) bool {
	return bytes.HasPrefix(input, []byte(codecHeaderSnappy))
}

// isDiffVarintSnappyStreamedEncodedPostings returns true, if input looks like it has been encoded by diff+varint+snappy streamed codec.
func isDiffVarintSnappyStreamedEncodedPostings(input []byte) bool {
	return bytes.HasPrefix(input, []byte(codecHeaderStreamedSnappy))
}

// estimateSnappyStreamSize estimates the number of bytes
// needed for encoding length postings. Note that in reality
// the number of bytes needed could be much bigger if postings
// different by a lot. Practically, stddev=64 is used.
func estimateSnappyStreamSize(length int) int {
	// Snappy stream writes data in chunks up to 65536 in size.
	// The stream begins with bytes 0xff 0x06 0x00 0x00 's' 'N' 'a' 'P' 'p' 'Y'.
	// Our encoded data also needs a header.
	// Each encoded (or uncompressed) chunk needs tag (chunk type 1B + chunk len 3B) + checksum 4B.

	// Mark for encoded data.
	ret := len(codecHeaderStreamedSnappy)
	// Magic snappy stream start.
	ret += 10

	const maxBlockSize = 65536

	length = 5 * length / 4 // estimate 1.25B per posting.

	blocks := length / maxBlockSize

	ret += blocks * snappy.MaxEncodedLen(maxBlockSize)
	length -= blocks * maxBlockSize
	if length > 0 {
		ret += snappy.MaxEncodedLen(length)
	}

	return ret
}

func diffVarintSnappyStreamedEncode(p index.Postings, length int) ([]byte, error) {
	compressedBuf := bytes.NewBuffer(make([]byte, 0, estimateSnappyStreamSize(length)))
	if n, err := compressedBuf.WriteString(codecHeaderStreamedSnappy); err != nil {
		return nil, fmt.Errorf("writing streamed snappy header")
	} else if n != len(codecHeaderStreamedSnappy) {
		return nil, fmt.Errorf("short-write streamed snappy header")
	}

	uvarintEncodeBuf := make([]byte, binary.MaxVarintLen64)

	sw, err := extsnappy.Compressor.Compress(compressedBuf)
	if err != nil {
		return nil, fmt.Errorf("creating snappy compressor: %w", err)
	}

	prev := storage.SeriesRef(0)
	for p.Next() {
		v := p.At()
		if v < prev {
			return nil, errors.Errorf("postings entries must be in increasing order, current: %d, previous: %d", v, prev)
		}

		uvarintSize := binary.PutUvarint(uvarintEncodeBuf, uint64(v-prev))
		if written, err := sw.Write(uvarintEncodeBuf[:uvarintSize]); err != nil {
			return nil, errors.Wrap(err, "writing uvarint encoded byte")
		} else if written != uvarintSize {
			return nil, errors.Wrap(err, "short-write for uvarint encoded byte")
		}

		prev = v
	}
	if p.Err() != nil {
		return nil, p.Err()
	}
	if err := sw.Close(); err != nil {
		return nil, errors.Wrap(err, "closing snappy stream writer")
	}

	return compressedBuf.Bytes(), nil
}

func diffVarintSnappyStreamedDecode(input []byte, disablePooling bool) (closeablePostings, error) {
	if !isDiffVarintSnappyStreamedEncodedPostings(input) {
		return nil, errors.New("header not found")
	}

	return newStreamedDiffVarintPostings(input[len(codecHeaderStreamedSnappy):], disablePooling)
}

type streamedDiffVarintPostings struct {
	curSeries storage.SeriesRef

	err               error
	input, buf        []byte
	maximumDecodedLen int

	db *encoding.Decbuf

	readSnappyIdentifier bool
	disablePooling       bool
}

const (
	chunkTypeCompressedData   = 0x00
	chunkTypeUncompressedData = 0x01
	chunkTypeStreamIdentifier = 0xff
	chunkTypePadding          = 0xfe
	checksumSize              = 4
)

func maximumDecodedLenSnappyStreamed(in []byte) (int, error) {
	maxDecodedLen := -1

	for len(in) > 0 {
		// Chunk type.
		chunkType := in[0]
		in = in[1:]
		chunkLen := int(in[0]) | int(in[1])<<8 | int(in[2])<<16
		in = in[3:]

		switch chunkType {
		case chunkTypeCompressedData:
			bl := in[:chunkLen]
			// NOTE: checksum will be checked later on.
			decodedLen, err := s2.DecodedLen(bl[checksumSize:])
			if err != nil {
				return 0, err
			}
			if decodedLen > maxDecodedLen {
				maxDecodedLen = decodedLen
			}
		case chunkTypeUncompressedData:
			// NOTE: checksum will be checked later on.
			n := chunkLen - checksumSize
			if n > maxDecodedLen {
				maxDecodedLen = n
			}
		}
		in = in[chunkLen:]
	}
	return maxDecodedLen, nil
}

var decodedBufPool = pool.MustNewBucketedPool[byte](1024, 65536, 2, 0)

func newStreamedDiffVarintPostings(input []byte, disablePooling bool) (closeablePostings, error) {
	// We can't use the regular s2.Reader because it assumes a stream.
	// We already everything in memory so let's avoid copying.
	// Algorithm:
	// 1. Step through all chunks all get maximum decoded len.
	// 2. Read into decoded step by step. For decoding call s2.Decode(r.decoded, buf).
	maximumDecodedLen, err := maximumDecodedLenSnappyStreamed(input)
	if err != nil {
		return nil, err
	}

	return &streamedDiffVarintPostings{
		input:             input,
		maximumDecodedLen: maximumDecodedLen,
		db:                &encoding.Decbuf{},
		disablePooling:    disablePooling,
	}, nil
}

func (it *streamedDiffVarintPostings) close() {
	if it.buf == nil {
		return
	}
	if it.disablePooling {
		return
	}
	decodedBufPool.Put(&it.buf)
}

func (it *streamedDiffVarintPostings) At() storage.SeriesRef {
	return it.curSeries
}

func (it *streamedDiffVarintPostings) readNextChunk(remainder []byte) bool {
	// Normal EOF.
	if len(it.input) == 0 {
		return false
	}

	// Read next chunk into it.db.B.
	chunkType := it.input[0]
	it.input = it.input[1:]

	if len(it.input) < 3 {
		it.err = io.ErrUnexpectedEOF
		return false
	}

	chunkLen := int(it.input[0]) | int(it.input[1])<<8 | int(it.input[2])<<16
	it.input = it.input[3:]

	switch chunkType {
	case chunkTypeStreamIdentifier:
		const magicBody = "sNaPpY"
		if chunkLen != len(magicBody) {
			it.err = fmt.Errorf("corrupted identifier")
			return false
		}
		if string(it.input[:len(magicBody)]) != magicBody {
			it.err = fmt.Errorf("got bad identifier %s", string(it.input[:6]))
			return false
		}
		it.input = it.input[6:]
		it.readSnappyIdentifier = true
		return it.readNextChunk(nil)
	case chunkTypeCompressedData:
		if !it.readSnappyIdentifier {
			it.err = fmt.Errorf("missing magic snappy marker")
			return false
		}
		if len(it.input) < 4 {
			it.err = io.ErrUnexpectedEOF
			return false
		}
		checksum := uint32(it.input[0]) | uint32(it.input[1])<<8 | uint32(it.input[2])<<16 | uint32(it.input[3])<<24
		if len(it.input) < chunkLen {
			it.err = io.ErrUnexpectedEOF
			return false
		}

		if it.buf == nil {
			if it.disablePooling {
				it.buf = make([]byte, it.maximumDecodedLen)
			} else {
				b, err := decodedBufPool.Get(it.maximumDecodedLen)
				if err != nil {
					it.err = err
					return false
				}
				it.buf = *b
			}
		}

		encodedBuf := it.input[:chunkLen]

		// NOTE(GiedriusS): we can probably optimize this better but this should be rare enough
		// and not cause any problems.
		if len(remainder) > 0 {
			remainderCopy := make([]byte, 0, len(remainder))
			remainderCopy = append(remainderCopy, remainder...)
			remainder = remainderCopy
		}
		decoded, err := s2.Decode(it.buf, encodedBuf[checksumSize:])
		if err != nil {
			it.err = err
			return false
		}
		if crc(decoded) != checksum {
			it.err = fmt.Errorf("mismatched checksum (got %v, expected %v)", crc(decoded), checksum)
			return false
		}
		if len(remainder) > 0 {
			it.db.B = append(remainder, decoded...)
		} else {
			it.db.B = decoded
		}
	case chunkTypeUncompressedData:
		if !it.readSnappyIdentifier {
			it.err = fmt.Errorf("missing magic snappy marker")
			return false
		}
		if len(it.input) < 4 {
			it.err = io.ErrUnexpectedEOF
			return false
		}
		checksum := uint32(it.input[0]) | uint32(it.input[1])<<8 | uint32(it.input[2])<<16 | uint32(it.input[3])<<24
		if len(it.input) < chunkLen {
			it.err = io.ErrUnexpectedEOF
			return false
		}
		uncompressedData := it.input[checksumSize:chunkLen]
		if crc(uncompressedData) != checksum {
			it.err = fmt.Errorf("mismatched checksum (got %v, expected %v)", crc(uncompressedData), checksum)
			return false
		}

		// NOTE(GiedriusS): we can probably optimize this better but this should be rare enough
		// and not cause any problems.
		if len(remainder) > 0 {
			remainderCopy := make([]byte, 0, len(remainder))
			remainderCopy = append(remainderCopy, remainder...)
			remainder = remainderCopy
		}

		if len(remainder) > 0 {
			it.db.B = append(remainder, uncompressedData...)
		} else {
			it.db.B = uncompressedData
		}
	default:
		if chunkType <= 0x7f {
			it.err = fmt.Errorf("unsupported chunk type %v", chunkType)
			return false
		}
		if chunkType > 0xfd {
			it.err = fmt.Errorf("invalid chunk type %v", chunkType)
			return false
		}
	}
	it.input = it.input[chunkLen:]

	return true
}

func (it *streamedDiffVarintPostings) Next() bool {
	// Continue reading next chunks until there is at least binary.MaxVarintLen64.
	// If we cannot add any more chunks then return false.
	for {
		val := it.db.Uvarint64()
		if it.db.Err() != nil {
			if !it.readNextChunk(it.db.B) {
				return false
			}
			it.db.E = nil
			continue
		}

		it.curSeries = it.curSeries + storage.SeriesRef(val)
		return true
	}
}

func (it *streamedDiffVarintPostings) Err() error {
	return it.err
}

func (it *streamedDiffVarintPostings) Seek(x storage.SeriesRef) bool {
	if it.curSeries >= x {
		return true
	}

	// We cannot do any search due to how values are stored,
	// so we simply advance until we find the right value.
	for it.Next() {
		if it.At() >= x {
			return true
		}
	}

	return false
}

// diffVarintSnappyEncode encodes postings into diff+varint representation,
// and applies snappy compression on the result.
// Returned byte slice starts with codecHeaderSnappy header.
// Length argument is expected number of postings, used for preallocating buffer.
// TODO(GiedriusS): remove for v1.0.
func diffVarintSnappyEncode(p index.Postings, length int) ([]byte, error) {
	buf, err := diffVarintEncodeNoHeader(p, length)
	if err != nil {
		return nil, err
	}

	// Make result buffer large enough to hold our header and compressed block.
	result := make([]byte, len(codecHeaderSnappy)+snappy.MaxEncodedLen(len(buf)))
	copy(result, codecHeaderSnappy)

	compressed := snappy.Encode(result[len(codecHeaderSnappy):], buf)

	// Slice result buffer based on compressed size.
	result = result[:len(codecHeaderSnappy)+len(compressed)]
	return result, nil
}

// diffVarintEncodeNoHeader encodes postings into diff+varint representation.
// It doesn't add any header to the output bytes.
// Length argument is expected number of postings, used for preallocating buffer.
func diffVarintEncodeNoHeader(p index.Postings, length int) ([]byte, error) {
	buf := encoding.Encbuf{}

	// This encoding uses around ~1 bytes per posting, but let's use
	// conservative 1.25 bytes per posting to avoid extra allocations.
	if length > 0 {
		buf.B = make([]byte, 0, 5*length/4)
	}

	prev := storage.SeriesRef(0)
	for p.Next() {
		v := p.At()
		if v < prev {
			return nil, errors.Errorf("postings entries must be in increasing order, current: %d, previous: %d", v, prev)
		}

		// This is the 'diff' part -- compute difference from previous value.
		buf.PutUvarint64(uint64(v - prev))
		prev = v
	}
	if p.Err() != nil {
		return nil, p.Err()
	}

	return buf.B, nil
}

// Creating 15 buckets from 1k to 32mb.
var snappyDecodePool = pool.MustNewBucketedPool[byte](1024, 32*1024*1024, 2, 0)

type closeablePostings interface {
	index.Postings
	close()
}

// alias returns true if given slices have the same both backing array.
// See: https://groups.google.com/g/golang-nuts/c/C6ufGl73Uzk.
func alias(x, y []byte) bool {
	return cap(x) > 0 && cap(y) > 0 && &x[0:cap(x)][cap(x)-1] == &y[0:cap(y)][cap(y)-1]
}

// TODO(GiedriusS): remove for v1.0.
func diffVarintSnappyDecode(input []byte, disablePooling bool) (closeablePostings, error) {
	if !isDiffVarintSnappyEncodedPostings(input) {
		return nil, errors.New("header not found")
	}

	toFree := make([][]byte, 0, 2)

	var dstBuf []byte
	if !disablePooling {
		if len, err := s2.DecodedLen(input[len(codecHeaderSnappy):]); err == nil {
			if decodeBuf, err := snappyDecodePool.Get(len); err == nil && decodeBuf != nil {
				dstBuf = *decodeBuf
				toFree = append(toFree, dstBuf)
			}
		}
	}

	raw, err := s2.Decode(dstBuf, input[len(codecHeaderSnappy):])
	if err != nil {
		return nil, errors.Wrap(err, "snappy decode")
	}

	if !alias(raw, dstBuf) && !disablePooling {
		toFree = append(toFree, raw)
	}

	return newDiffVarintPostings(raw, toFree), nil
}

func newDiffVarintPostings(input []byte, freeSlices [][]byte) *diffVarintPostings {
	return &diffVarintPostings{freeSlices: freeSlices, buf: &encoding.Decbuf{B: input}}
}

// diffVarintPostings is an implementation of index.Postings based on diff+varint encoded data.
type diffVarintPostings struct {
	buf        *encoding.Decbuf
	cur        storage.SeriesRef
	freeSlices [][]byte
}

func (it *diffVarintPostings) close() {
	for i := range it.freeSlices {
		snappyDecodePool.Put(&it.freeSlices[i])
	}
}

func (it *diffVarintPostings) At() storage.SeriesRef {
	return it.cur
}

func (it *diffVarintPostings) Next() bool {
	if it.buf.Err() != nil || it.buf.Len() == 0 {
		return false
	}

	val := it.buf.Uvarint64()
	if it.buf.Err() != nil {
		return false
	}

	it.cur = it.cur + storage.SeriesRef(val)
	return true
}

func (it *diffVarintPostings) Seek(x storage.SeriesRef) bool {
	if it.cur >= x {
		return true
	}

	// We cannot do any search due to how values are stored,
	// so we simply advance until we find the right value.
	for it.Next() {
		if it.At() >= x {
			return true
		}
	}

	return false
}

func (it *diffVarintPostings) Err() error {
	return it.buf.Err()
}

func snappyStreamedEncode(postingsLength int, diffVarintPostings []byte) ([]byte, error) {
	compressedBuf := bytes.NewBuffer(make([]byte, 0, estimateSnappyStreamSize(postingsLength)))
	if n, err := compressedBuf.WriteString(codecHeaderStreamedSnappy); err != nil {
		return nil, fmt.Errorf("writing streamed snappy header")
	} else if n != len(codecHeaderStreamedSnappy) {
		return nil, fmt.Errorf("short-write streamed snappy header")
	}

	sw, err := extsnappy.Compressor.Compress(compressedBuf)
	if err != nil {
		return nil, fmt.Errorf("creating snappy compressor: %w", err)
	}
	_, err = sw.Write(diffVarintPostings)
	if err != nil {
		return nil, err
	}
	if err := sw.Close(); err != nil {
		return nil, errors.Wrap(err, "closing snappy stream writer")
	}

	return compressedBuf.Bytes(), nil
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// crc implements the checksum specified in section 3 of
// https://github.com/google/snappy/blob/master/framing_format.txt
func crc(b []byte) uint32 {
	c := crc32.Update(0, crcTable, b)
	return c>>15 | c<<17 + 0xa282ead8
}
