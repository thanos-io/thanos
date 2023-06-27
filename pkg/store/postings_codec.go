// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/s2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
	extsnappy "github.com/thanos-io/thanos/pkg/extgrpc/snappy"
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
	var df func([]byte) (closeablePostings, error)

	switch {
	case isDiffVarintSnappyEncodedPostings(input):
		df = diffVarintSnappyDecode
	case isDiffVarintSnappyStreamedEncodedPostings(input):
		df = diffVarintSnappyStreamedDecode
	default:
		return nil, fmt.Errorf("unrecognize postings format")
	}

	return df(input)
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

func diffVarintSnappyStreamedDecode(input []byte) (closeablePostings, error) {
	if !isDiffVarintSnappyStreamedEncodedPostings(input) {
		return nil, errors.New("header not found")
	}

	return newStreamedDiffVarintPostings(input[len(codecHeaderStreamedSnappy):])
}

type streamedDiffVarintPostings struct {
	cur storage.SeriesRef

	sr  io.ByteReader
	err error
}

func newStreamedDiffVarintPostings(input []byte) (closeablePostings, error) {
	r, err := extsnappy.Compressor.DecompressByteReader(bytes.NewBuffer(input))
	if err != nil {
		return nil, fmt.Errorf("decompressing snappy postings: %w", err)
	}

	return &streamedDiffVarintPostings{sr: r}, nil
}

func (it *streamedDiffVarintPostings) close() {
}

func (it *streamedDiffVarintPostings) At() storage.SeriesRef {
	return it.cur
}

func (it *streamedDiffVarintPostings) Next() bool {
	val, err := binary.ReadUvarint(it.sr)
	if err != nil {
		if err != io.EOF {
			it.err = err
		}
		return false
	}

	it.cur = it.cur + storage.SeriesRef(val)
	return true
}

func (it *streamedDiffVarintPostings) Err() error {
	return it.err
}

func (it *streamedDiffVarintPostings) Seek(x storage.SeriesRef) bool {
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

var snappyDecodePool sync.Pool

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
func diffVarintSnappyDecode(input []byte) (closeablePostings, error) {
	if !isDiffVarintSnappyEncodedPostings(input) {
		return nil, errors.New("header not found")
	}

	toFree := make([][]byte, 0, 2)

	var dstBuf []byte
	decodeBuf := snappyDecodePool.Get()
	if decodeBuf != nil {
		dstBuf = *(decodeBuf.(*[]byte))
		toFree = append(toFree, dstBuf)
	}

	raw, err := s2.Decode(dstBuf, input[len(codecHeaderSnappy):])
	if err != nil {
		return nil, errors.Wrap(err, "snappy decode")
	}

	if !alias(raw, dstBuf) {
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
