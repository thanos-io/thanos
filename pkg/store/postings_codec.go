// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	encoding_binary "encoding/binary"
	"fmt"

	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/index"
)

const (
	// These headers should not be a prefix of each other.
	codecHeaderRaw    = "diff+varint+raw"
	codecHeaderSnappy = "diff+varint+snappy"
)

// isDiffVarintEncodedPostings returns true, if input looks like it has been encoded by diff+varint(+snappy) codec.
func isDiffVarintEncodedPostings(input []byte) bool {
	return bytes.HasPrefix(input, []byte(codecHeaderRaw)) || bytes.HasPrefix(input, []byte(codecHeaderSnappy))
}

// diffVarintSnappyEncode encodes postings using diff+varint+snappy codec.
func diffVarintSnappyEncode(p index.Postings) ([]byte, error) {
	return diffVarintEncode(p, true)
}

// diffVarintEncode encodes postings into diff+varint representation and optional snappy compression.
func diffVarintEncode(p index.Postings, useSnappy bool) ([]byte, error) {
	varintBuf := make([]byte, encoding_binary.MaxVarintLen64)

	buf := bytes.Buffer{}

	// If we're returning raw data, write the header to the buffer, and then return buffer directly.
	if !useSnappy {
		buf.WriteString(codecHeaderRaw)
	}

	prev := uint64(0)
	for p.Next() {
		v := p.At()
		n := encoding_binary.PutUvarint(varintBuf, v-prev)
		buf.Write(varintBuf[:n])

		prev = v
	}

	if p.Err() != nil {
		return nil, p.Err()
	}

	if !useSnappy {
		// This already has the correct header.
		return buf.Bytes(), nil
	}

	// Make result buffer large enough to hold our header and compressed block.
	resultBuf := make([]byte, len(codecHeaderSnappy)+snappy.MaxEncodedLen(buf.Len()))
	copy(resultBuf, codecHeaderSnappy)

	compressed := snappy.Encode(resultBuf[len(codecHeaderSnappy):], buf.Bytes())

	// Slice result buffer based on compressed size.
	resultBuf = resultBuf[:len(codecHeaderSnappy)+len(compressed)]
	return resultBuf, nil
}

func diffVarintDecode(input []byte) (index.Postings, error) {
	compressed := false
	headerLen := 0
	switch {
	case bytes.HasPrefix(input, []byte(codecHeaderRaw)):
		headerLen = len(codecHeaderRaw)
	case bytes.HasPrefix(input, []byte(codecHeaderSnappy)):
		headerLen = len(codecHeaderSnappy)
		compressed = true
	default:
		return nil, errors.New("header not found")
	}

	raw := input[headerLen:]
	if compressed {
		var err error
		raw, err = snappy.Decode(nil, raw)
		if err != nil {
			return nil, fmt.Errorf("snappy decode: %w", err)
		}
	}

	return &diffVarintPostings{data: raw}, nil
}

// Implementation of index.Postings based on diff+varint encoded data.
type diffVarintPostings struct {
	data []byte
	cur  uint64
	err  error
}

func (it *diffVarintPostings) At() uint64 {
	return it.cur
}

func (it *diffVarintPostings) Next() bool {
	if len(it.data) == 0 {
		return false
	}

	val, n := encoding_binary.Uvarint(it.data)
	if n == 0 {
		it.err = errors.New("not enough data")
		return false
	}

	it.data = it.data[n:]
	it.cur = it.cur + val
	it.err = nil
	return true
}

func (it *diffVarintPostings) Seek(x uint64) bool {
	if it.cur >= x {
		return true
	}

	// we cannot do any search due to how values are stored,
	// so we simply advance until we find the right value
	for it.Next() {
		if it.At() >= x {
			return true
		}
	}

	return false
}

func (it *diffVarintPostings) Err() error {
	return it.err
}
