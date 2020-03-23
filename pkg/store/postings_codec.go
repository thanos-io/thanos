// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"

	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/encoding"
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

// diffVarintEncode encodes postings into diff+varint representation and optional snappy compression.
func diffVarintEncode(p index.Postings, useSnappy bool) ([]byte, error) {
	buf := encoding.Encbuf{}

	if !useSnappy {
		buf.PutString(codecHeaderRaw)
	}

	prev := uint64(0)
	for p.Next() {
		v := p.At()
		if v < prev {
			// Postings entries must be in increasing order.
			return nil, errors.Errorf("decreasing entry, val: %d, prev: %d", v, prev)
		}

		buf.PutUvarint64(v - prev)
		prev = v
	}

	if p.Err() != nil {
		return nil, p.Err()
	}

	if !useSnappy {
		// When not using Snappy, buffer already has the correct header.
		return buf.B, nil
	}

	// Make result buffer large enough to hold our header and compressed block.
	resultBuf := make([]byte, len(codecHeaderSnappy)+snappy.MaxEncodedLen(buf.Len()))
	copy(resultBuf, codecHeaderSnappy)

	compressed := snappy.Encode(resultBuf[len(codecHeaderSnappy):], buf.B)

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
			return nil, errors.Errorf("snappy decode: %w", err)
		}
	}

	return &diffVarintPostings{buf: &encoding.Decbuf{B: raw}}, nil
}

// Implementation of index.Postings based on diff+varint encoded data.
type diffVarintPostings struct {
	buf *encoding.Decbuf
	cur uint64
}

func (it *diffVarintPostings) At() uint64 {
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

	it.cur = it.cur + val
	return true
}

func (it *diffVarintPostings) Seek(x uint64) bool {
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
