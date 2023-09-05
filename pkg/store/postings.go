// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

type postingsReaderBuilder struct {
	e       error
	readBuf []byte

	r        *bufio.Reader
	postings []postingPtr

	lastOffset int64
	pi         int

	start, length         int64
	cur                   []byte
	keyID                 int
	repeatFor             int
	numberOfPostingsInCur uint64
	uvarintEncodeBuf      []byte
	ctx                   context.Context
}

// newPostingsReaderBuilder is a builder that reads directly from the index
// and builds a diff varint encoded []byte that could be later used directly.
func newPostingsReaderBuilder(ctx context.Context, r *bufio.Reader, postings []postingPtr, start, length int64) *postingsReaderBuilder {
	prb := &postingsReaderBuilder{
		r:                r,
		readBuf:          make([]byte, 4),
		start:            start,
		length:           length,
		postings:         postings,
		uvarintEncodeBuf: make([]byte, binary.MaxVarintLen64),
		ctx:              ctx,
	}

	return prb
}

func getInt32(r io.Reader, buf []byte) (uint32, error) {
	read, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, errors.Wrap(err, "reading")
	}
	if read != 4 {
		return 0, fmt.Errorf("read got %d bytes instead of 4", read)
	}
	return binary.BigEndian.Uint32(buf), nil
}

func (r *postingsReaderBuilder) Next() bool {
	if r.ctx.Err() != nil {
		r.e = r.ctx.Err()
		return false
	}
	if r.repeatFor > 0 {
		r.keyID = r.postings[r.pi-r.repeatFor].keyID
		r.repeatFor--
		return true
	}
	if r.pi >= len(r.postings) {
		return false
	}
	if r.Error() != nil {
		return false
	}
	from := r.postings[r.pi].ptr.Start - r.start

	if from-r.lastOffset < 0 {
		panic("would have skipped negative bytes")
	}

	_, err := r.r.Discard(int(from - r.lastOffset))
	if err != nil {
		r.e = err
		return false
	}
	r.lastOffset += from - r.lastOffset

	postingsCount, err := getInt32(r.r, r.readBuf[:])
	if err != nil {
		r.e = err
		return false
	}
	r.lastOffset += 4

	// Assume 1.25 bytes per compressed posting.
	r.cur = make([]byte, 0, int(float64(postingsCount)*1.25))

	prev := uint32(0)

	for i := 0; i < int(postingsCount); i++ {
		posting, err := getInt32(r.r, r.readBuf[:])
		if err != nil {
			r.e = err
			return false
		}
		r.lastOffset += 4

		uvarintSize := binary.PutUvarint(r.uvarintEncodeBuf, uint64(posting-prev))
		r.cur = append(r.cur, r.uvarintEncodeBuf[:uvarintSize]...)
		prev = posting
	}
	r.numberOfPostingsInCur = uint64(postingsCount)

	r.keyID = r.postings[r.pi].keyID
	r.pi++
	for {
		if r.pi >= len(r.postings) {
			break
		}

		if r.postings[r.pi].ptr.Start == r.postings[r.pi-1].ptr.Start &&
			r.postings[r.pi].ptr.End == r.postings[r.pi-1].ptr.End {
			r.repeatFor++
			r.pi++
			continue
		}

		break
	}

	return true
}

func (r *postingsReaderBuilder) Error() error {
	return r.e
}

func (r *postingsReaderBuilder) AtDiffVarint() ([]byte, uint64, int) {
	return r.cur, r.numberOfPostingsInCur, r.keyID
}
