// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

type indexBitmapPostings interface {
	index.Postings
	GetBitmap() *roaring.Bitmap
}

type bitmapPostingsReader struct {
	r        *bufio.Reader
	postings []postingPtr

	lastOffset int64
	pi         int
	err        error

	start, length int64

	cur     *bitmapPostings
	readBuf []byte

	repeatFor int
}

type bitmapPostings struct {
	cur          storage.SeriesRef
	bmap         *roaring.Bitmap
	bmapIterator roaring.IntIterable
}

func newBitmapPostingsReader(r *bufio.Reader, postings []postingPtr, start, length int64) *bitmapPostingsReader {
	return &bitmapPostingsReader{
		r:        r,
		postings: postings,
		start:    start,
		length:   length,
		readBuf:  make([]byte, 4),
	}
}

func getInt32(r io.Reader, buf []byte) (uint32, error) {
	read, err := r.Read(buf)
	if err != nil {
		return 0, errors.Wrap(err, "reading")
	}
	if read != 4 {
		return 0, fmt.Errorf("read got %d bytes instead of 4", read)
	}
	return binary.BigEndian.Uint32(buf), nil
}

func (r *bitmapPostingsReader) Err() error {
	if errors.Is(r.err, io.EOF) {
		return nil
	}
	return r.err
}

func (r *bitmapPostings) GetBitmap() *roaring.Bitmap {
	return r.bmap
}

type errBitmapPostings struct {
	e error
}

func (e errBitmapPostings) Next() bool                  { return false }
func (e errBitmapPostings) Seek(storage.SeriesRef) bool { return false }
func (e errBitmapPostings) At() storage.SeriesRef       { return 0 }
func (e errBitmapPostings) Err() error                  { return e.e }
func (e errBitmapPostings) GetBitmap() *roaring.Bitmap  { return nil }

func (r *bitmapPostingsReader) Next() bool {
	if r.repeatFor > 0 {
		r.repeatFor--
		return true
	}
	if r.pi >= len(r.postings) {
		return false
	}
	if r.Err() != nil {
		return false
	}
	from := r.postings[r.pi].ptr.Start - r.start

	if from-r.lastOffset < 0 {
		panic("would have skipped negative bytes")
	}

	_, err := r.r.Discard(int(from - r.lastOffset))
	if err != nil {
		return false
	}
	r.lastOffset += from - r.lastOffset

	bmap := roaring.NewBitmap()

	postingsCount, err := getInt32(r.r, r.readBuf[:])
	if err != nil {
		r.err = err
		return false
	}
	r.lastOffset += 4

	for i := 0; i < int(postingsCount); i++ {
		posting, err := getInt32(r.r, r.readBuf[:])
		if err != nil {
			r.err = err
			return false
		}
		r.lastOffset += 4

		bmap.Add(posting)
	}
	r.cur = &bitmapPostings{
		bmap: bmap,
	}
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

func (r *bitmapPostingsReader) At() *bitmapPostings {
	r.cur.bmapIterator = r.cur.bmap.Iterator()
	return r.cur
}

func (bp *bitmapPostings) Next() bool {
	ret := bp.bmapIterator.HasNext()
	if ret {
		bp.cur = storage.SeriesRef(bp.bmapIterator.Next())
		return true
	}
	return false
}

func (bp *bitmapPostings) Seek(v storage.SeriesRef) bool {
	if bp.At() >= v {
		return true
	}

	for bp.Next() {
		if bp.At() >= v {
			return true
		}
	}

	return false
}

func (bp *bitmapPostings) At() storage.SeriesRef {
	return bp.cur
}

func (bp *bitmapPostings) Err() error {
	return nil
}
