// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/alecthomas/units"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/errutil"
	extsnappy "github.com/thanos-io/thanos/pkg/extgrpc/snappy"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
)

// Read postings directly from a reader.
// While reading, encode them.
// At the end of reading, store them!
// Note that this depends on the reader completely
// exhausting the postings.
type streamedPostingsReader struct {
	bktReader      io.ReadCloser
	bufferedReader io.Reader

	blockID    ulid.ULID
	l          labels.Label
	indexCache storecache.IndexCache
	cur        uint32
	err        error

	uvarintEncodeBuf []byte
	readBuf          []byte

	sw            io.WriteCloser
	compressedBuf *bytes.Buffer

	stats         *queryStats
	statsMtx      *sync.Mutex
	postingsCount uint32
}

func newStreamedPostingsReader(
	bktReader io.ReadCloser,
	blockID ulid.ULID,
	l labels.Label,
	indexCache storecache.IndexCache,
	stats *queryStats,
	statsMtx *sync.Mutex,
) (*streamedPostingsReader, error) {
	r := &streamedPostingsReader{
		bktReader:        bktReader,
		blockID:          blockID,
		l:                l,
		indexCache:       indexCache,
		uvarintEncodeBuf: make([]byte, binary.MaxVarintLen64),
		readBuf:          make([]byte, 4),

		stats:    stats,
		statsMtx: statsMtx,
	}

	postingsCount, err := getInt32(bktReader, r.readBuf[:0])
	if err != nil {
		return nil, errors.Wrap(err, "getting postings count")
	}
	r.postingsCount = postingsCount
	r.bufferedReader = bufio.NewReader(io.LimitReader(bktReader, int64(postingsCount)*4))

	compressedBuf := bytes.NewBuffer(make([]byte, 0, estimateSnappyStreamSize(int(postingsCount))))
	if n, err := compressedBuf.WriteString(codecHeaderStreamedSnappy); err != nil {
		return nil, fmt.Errorf("writing streamed snappy header")
	} else if n != len(codecHeaderStreamedSnappy) {
		return nil, fmt.Errorf("short-write streamed snappy header")
	}

	sw, err := extsnappy.Compressor.Compress(compressedBuf)
	if err != nil {
		return nil, fmt.Errorf("creating snappy compressor: %w", err)
	}
	r.sw = sw
	r.compressedBuf = compressedBuf

	return r, nil
}

func getInt32(r io.Reader, buf []byte) (uint32, error) {
	read, err := r.Read(buf[:0])
	if err != nil {
		return 0, errors.Wrap(err, "reading")
	}
	if read != 4 {
		return 0, fmt.Errorf("read got %d bytes instead of 4", read)
	}
	return binary.BigEndian.Uint32(buf), nil
}

func (r *streamedPostingsReader) Close() error {
	var errs errutil.MultiError

	closeSnappyErr := r.sw.Close()
	errs.Add(closeSnappyErr)

	if errors.Is(r.err, io.EOF) && closeSnappyErr == nil {
		r.indexCache.StorePostings(r.blockID, r.l, r.compressedBuf.Bytes())
	}

	if r.stats != nil {
		r.statsMtx.Lock()
		r.stats.cachedPostingsCompressions++
		r.stats.CachedPostingsOriginalSizeSum += units.Base2Bytes(4 + 4*r.postingsCount)
		r.stats.PostingsTouchedSizeSum += units.Base2Bytes(4 + 4*r.postingsCount)
		r.stats.CachedPostingsCompressedSizeSum += units.Base2Bytes(r.compressedBuf.Len())
		r.statsMtx.Unlock()
	}

	errs.Add(r.bktReader.Close())
	return errs.Err()
}

func (r *streamedPostingsReader) At() storage.SeriesRef {
	return storage.SeriesRef(r.cur)
}

func (r *streamedPostingsReader) Err() error {
	if errors.Is(r.err, io.EOF) {
		return nil
	}
	return r.err
}

func (r *streamedPostingsReader) bumpCompressionErrors() {
	if r.stats == nil {
		return
	}
	r.statsMtx.Lock()
	r.stats.cachedPostingsCompressionErrors++
	r.statsMtx.Unlock()
}

func (r *streamedPostingsReader) Next() bool {
	n, err := getInt32(r.bufferedReader, r.readBuf[:0])
	if err != nil {
		r.err = err
		if !errors.Is(err, io.EOF) {
			r.bumpCompressionErrors()
		}
		return false
	}
	if n < r.cur {
		r.bumpCompressionErrors()
		r.err = fmt.Errorf("got non-decreasing values: %v, %v", n, r.cur)
		return false
	}

	uvarintSize := binary.PutUvarint(r.uvarintEncodeBuf, uint64(n-r.cur))
	if written, err := r.sw.Write(r.uvarintEncodeBuf[:uvarintSize]); err != nil {
		r.bumpCompressionErrors()
		r.err = errors.Wrap(err, "writing uvarint encoded byte")
		return false
	} else if written != uvarintSize {
		r.bumpCompressionErrors()
		r.err = errors.Wrap(err, "short-write for uvarint encoded byte")
		return false
	}

	r.cur = n
	return true
}

func (r *streamedPostingsReader) Seek(x storage.SeriesRef) bool {
	if r.At() >= x {
		return true
	}

	// We cannot do any search due to how values are stored,
	// so we simply advance until we find the right value.
	for r.Next() {
		if r.At() >= x {
			return true
		}
	}

	return false
}
