package query

import (
	"sort"
	"unsafe"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb/chunks"
)

type errSeriesSet struct {
	err error
}

func (errSeriesSet) Next() bool                             { return false }
func (s errSeriesSet) Err() error                           { return s.err }
func (errSeriesSet) At() ([]storepb.Label, []storepb.Chunk) { return nil, nil }

type storeSeriesSet struct {
	series []storepb.Series
	i      int
}

func (s *storeSeriesSet) Next() bool {
	if s.i >= len(s.series)-1 {
		return false
	}
	s.i++
	// Skip empty series.
	if len(s.series[s.i].Chunks) == 0 {
		return s.Next()
	}
	return true
}

func (storeSeriesSet) Err() error {
	return nil
}

func (s storeSeriesSet) At() ([]storepb.Label, []storepb.Chunk) {
	ser := s.series[s.i]
	return ser.Labels, ser.Chunks
}

// chunkSeries implements storage.Series for a series on storepb types.
type chunkSeries struct {
	lset       labels.Labels
	chunks     []storepb.Chunk
	mint, maxt int64
}

func newChunkSeries(lset []storepb.Label, chunks []storepb.Chunk, mint, maxt int64) *chunkSeries {
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].MinTime < chunks[j].MinTime
	})
	l := *(*labels.Labels)(unsafe.Pointer(&lset)) // YOLO!

	return &chunkSeries{lset: l, chunks: chunks, mint: mint, maxt: maxt}
}

func (s *chunkSeries) Labels() labels.Labels {
	return s.lset
}

func (s *chunkSeries) Iterator() storage.SeriesIterator {
	return newChunkSeriesIterator(s.chunks, s.mint, s.maxt)
}

type errSeriesIterator struct {
	err error
}

func (errSeriesIterator) Seek(int64) bool      { return false }
func (errSeriesIterator) Next() bool           { return false }
func (errSeriesIterator) At() (int64, float64) { return 0, 0 }
func (s errSeriesIterator) Err() error         { return s.err }

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type chunkSeriesIterator struct {
	chunks     []storepb.Chunk
	maxt, mint int64

	i   int
	cur chunks.Iterator
	err error
}

func newChunkSeriesIterator(cs []storepb.Chunk, mint, maxt int64) storage.SeriesIterator {
	it := &chunkSeriesIterator{
		chunks: cs,
		i:      0,
		mint:   mint,
		maxt:   maxt,
	}
	it.openChunk()
	return it
}

func (it *chunkSeriesIterator) openChunk() bool {
	c, err := chunks.FromData(chunks.EncXOR, it.chunks[it.i].Data)
	if err != nil {
		it.err = err
		return false
	}
	it.cur = c.Iterator()
	return true
}

func (it *chunkSeriesIterator) Seek(t int64) (ok bool) {
	if it.err != nil {
		return false
	}
	if t > it.maxt {
		return false
	}
	// We generally expect the chunks already to be cut down
	// to the range we are interested in. There's not much to be gained from
	// hopping across chunks so we just call next until we reach t.
	for {
		ct, _ := it.At()
		if ct >= t {
			return true
		}
		if !it.Next() {
			return false
		}
	}
}

func (it *chunkSeriesIterator) At() (t int64, v float64) {
	return it.cur.At()
}

func (it *chunkSeriesIterator) Next() bool {
	if it.cur.Next() {
		t, _ := it.cur.At()

		if t < it.mint {
			if !it.Seek(it.mint) {
				return false
			}
			t, _ = it.At()

			return t <= it.maxt
		}
		if t > it.maxt {
			return false
		}
		return true
	}
	if err := it.cur.Err(); err != nil {
		return false
	}
	if it.i == len(it.chunks)-1 {
		return false
	}

	lastT, _ := it.At()

	it.i++
	if !it.openChunk() {
		return false
	}
	// Ensure we don't go back in time.
	return it.Seek(lastT + 1)
}

func (it *chunkSeriesIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.cur.Err()
}
