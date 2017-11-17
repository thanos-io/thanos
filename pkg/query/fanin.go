package query

import (
	"sort"
	"unsafe"

	"strings"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb/chunks"
)

func mergeAllSeriesSets(all ...chunkSeriesSet) chunkSeriesSet {
	switch len(all) {
	case 0:
		return &storeSeriesSet{}
	case 1:
		return all[0]
	}
	h := len(all) / 2

	return newMergedSeriesSet(
		mergeAllSeriesSets(all[:h]...),
		mergeAllSeriesSets(all[h:]...),
	)
}

type chunkSeriesSet interface {
	Next() bool
	At() ([]storepb.Label, []storepb.Chunk)
	Err() error
}

// mergedSeriesSet takes two series sets as a single series set. The input series sets
// must be sorted and sequential in time, i.e. if they have the same label set,
// the datapoints of a must be before the datapoints of b.
type mergedSeriesSet struct {
	a, b chunkSeriesSet

	lset         []storepb.Label
	chunks       []storepb.Chunk
	adone, bdone bool
}

// NewMergedSeriesSet takes two series sets as a single series set.
// Series that occur in both sets should have disjoint time ranges and a should com before b.
// If the ranges overlap, the result series will still have monotonically increasing timestamps,
// but all samples in the overlapping range in b will be dropped.
func newMergedSeriesSet(a, b chunkSeriesSet) *mergedSeriesSet {
	s := &mergedSeriesSet{a: a, b: b}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	s.adone = !s.a.Next()
	s.bdone = !s.b.Next()

	return s
}

func (s *mergedSeriesSet) At() ([]storepb.Label, []storepb.Chunk) {
	return s.lset, s.chunks
}

func (s *mergedSeriesSet) Err() error {
	if s.a.Err() != nil {
		return s.a.Err()
	}
	return s.b.Err()
}

func (s *mergedSeriesSet) compare() int {
	if s.adone {
		return 1
	}
	if s.bdone {
		return -1
	}
	lsetA, _ := s.a.At()
	lsetB, _ := s.b.At()
	return storepb.CompareLabels(lsetA, lsetB)
}

func (s *mergedSeriesSet) Next() bool {
	if s.adone && s.bdone || s.Err() != nil {
		return false
	}

	d := s.compare()

	// Both sets contain the current series. Chain them into a single one.
	if d > 0 {
		s.lset, s.chunks = s.b.At()
		s.bdone = !s.b.Next()
	} else if d < 0 {
		s.lset, s.chunks = s.a.At()
		s.adone = !s.a.Next()
	} else {
		// Concatenate chunks from both series sets. They may be out of order
		// w.r.t to their time range. This must be accounted for later.
		lset, chksA := s.a.At()
		_, chksB := s.b.At()

		s.lset = lset
		// Slice reuse is not generally safe with nested merge iterators.
		// We err on the safe side an create a new slice.
		s.chunks = make([]storepb.Chunk, 0, len(chksA)+len(chksB))
		s.chunks = append(s.chunks, chksA...)
		s.chunks = append(s.chunks, chksB...)

		s.adone = !s.a.Next()
		s.bdone = !s.b.Next()
	}
	return true
}

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

func dedupStrings(a []string) []string {
	if len(a) == 0 {
		return nil
	}
	if len(a) == 1 {
		return a
	}
	l := len(a) / 2
	return mergeStrings(dedupStrings(a[:l]), dedupStrings(a[l:]))
}

func mergeStrings(a, b []string) []string {
	maxl := len(a)
	if len(b) > len(a) {
		maxl = len(b)
	}
	res := make([]string, 0, maxl*10/9)

	for len(a) > 0 && len(b) > 0 {
		d := strings.Compare(a[0], b[0])

		if d == 0 {
			res = append(res, a[0])
			a, b = a[1:], b[1:]
		} else if d < 0 {
			res = append(res, a[0])
			a = a[1:]
		} else if d > 0 {
			res = append(res, b[0])
			b = b[1:]
		}
	}

	// Append all remaining elements.
	res = append(res, a...)
	res = append(res, b...)
	return res
}
