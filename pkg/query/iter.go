package query

import (
	"math"
	"sort"
	"unsafe"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb/chunkenc"
)

// promSeriesSet implements the SeriesSet interface of the Prometheus storage
// package on top of our storepb SeriesSet.
type promSeriesSet struct {
	set        storepb.SeriesSet
	mint, maxt int64
}

func (s promSeriesSet) Next() bool { return s.set.Next() }
func (s promSeriesSet) Err() error { return s.set.Err() }

func (s promSeriesSet) At() storage.Series {
	lset, chunks := s.set.At()
	return newChunkSeries(lset, chunks, s.mint, s.maxt)
}

func translateMatcher(m *labels.Matcher) (storepb.LabelMatcher, error) {
	var t storepb.LabelMatcher_Type

	switch m.Type {
	case labels.MatchEqual:
		t = storepb.LabelMatcher_EQ
	case labels.MatchNotEqual:
		t = storepb.LabelMatcher_NEQ
	case labels.MatchRegexp:
		t = storepb.LabelMatcher_RE
	case labels.MatchNotRegexp:
		t = storepb.LabelMatcher_NRE
	default:
		return storepb.LabelMatcher{}, errors.Errorf("unrecognized matcher type %d", m.Type)
	}
	return storepb.LabelMatcher{Type: t, Name: m.Name, Value: m.Value}, nil
}

func translateMatchers(ms ...*labels.Matcher) ([]storepb.LabelMatcher, error) {
	res := make([]storepb.LabelMatcher, 0, len(ms))
	for _, m := range ms {
		r, err := translateMatcher(m)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

// storeSeriesSet implements a storepb SeriesSet against a list of storepb.Series.
type storeSeriesSet struct {
	series []storepb.Series
	i      int
}

func newStoreSeriesSet(s []storepb.Series) *storeSeriesSet {
	return &storeSeriesSet{series: s, i: -1}
}

func (s *storeSeriesSet) Next() bool {
	if s.i >= len(s.series)-1 {
		return false
	}
	s.i++
	return true
}

func (storeSeriesSet) Err() error {
	return nil
}

func (s storeSeriesSet) At() ([]storepb.Label, []storepb.AggrChunk) {
	ser := s.series[s.i]
	return ser.Labels, ser.Chunks
}

// chunkSeries implements storage.Series for a series on storepb types.
type chunkSeries struct {
	lset       labels.Labels
	chunks     []storepb.AggrChunk
	mint, maxt int64
}

func newChunkSeries(lset []storepb.Label, chunks []storepb.AggrChunk, mint, maxt int64) *chunkSeries {
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
	chks := make([]storepb.Chunk, 0, len(s.chunks))
	for _, c := range s.chunks {
		chks = append(chks, *c.Raw)
	}
	return newChunkSeriesIterator(chks, s.mint, s.maxt)
}

type nopSeriesIterator struct{}

func (nopSeriesIterator) Seek(int64) bool      { return false }
func (nopSeriesIterator) Next() bool           { return false }
func (nopSeriesIterator) At() (int64, float64) { return 0, 0 }
func (nopSeriesIterator) Err() error           { return nil }

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type chunkSeriesIterator struct {
	chunks     []storepb.Chunk
	maxt, mint int64

	i   int
	cur chunkenc.Iterator
	err error
}

func newChunkSeriesIterator(cs []storepb.Chunk, mint, maxt int64) storage.SeriesIterator {
	if len(cs) == 0 {
		// This should not happen. StoreAPI implementations should not send empty results.
		// NOTE(bplotka): Metric, err log here?
		return nopSeriesIterator{}
	}
	it := &chunkSeriesIterator{
		chunks: cs,
		i:      0,
		mint:   mint,
		maxt:   maxt,
	}
	// TODO(fabxc): just open all iterators immediately?
	it.openChunk()
	return it
}

func (it *chunkSeriesIterator) openChunk() bool {
	c, err := chunkenc.FromData(chunkenc.EncXOR, it.chunks[it.i].Data)
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
		if ct > 0 && ct >= t {
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

type dedupSeriesSet struct {
	set          storage.SeriesSet
	replicaLabel string

	replicas []storage.Series
	lset     labels.Labels
	peek     storage.Series
	ok       bool
}

func newDedupSeriesSet(set storage.SeriesSet, replicaLabel string) storage.SeriesSet {
	s := &dedupSeriesSet{set: set, replicaLabel: replicaLabel}
	s.ok = s.set.Next()
	if s.ok {
		s.peek = s.set.At()
	}
	return s
}

func (s *dedupSeriesSet) Next() bool {
	if !s.ok {
		return false
	}
	// Set the label set we are currently gathering to the peek element
	// without the replica label if it exists.
	s.lset = s.peekLset()
	s.replicas = append(s.replicas[:0], s.peek)
	return s.next()
}

// peekLset returns the label set of the current peek element stripped from the
// replica label if it exists
func (s *dedupSeriesSet) peekLset() labels.Labels {
	lset := s.peek.Labels()
	if lset[len(lset)-1].Name != s.replicaLabel {
		return lset
	}
	return lset[:len(lset)-1]
}

func (s *dedupSeriesSet) next() bool {
	// Peek the next series to see whether it's a replica for the current series.
	s.ok = s.set.Next()
	if !s.ok {
		// There's no next series, the current replicas are the last element.
		return len(s.replicas) > 0
	}
	s.peek = s.set.At()
	nextLset := s.peekLset()

	// If the label set modulo the replica label is equal to the current label set
	// look for more replicas, otherwise a series is complete.
	if !labels.Equal(s.lset, nextLset) {
		return true
	}
	s.replicas = append(s.replicas, s.peek)
	return s.next()
}

func (s *dedupSeriesSet) At() storage.Series {
	if len(s.replicas) == 1 {
		return seriesWithLabels{Series: s.replicas[0], lset: s.lset}
	}
	// Clients may store the series, so we must make a copy of the slice
	// before advancing.
	repl := make([]storage.Series, len(s.replicas))
	copy(repl, s.replicas)
	return newDedupSeries(s.lset, repl...)
}

func (s *dedupSeriesSet) Err() error {
	return s.set.Err()
}

type seriesWithLabels struct {
	storage.Series
	lset labels.Labels
}

func (s seriesWithLabels) Labels() labels.Labels { return s.lset }

type dedupSeries struct {
	lset     labels.Labels
	replicas []storage.Series
}

func newDedupSeries(lset labels.Labels, replicas ...storage.Series) *dedupSeries {
	return &dedupSeries{lset: lset, replicas: replicas}
}

func (s *dedupSeries) Labels() labels.Labels {
	return s.lset
}

func (s *dedupSeries) Iterator() (it storage.SeriesIterator) {
	it = s.replicas[0].Iterator()
	for _, o := range s.replicas[1:] {
		it = newDedupSeriesIterator(it, o.Iterator())
	}
	return it
}

type dedupSeriesIterator struct {
	a, b storage.SeriesIterator
	i    int

	aok, bok   bool
	lastT      int64
	penA, penB int64
	useA       bool
}

func newDedupSeriesIterator(a, b storage.SeriesIterator) *dedupSeriesIterator {
	return &dedupSeriesIterator{
		a:     a,
		b:     b,
		lastT: math.MinInt64,
		aok:   true,
		bok:   true,
	}
}

func (it *dedupSeriesIterator) Next() bool {
	// Advance both iterators to at least the next highest timestamp plus the potential penalty.
	if it.aok {
		it.aok = it.a.Seek(it.lastT + 1 + it.penA)
	}
	if it.bok {
		it.bok = it.b.Seek(it.lastT + 1 + it.penB)
	}
	// Handle basic cases where one iterator is exhausted before the other.
	if !it.aok {
		it.useA = false
		if it.bok {
			it.lastT, _ = it.b.At()
			it.penB = 0
		}
		return it.bok
	}
	if !it.bok {
		it.useA = true
		it.lastT, _ = it.a.At()
		it.penA = 0
		return true
	}
	// General case where both iterators still have data. We pick the one
	// with the smaller timestamp.
	// The applied penalty potentially already skipped potential samples already
	// that would have resulted in exaggerated sampling frequency.
	ta, _ := it.a.At()
	tb, _ := it.b.At()

	it.useA = ta <= tb

	// For the series we didn't pick, add a penalty twice as high as the delta of the last two
	// samples to the next seek against it.
	// This ensures that we don't pick a sample too close, which would increase the overall
	// sample frequency. It also guards against clock drift and inaccuracies during
	// timestamp assignment.
	// If we don't know a delta yet, we pick 5000 as a constant, which is based on the knowledge
	// that timestamps are in milliseconds and sampling frequencies typically multiple seconds long.
	const initialPenality = 5000

	if it.useA {
		if it.lastT != math.MinInt64 {
			it.penB = 2 * (ta - it.lastT)
		} else {
			it.penB = initialPenality
		}
		it.penA = 0
		it.lastT = ta
		return true
	}
	if it.lastT != math.MinInt64 {
		it.penA = 2 * (tb - it.lastT)
	} else {
		it.penA = initialPenality
	}
	it.penB = 0
	it.lastT = tb
	return true
}

func (it *dedupSeriesIterator) Seek(t int64) bool {
	for {
		ts, _ := it.At()
		if ts > 0 && ts >= t {
			return true
		}
		if !it.Next() {
			return false
		}
	}
}

func (it *dedupSeriesIterator) At() (int64, float64) {
	if it.useA {
		return it.a.At()
	}
	return it.b.At()
}

func (it *dedupSeriesIterator) Err() error {
	if it.a.Err() != nil {
		return it.a.Err()
	}
	return it.b.Err()
}
