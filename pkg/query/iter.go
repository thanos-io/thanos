// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"math"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// promSeriesSet implements the SeriesSet interface of the Prometheus storage
// package on top of our storepb SeriesSet.
type promSeriesSet struct {
	set  storepb.SeriesSet
	done bool

	mint, maxt int64
	aggrs      []storepb.Aggr
	initiated  bool

	currLset   []storepb.Label
	currChunks []storepb.AggrChunk

	warns storage.Warnings
}

func (s *promSeriesSet) Next() bool {
	if !s.initiated {
		s.initiated = true
		s.done = s.set.Next()
	}

	if !s.done {
		return false
	}

	// storage.Series are more strict then SeriesSet:
	// * It requires storage.Series to iterate over full series.
	s.currLset, s.currChunks = s.set.At()
	for {
		s.done = s.set.Next()
		if !s.done {
			break
		}
		nextLset, nextChunks := s.set.At()
		if storepb.CompareLabels(s.currLset, nextLset) != 0 {
			break
		}
		s.currChunks = append(s.currChunks, nextChunks...)
	}

	// Samples (so chunks as well) have to be sorted by time.
	// TODO(bwplotka): Benchmark if we can do better.
	// For example we could iterate in above loop and write our own binary search based insert sort.
	// We could also remove duplicates in same loop.
	sort.Slice(s.currChunks, func(i, j int) bool {
		return s.currChunks[i].MinTime < s.currChunks[j].MinTime
	})

	// Proxy handles duplicates between different series, let's handle duplicates within single series now as well.
	// We don't need to decode those.
	s.currChunks = removeExactDuplicates(s.currChunks)
	return true
}

// removeExactDuplicates returns chunks without 1:1 duplicates.
// NOTE: input chunks has to be sorted by minTime.
func removeExactDuplicates(chks []storepb.AggrChunk) []storepb.AggrChunk {
	if len(chks) <= 1 {
		return chks
	}

	ret := make([]storepb.AggrChunk, 0, len(chks))
	ret = append(ret, chks[0])

	for _, c := range chks[1:] {
		if ret[len(ret)-1].Compare(c) == 0 {
			continue
		}
		ret = append(ret, c)
	}
	return ret
}

func (s *promSeriesSet) At() storage.Series {
	if !s.initiated || s.set.Err() != nil {
		return nil
	}
	return newChunkSeries(s.currLset, s.currChunks, s.mint, s.maxt, s.aggrs)
}

func (s *promSeriesSet) Err() error {
	return s.set.Err()
}

func (s *promSeriesSet) Warnings() storage.Warnings {
	return s.warns
}

// storeSeriesSet implements a storepb SeriesSet against a list of storepb.Series.
type storeSeriesSet struct {
	// TODO(bwplotka): Don't buffer all, we have to buffer single series (to sort and dedup chunks), but nothing more.
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
	return s.series[s.i].Labels, s.series[s.i].Chunks
}

// chunkSeries implements storage.Series for a series on storepb types.
type chunkSeries struct {
	lset       labels.Labels
	chunks     []storepb.AggrChunk
	mint, maxt int64
	aggrs      []storepb.Aggr
}

// newChunkSeries allows to iterate over samples for each sorted and non-overlapped chunks.
func newChunkSeries(lset []storepb.Label, chunks []storepb.AggrChunk, mint, maxt int64, aggrs []storepb.Aggr) *chunkSeries {
	return &chunkSeries{
		lset:   storepb.LabelsToPromLabels(lset),
		chunks: chunks,
		mint:   mint,
		maxt:   maxt,
		aggrs:  aggrs,
	}
}

func (s *chunkSeries) Labels() labels.Labels {
	return s.lset
}

func (s *chunkSeries) Iterator() chunkenc.Iterator {
	var sit chunkenc.Iterator
	its := make([]chunkenc.Iterator, 0, len(s.chunks))

	if len(s.aggrs) == 1 {
		switch s.aggrs[0] {
		case storepb.Aggr_COUNT:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Count, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case storepb.Aggr_SUM:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Sum, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case storepb.Aggr_MIN:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Min, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case storepb.Aggr_MAX:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Max, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case storepb.Aggr_COUNTER:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Counter, c.Raw))
			}
			sit = downsample.NewApplyCounterResetsIterator(its...)
		default:
			return errSeriesIterator{err: errors.Errorf("unexpected result aggregate type %v", s.aggrs)}
		}
		return newBoundedSeriesIterator(sit, s.mint, s.maxt)
	}

	if len(s.aggrs) != 2 {
		return errSeriesIterator{err: errors.Errorf("unexpected result aggregate type %v", s.aggrs)}
	}

	switch {
	case s.aggrs[0] == storepb.Aggr_SUM && s.aggrs[1] == storepb.Aggr_COUNT,
		s.aggrs[0] == storepb.Aggr_COUNT && s.aggrs[1] == storepb.Aggr_SUM:

		for _, c := range s.chunks {
			if c.Raw != nil {
				its = append(its, getFirstIterator(c.Raw))
			} else {
				sum, cnt := getFirstIterator(c.Sum), getFirstIterator(c.Count)
				its = append(its, downsample.NewAverageChunkIterator(cnt, sum))
			}
		}
		sit = newChunkSeriesIterator(its)
	default:
		return errSeriesIterator{err: errors.Errorf("unexpected result aggregate type %v", s.aggrs)}
	}
	return newBoundedSeriesIterator(sit, s.mint, s.maxt)
}

func getFirstIterator(cs ...*storepb.Chunk) chunkenc.Iterator {
	for _, c := range cs {
		if c == nil {
			continue
		}
		chk, err := chunkenc.FromData(chunkEncoding(c.Type), c.Data)
		if err != nil {
			return errSeriesIterator{err}
		}
		return chk.Iterator(nil)
	}
	return errSeriesIterator{errors.New("no valid chunk found")}
}

func chunkEncoding(e storepb.Chunk_Encoding) chunkenc.Encoding {
	switch e {
	case storepb.Chunk_XOR:
		return chunkenc.EncXOR
	}
	return 255 // Invalid.
}

type errSeriesIterator struct {
	err error
}

func (errSeriesIterator) Seek(int64) bool      { return false }
func (errSeriesIterator) Next() bool           { return false }
func (errSeriesIterator) At() (int64, float64) { return 0, 0 }
func (it errSeriesIterator) Err() error        { return it.err }

// boundedSeriesIterator wraps a series iterator and ensures that it only emits
// samples within a fixed time range.
type boundedSeriesIterator struct {
	it         chunkenc.Iterator
	mint, maxt int64
}

func newBoundedSeriesIterator(it chunkenc.Iterator, mint, maxt int64) *boundedSeriesIterator {
	return &boundedSeriesIterator{it: it, mint: mint, maxt: maxt}
}

func (it *boundedSeriesIterator) Seek(t int64) (ok bool) {
	if t > it.maxt {
		return false
	}
	if t < it.mint {
		t = it.mint
	}
	return it.it.Seek(t)
}

func (it *boundedSeriesIterator) At() (t int64, v float64) {
	return it.it.At()
}

func (it *boundedSeriesIterator) Next() bool {
	if !it.it.Next() {
		return false
	}
	t, _ := it.it.At()

	// Advance the iterator if we are before the valid interval.
	if t < it.mint {
		if !it.Seek(it.mint) {
			return false
		}
		t, _ = it.it.At()
	}
	// Once we passed the valid interval, there is no going back.
	return t <= it.maxt
}

func (it *boundedSeriesIterator) Err() error {
	return it.it.Err()
}

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type chunkSeriesIterator struct {
	chunks []chunkenc.Iterator
	i      int
}

func newChunkSeriesIterator(cs []chunkenc.Iterator) chunkenc.Iterator {
	if len(cs) == 0 {
		// This should not happen. StoreAPI implementations should not send empty results.
		return errSeriesIterator{err: errors.Errorf("store returned an empty result")}
	}
	return &chunkSeriesIterator{chunks: cs}
}

func (it *chunkSeriesIterator) Seek(t int64) (ok bool) {
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
	return it.chunks[it.i].At()
}

func (it *chunkSeriesIterator) Next() bool {
	lastT, _ := it.At()

	if it.chunks[it.i].Next() {
		return true
	}
	if it.Err() != nil {
		return false
	}
	if it.i >= len(it.chunks)-1 {
		return false
	}
	// Chunks are guaranteed to be ordered but not generally guaranteed to not overlap.
	// We must ensure to skip any overlapping range between adjacent chunks.
	it.i++
	return it.Seek(lastT + 1)
}

func (it *chunkSeriesIterator) Err() error {
	return it.chunks[it.i].Err()
}

type dedupSeriesSet struct {
	set           storage.SeriesSet
	replicaLabels map[string]struct{}
	isCounter     bool

	replicas []storage.Series
	lset     labels.Labels
	peek     storage.Series
	ok       bool
}

func newDedupSeriesSet(set storage.SeriesSet, replicaLabels map[string]struct{}, isCounter bool) storage.SeriesSet {
	s := &dedupSeriesSet{set: set, replicaLabels: replicaLabels, isCounter: isCounter}
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
// replica label if it exists.
func (s *dedupSeriesSet) peekLset() labels.Labels {
	lset := s.peek.Labels()
	if len(s.replicaLabels) == 0 {
		return lset
	}
	// Check how many replica labels are present so that these are removed.
	var totalToRemove int
	for i := 0; i < len(s.replicaLabels); i++ {
		if len(lset)-i == 0 {
			break
		}

		if _, ok := s.replicaLabels[lset[len(lset)-i-1].Name]; ok {
			totalToRemove++
		}
	}
	// Strip all present replica labels.
	return lset[:len(lset)-totalToRemove]
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
	// Clients may store the series, so we must make a copy of the slice before advancing.
	repl := make([]storage.Series, len(s.replicas))
	copy(repl, s.replicas)
	return newDedupSeries(s.lset, repl, s.isCounter)
}

func (s *dedupSeriesSet) Err() error {
	return s.set.Err()
}

func (s *dedupSeriesSet) Warnings() storage.Warnings {
	return s.set.Warnings()
}

type seriesWithLabels struct {
	storage.Series
	lset labels.Labels
}

func (s seriesWithLabels) Labels() labels.Labels { return s.lset }

type dedupSeries struct {
	lset     labels.Labels
	replicas []storage.Series

	isCounter bool
}

func newDedupSeries(lset labels.Labels, replicas []storage.Series, isCounter bool) *dedupSeries {
	return &dedupSeries{lset: lset, isCounter: isCounter, replicas: replicas}
}

func (s *dedupSeries) Labels() labels.Labels {
	return s.lset
}

func (s *dedupSeries) Iterator() chunkenc.Iterator {
	var it adjustableSeriesIterator
	if s.isCounter {
		it = &counterErrAdjustSeriesIterator{Iterator: s.replicas[0].Iterator()}
	} else {
		it = noopAdjustableSeriesIterator{Iterator: s.replicas[0].Iterator()}
	}

	for _, o := range s.replicas[1:] {
		var replicaIter adjustableSeriesIterator
		if s.isCounter {
			replicaIter = &counterErrAdjustSeriesIterator{Iterator: o.Iterator()}
		} else {
			replicaIter = noopAdjustableSeriesIterator{Iterator: o.Iterator()}
		}
		it = newDedupSeriesIterator(it, replicaIter)
	}
	return it
}

// adjustableSeriesIterator iterates over the data of a time series and allows to adjust current value based on
// given lastValue iterated.
type adjustableSeriesIterator interface {
	chunkenc.Iterator

	// adjustAtValue allows to adjust value by implementation if needed knowing the last value. This is used by counter
	// implementation which can adjust for obsolete counter value.
	adjustAtValue(lastValue float64)
}

type noopAdjustableSeriesIterator struct {
	chunkenc.Iterator
}

func (it noopAdjustableSeriesIterator) adjustAtValue(float64) {}

// counterErrAdjustSeriesIterator is extendedSeriesIterator used when we deduplicate counter.
// It makes sure we always adjust for the latest seen last counter value for all replicas.
// Let's consider following example:
//
// Replica 1 counter scrapes: 20    30    40    Nan      -     0     5
// Replica 2 counter scrapes:    25    35    45     Nan     -     2
//
// Now for downsampling purposes we are accounting the resets(rewriting the samples value)
// so our replicas before going to dedup iterator looks like this:
//
// Replica 1 counter total: 20    30    40   -      -     40     45
// Replica 2 counter total:    25    35    45    -     -     47
//
// Now if at any point we will switch our focus from replica 2 to replica 1 we will experience lower value than previous,
// which will trigger false positive counter reset in PromQL.
//
// We mitigate this by taking allowing invoking AdjustAtValue which adjust the value in case of last value being larger than current at.
// (Counter cannot go down)
//
// This is to mitigate https://github.com/thanos-io/thanos/issues/2401.
// TODO(bwplotka): Find better deduplication algorithm that does not require knowledge if the given
// series is counter or not: https://github.com/thanos-io/thanos/issues/2547.
type counterErrAdjustSeriesIterator struct {
	chunkenc.Iterator

	errAdjust float64
}

func (it *counterErrAdjustSeriesIterator) adjustAtValue(lastValue float64) {
	_, v := it.At()
	if lastValue > v {
		// This replica has obsolete value (did not see the correct "end" of counter value before app restart). Adjust.
		it.errAdjust += lastValue - v
	}
}

func (it *counterErrAdjustSeriesIterator) At() (int64, float64) {
	t, v := it.Iterator.At()
	return t, v + it.errAdjust
}

type dedupSeriesIterator struct {
	a, b adjustableSeriesIterator

	aok, bok bool

	// TODO(bwplotka): Don't base on LastT, but on detected scrape interval. This will allow us to be more
	// responsive to gaps: https://github.com/thanos-io/thanos/issues/981, let's do it in next PR.
	lastT int64
	lastV float64

	penA, penB int64
	useA       bool
}

func newDedupSeriesIterator(a, b adjustableSeriesIterator) *dedupSeriesIterator {
	return &dedupSeriesIterator{
		a:     a,
		b:     b,
		lastT: math.MinInt64,
		lastV: float64(math.MinInt64),
		aok:   a.Next(),
		bok:   b.Next(),
	}
}

func (it *dedupSeriesIterator) Next() bool {
	lastValue := it.lastV
	lastUseA := it.useA
	defer func() {
		if it.useA != lastUseA {
			// We switched replicas.
			// Ensure values are correct bases on value before At.
			it.adjustAtValue(lastValue)
		}
	}()

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
			it.lastT, it.lastV = it.b.At()
			it.penB = 0
		}
		return it.bok
	}
	if !it.bok {
		it.useA = true
		it.lastT, it.lastV = it.a.At()
		it.penA = 0
		return true
	}
	// General case where both iterators still have data. We pick the one
	// with the smaller timestamp.
	// The applied penalty potentially already skipped potential samples already
	// that would have resulted in exaggerated sampling frequency.
	ta, va := it.a.At()
	tb, vb := it.b.At()

	it.useA = ta <= tb

	// For the series we didn't pick, add a penalty twice as high as the delta of the last two
	// samples to the next seek against it.
	// This ensures that we don't pick a sample too close, which would increase the overall
	// sample frequency. It also guards against clock drift and inaccuracies during
	// timestamp assignment.
	// If we don't know a delta yet, we pick 5000 as a constant, which is based on the knowledge
	// that timestamps are in milliseconds and sampling frequencies typically multiple seconds long.
	const initialPenalty = 5000

	if it.useA {
		if it.lastT != math.MinInt64 {
			it.penB = 2 * (ta - it.lastT)
		} else {
			it.penB = initialPenalty
		}
		it.penA = 0
		it.lastT = ta
		it.lastV = va
		return true
	}
	if it.lastT != math.MinInt64 {
		it.penA = 2 * (tb - it.lastT)
	} else {
		it.penA = initialPenalty
	}
	it.penB = 0
	it.lastT = tb
	it.lastV = vb
	return true
}

func (it *dedupSeriesIterator) adjustAtValue(lastValue float64) {
	if it.aok {
		it.a.adjustAtValue(lastValue)
	}
	if it.bok {
		it.b.adjustAtValue(lastValue)
	}
}

func (it *dedupSeriesIterator) Seek(t int64) bool {
	// Don't use underlying Seek, but iterate over next to not miss gaps.
	for {
		ts, _ := it.At()
		if ts >= t {
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

type lazySeriesSet struct {
	create func() (s storage.SeriesSet, ok bool)

	set storage.SeriesSet
}

func (c *lazySeriesSet) Next() bool {
	if c.set != nil {
		return c.set.Next()
	}

	var ok bool
	c.set, ok = c.create()
	return ok
}

func (c *lazySeriesSet) Err() error {
	if c.set != nil {
		return c.set.Err()
	}
	return nil
}

func (c *lazySeriesSet) At() storage.Series {
	if c.set != nil {
		return c.set.At()
	}
	return nil
}

func (c *lazySeriesSet) Warnings() storage.Warnings {
	if c.set != nil {
		return c.set.Warnings()
	}
	return nil
}
