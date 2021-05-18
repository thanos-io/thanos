// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dedup

import (
	"math"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type dedupSeriesSet struct {
	set           storage.SeriesSet
	replicaLabels map[string]struct{}
	isCounter     bool

	replicas []storage.Series
	lset     labels.Labels
	peek     storage.Series
	ok       bool
}

func NewSeriesSet(set storage.SeriesSet, replicaLabels map[string]struct{}, isCounter bool) storage.SeriesSet {
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

// boundedSeriesIterator wraps a series iterator and ensures that it only emits
// samples within a fixed time range.
type boundedSeriesIterator struct {
	it         chunkenc.Iterator
	mint, maxt int64
}

func NewBoundedSeriesIterator(it chunkenc.Iterator, mint, maxt int64) *boundedSeriesIterator {
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
