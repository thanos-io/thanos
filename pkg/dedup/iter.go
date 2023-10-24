// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dedup

import (
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type dedupSeriesSet struct {
	set       storage.SeriesSet
	isCounter bool

	replicas []storage.Series
	// Pushed down series. Currently, they are being handled in a specific way.
	// In the future, we might want to relax this and handle these depending
	// on what function has been passed.
	pushedDown []storage.Series

	lset labels.Labels
	peek storage.Series
	ok   bool

	f               string
	pushdownEnabled bool
}

// isCounter deduces whether a counter metric has been passed. There must be
// a better way to deduce this.
func isCounter(f string) bool {
	return f == "increase" || f == "rate" || f == "irate" || f == "resets"
}

// NewOverlapSplit splits overlapping chunks into separate series entry, so existing algorithm can work as usual.
// We cannot do this in dedup.SeriesSet as it iterates over samples already.
// TODO(bwplotka): Remove when we move to per chunk deduplication code.
// We expect non-duplicated series with sorted chunks by min time (possibly overlapped).
func NewOverlapSplit(set storepb.SeriesSet) storepb.SeriesSet {
	return &overlapSplitSet{set: set, ok: true}
}

type overlapSplitSet struct {
	ok  bool
	set storepb.SeriesSet

	currLabels labels.Labels
	currI      int
	replicas   [][]storepb.AggrChunk
}

func (o *overlapSplitSet) Next() bool {
	if !o.ok {
		return false
	}

	o.currI++
	if o.currI < len(o.replicas) {
		return true
	}

	o.currI = 0
	o.replicas = o.replicas[:0]
	o.replicas = append(o.replicas, nil)

	o.ok = o.set.Next()
	if !o.ok {
		return false
	}

	var chunks []storepb.AggrChunk
	o.currLabels, chunks = o.set.At()
	if len(chunks) == 0 {
		return true
	}

	o.replicas[0] = append(o.replicas[0], chunks[0])

chunksLoop:
	for i := 1; i < len(chunks); i++ {
		currMinTime := chunks[i].MinTime
		for ri := range o.replicas {
			if len(o.replicas[ri]) == 0 || o.replicas[ri][len(o.replicas[ri])-1].MaxTime < currMinTime {
				o.replicas[ri] = append(o.replicas[ri], chunks[i])
				continue chunksLoop
			}
		}
		o.replicas = append(o.replicas, []storepb.AggrChunk{chunks[i]}) // Not found, add to a new "fake" series.
	}
	return true
}

func (o *overlapSplitSet) At() (labels.Labels, []storepb.AggrChunk) {
	return o.currLabels, o.replicas[o.currI]
}

func (o *overlapSplitSet) Err() error {
	return o.set.Err()
}

// NewSeriesSet returns seriesSet that deduplicates the same series.
// The series in series set are expected be sorted by all labels.
func NewSeriesSet(set storage.SeriesSet, f string, pushdownEnabled bool) storage.SeriesSet {
	// TODO: remove dependency on knowing whether it is a counter.
	s := &dedupSeriesSet{pushdownEnabled: pushdownEnabled, set: set, isCounter: isCounter(f), f: f}
	s.ok = s.set.Next()
	if s.ok {
		s.peek = s.set.At()
	}
	return s
}

// trimPushdownMarker trims the pushdown marker from the given labels.
// Returns true if there was a pushdown marker.
func trimPushdownMarker(lbls labels.Labels) (labels.Labels, bool) {
	return labels.NewBuilder(lbls).Del(PushdownMarker.Name).Labels(), lbls.Has(PushdownMarker.Name)
}

func (s *dedupSeriesSet) Next() bool {
	if !s.ok {
		return false
	}
	// Reset both because they might have some leftovers.
	if s.pushdownEnabled {
		s.pushedDown = s.pushedDown[:0]
	}
	s.replicas = s.replicas[:0]

	// Set the label set we are currently gathering to the peek element.
	s.lset = s.peek.Labels()

	pushedDown := false
	if s.pushdownEnabled {
		s.lset, pushedDown = trimPushdownMarker(s.lset)
	}
	if pushedDown {
		s.pushedDown = append(s.pushedDown[:0], s.peek)
	} else {
		s.replicas = append(s.replicas[:0], s.peek)
	}
	return s.next()
}

func (s *dedupSeriesSet) next() bool {
	// Peek the next series to see whether it's a replica for the current series.
	s.ok = s.set.Next()
	if !s.ok {
		// There's no next series, the current replicas are the last element.
		return len(s.replicas) > 0 || len(s.pushedDown) > 0
	}
	s.peek = s.set.At()
	nextLset := s.peek.Labels()

	var pushedDown bool
	if s.pushdownEnabled {
		nextLset, pushedDown = trimPushdownMarker(nextLset)
	}

	// If the label set modulo the replica label is equal to the current label set
	// look for more replicas, otherwise a series is complete.
	if !labels.Equal(s.lset, nextLset) {
		return true
	}

	if pushedDown {
		s.pushedDown = append(s.pushedDown, s.peek)
	} else {
		s.replicas = append(s.replicas, s.peek)
	}

	return s.next()
}

func (s *dedupSeriesSet) At() storage.Series {
	if len(s.replicas) == 1 && len(s.pushedDown) == 0 {
		return seriesWithLabels{Series: s.replicas[0], lset: s.lset}
	}
	if len(s.replicas) == 0 && len(s.pushedDown) == 1 {
		return seriesWithLabels{Series: s.pushedDown[0], lset: s.lset}
	}
	// Clients may store the series, so we must make a copy of the slice before advancing.
	repl := make([]storage.Series, len(s.replicas))
	copy(repl, s.replicas)

	var pushedDown []storage.Series
	if s.pushdownEnabled {
		pushedDown = make([]storage.Series, len(s.pushedDown))
		copy(pushedDown, s.pushedDown)
	}

	return newDedupSeries(s.lset, repl, pushedDown, s.f)
}

func (s *dedupSeriesSet) Err() error {
	return s.set.Err()
}

func (s *dedupSeriesSet) Warnings() annotations.Annotations {
	return s.set.Warnings()
}

type seriesWithLabels struct {
	storage.Series
	lset labels.Labels
}

func (s seriesWithLabels) Labels() labels.Labels { return s.lset }

type dedupSeries struct {
	lset       labels.Labels
	replicas   []storage.Series
	pushedDown []storage.Series

	isCounter bool
	f         string
}

func newDedupSeries(lset labels.Labels, replicas []storage.Series, pushedDown []storage.Series, f string) *dedupSeries {
	return &dedupSeries{lset: lset, isCounter: isCounter(f), replicas: replicas, pushedDown: pushedDown, f: f}
}

func (s *dedupSeries) Labels() labels.Labels {
	return s.lset
}

// pushdownIterator creates an iterator that handles
// all pushed down series.
func (s *dedupSeries) pushdownIterator(_ chunkenc.Iterator) chunkenc.Iterator {
	var pushedDownIterator adjustableSeriesIterator
	if s.isCounter {
		pushedDownIterator = &counterErrAdjustSeriesIterator{Iterator: s.pushedDown[0].Iterator(nil)}
	} else {
		pushedDownIterator = noopAdjustableSeriesIterator{Iterator: s.pushedDown[0].Iterator(nil)}
	}

	for _, o := range s.pushedDown[1:] {
		var replicaIterator adjustableSeriesIterator

		if s.isCounter {
			replicaIterator = &counterErrAdjustSeriesIterator{Iterator: o.Iterator(nil)}
		} else {
			replicaIterator = noopAdjustableSeriesIterator{Iterator: o.Iterator(nil)}
		}

		pushedDownIterator = noopAdjustableSeriesIterator{newPushdownSeriesIterator(pushedDownIterator, replicaIterator, s.f)}
	}

	return pushedDownIterator
}

// allSeriesIterator creates an iterator over all series - pushed down
// and regular replicas.
func (s *dedupSeries) allSeriesIterator(_ chunkenc.Iterator) chunkenc.Iterator {
	var replicasIterator, pushedDownIterator adjustableSeriesIterator
	if len(s.replicas) != 0 {
		if s.isCounter {
			replicasIterator = &counterErrAdjustSeriesIterator{Iterator: s.replicas[0].Iterator(nil)}
		} else {
			replicasIterator = noopAdjustableSeriesIterator{Iterator: s.replicas[0].Iterator(nil)}
		}

		for _, o := range s.replicas[1:] {
			var replicaIter adjustableSeriesIterator
			if s.isCounter {
				replicaIter = &counterErrAdjustSeriesIterator{Iterator: o.Iterator(nil)}
			} else {
				replicaIter = noopAdjustableSeriesIterator{Iterator: o.Iterator(nil)}
			}
			replicasIterator = newDedupSeriesIterator(replicasIterator, replicaIter)
		}
	}

	if len(s.pushedDown) != 0 {
		if s.isCounter {
			pushedDownIterator = &counterErrAdjustSeriesIterator{Iterator: s.pushedDown[0].Iterator(nil)}
		} else {
			pushedDownIterator = noopAdjustableSeriesIterator{Iterator: s.pushedDown[0].Iterator(nil)}
		}

		for _, o := range s.pushedDown[1:] {
			var replicaIter adjustableSeriesIterator
			if s.isCounter {
				replicaIter = &counterErrAdjustSeriesIterator{Iterator: o.Iterator(nil)}
			} else {
				replicaIter = noopAdjustableSeriesIterator{Iterator: o.Iterator(nil)}
			}
			pushedDownIterator = newDedupSeriesIterator(pushedDownIterator, replicaIter)
		}
	}

	if replicasIterator == nil {
		return pushedDownIterator
	}
	if pushedDownIterator == nil {
		return replicasIterator
	}
	return newDedupSeriesIterator(pushedDownIterator, replicasIterator)
}

func (s *dedupSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	// This function needs a regular iterator over all series. Behavior is identical
	// whether it was pushed down or not.
	if s.f == "group" {
		return s.allSeriesIterator(nil)
	}
	// If there are no replicas then jump straight to constructing an iterator
	// for pushed down series.
	if len(s.replicas) == 0 {
		return s.pushdownIterator(nil)
	}

	// Finally, if we have both then construct a tree out of them.
	// Pushed down series have their own special iterator.
	// We deduplicate everything in the end.
	var it adjustableSeriesIterator
	if s.isCounter {
		it = &counterErrAdjustSeriesIterator{Iterator: s.replicas[0].Iterator(nil)}
	} else {
		it = noopAdjustableSeriesIterator{Iterator: s.replicas[0].Iterator(nil)}
	}

	for _, o := range s.replicas[1:] {
		var replicaIter adjustableSeriesIterator
		if s.isCounter {
			replicaIter = &counterErrAdjustSeriesIterator{Iterator: o.Iterator(nil)}
		} else {
			replicaIter = noopAdjustableSeriesIterator{Iterator: o.Iterator(nil)}
		}
		it = newDedupSeriesIterator(it, replicaIter)
	}

	if len(s.pushedDown) == 0 {
		return it
	}

	// Join all of the pushed down iterators into one.
	var pushedDownIterator adjustableSeriesIterator
	if s.isCounter {
		pushedDownIterator = &counterErrAdjustSeriesIterator{Iterator: s.pushedDown[0].Iterator(nil)}
	} else {
		pushedDownIterator = noopAdjustableSeriesIterator{Iterator: s.pushedDown[0].Iterator(nil)}
	}

	for _, o := range s.pushedDown[1:] {
		var replicaIterator adjustableSeriesIterator

		if s.isCounter {
			replicaIterator = &counterErrAdjustSeriesIterator{Iterator: o.Iterator(nil)}
		} else {
			replicaIterator = noopAdjustableSeriesIterator{Iterator: o.Iterator(nil)}
		}

		pushedDownIterator = noopAdjustableSeriesIterator{newPushdownSeriesIterator(pushedDownIterator, replicaIterator, s.f)}
	}

	return newDedupSeriesIterator(it, pushedDownIterator)
}

// adjustableSeriesIterator iterates over the data of a time series and allows to adjust current value based on
// given lastValue iterated.
type adjustableSeriesIterator interface {
	chunkenc.Iterator

	// adjustAtValue allows to adjust value by implementation if needed knowing the last value. This is used by counter
	// implementation which can adjust for obsolete counter value.
	adjustAtValue(lastFloatValue float64)
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

func (it *counterErrAdjustSeriesIterator) adjustAtValue(lastFloatValue float64) {
	_, v := it.At()
	if lastFloatValue > v {
		// This replica has obsolete value (did not see the correct "end" of counter value before app restart). Adjust.
		it.errAdjust += lastFloatValue - v
	}
}

func (it *counterErrAdjustSeriesIterator) At() (int64, float64) {
	t, v := it.Iterator.At()
	return t, v + it.errAdjust
}

type dedupSeriesIterator struct {
	a, b adjustableSeriesIterator

	aval, bval chunkenc.ValueType

	// TODO(bwplotka): Don't base on LastT, but on detected scrape interval. This will allow us to be more
	// responsive to gaps: https://github.com/thanos-io/thanos/issues/981, let's do it in next PR.
	lastT    int64
	lastIter chunkenc.Iterator

	penA, penB int64
	useA       bool
}

func newDedupSeriesIterator(a, b adjustableSeriesIterator) *dedupSeriesIterator {
	return &dedupSeriesIterator{
		a:        a,
		b:        b,
		lastT:    math.MinInt64,
		lastIter: a,
		aval:     a.Next(),
		bval:     b.Next(),
	}
}

func (it *dedupSeriesIterator) Next() chunkenc.ValueType {
	lastFloatVal, isFloatVal := it.lastFloatVal()
	lastUseA := it.useA
	defer func() {
		if it.useA != lastUseA && isFloatVal {
			// We switched replicas.
			// Ensure values are correct bases on value before At.
			// TODO(rabenhorst): Investigate if we also need to implement adjusting histograms here.
			it.adjustAtValue(lastFloatVal)
		}
	}()

	// Advance both iterators to at least the next highest timestamp plus the potential penalty.
	if it.aval != chunkenc.ValNone {
		it.aval = it.a.Seek(it.lastT + 1 + it.penA)
	}
	if it.bval != chunkenc.ValNone {
		it.bval = it.b.Seek(it.lastT + 1 + it.penB)
	}

	// Handle basic cases where one iterator is exhausted before the other.
	if it.aval == chunkenc.ValNone {
		it.useA = false
		if it.bval != chunkenc.ValNone {
			it.lastT = it.b.AtT()
			it.lastIter = it.b
			it.penB = 0
		}
		return it.bval
	}
	if it.bval == chunkenc.ValNone {
		it.useA = true
		it.lastT = it.a.AtT()
		it.lastIter = it.a
		it.penA = 0
		return it.aval
	}
	// General case where both iterators still have data. We pick the one
	// with the smaller timestamp.
	// The applied penalty potentially already skipped potential samples already
	// that would have resulted in exaggerated sampling frequency.
	ta := it.a.AtT()
	tb := it.b.AtT()

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
		it.lastIter = it.a

		return it.aval
	}
	if it.lastT != math.MinInt64 {
		it.penA = 2 * (tb - it.lastT)
	} else {
		it.penA = initialPenalty
	}
	it.penB = 0
	it.lastT = tb
	it.lastIter = it.b
	return it.bval
}

func (it *dedupSeriesIterator) lastFloatVal() (float64, bool) {
	if it.useA && it.aval == chunkenc.ValFloat {
		_, v := it.lastIter.At()
		return v, true
	}
	if !it.useA && it.bval == chunkenc.ValFloat {
		_, v := it.lastIter.At()
		return v, true
	}
	return 0, false
}

func (it *dedupSeriesIterator) adjustAtValue(lastFloatValue float64) {
	if it.aval == chunkenc.ValFloat {
		it.a.adjustAtValue(lastFloatValue)
	}
	if it.bval == chunkenc.ValFloat {
		it.b.adjustAtValue(lastFloatValue)
	}
}

func (it *dedupSeriesIterator) Seek(t int64) chunkenc.ValueType {
	// Don't use underlying Seek, but iterate over next to not miss gaps.
	for {
		ts := it.AtT()
		if ts >= t {
			if it.useA {
				return it.a.Seek(ts)
			}
			return it.b.Seek(ts)
		}
		if it.Next() == chunkenc.ValNone {
			return chunkenc.ValNone
		}
	}
}

func (it *dedupSeriesIterator) At() (int64, float64) {
	return it.lastIter.At()
}

func (it *dedupSeriesIterator) AtHistogram() (int64, *histogram.Histogram) {
	return it.lastIter.AtHistogram()
}

func (it *dedupSeriesIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	return it.lastIter.AtFloatHistogram()
}

func (it *dedupSeriesIterator) AtT() int64 {
	var t int64
	if it.useA {
		t = it.a.AtT()
	} else {
		t = it.b.AtT()
	}
	return t
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

func (it *boundedSeriesIterator) Seek(t int64) chunkenc.ValueType {
	if t > it.maxt {
		return chunkenc.ValNone
	}
	if t < it.mint {
		t = it.mint
	}
	return it.it.Seek(t)
}

func (it *boundedSeriesIterator) At() (t int64, v float64) {
	return it.it.At()
}

func (it *boundedSeriesIterator) AtHistogram() (int64, *histogram.Histogram) {
	return it.it.AtHistogram()
}

func (it *boundedSeriesIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	return it.it.AtFloatHistogram()
}

func (it *boundedSeriesIterator) AtT() int64 {
	return it.it.AtT()
}

func (it *boundedSeriesIterator) Next() chunkenc.ValueType {
	valueType := it.it.Next()
	if valueType == chunkenc.ValNone {
		return chunkenc.ValNone
	}
	t := it.it.AtT()

	// Advance the iterator if we are before the valid interval.
	if t < it.mint {
		if it.Seek(it.mint) == chunkenc.ValNone {
			return chunkenc.ValNone
		}
		t = it.it.AtT()
	}
	// Once we passed the valid interval, there is no going back.
	if t <= it.maxt {
		return valueType
	}

	return chunkenc.ValNone
}

func (it *boundedSeriesIterator) Err() error {
	return it.it.Err()
}
