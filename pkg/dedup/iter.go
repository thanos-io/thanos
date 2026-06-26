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

const (
	AlgorithmPenalty = "penalty"
	AlgorithmChain   = "chain"
)

type dedupSeriesSet struct {
	set               storage.SeriesSet
	isCounter         bool
	deduplicationFunc string

	replicas []storage.Series

	lset labels.Labels
	peek storage.Series
	ok   bool

	f string
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
func NewSeriesSet(set storage.SeriesSet, f string, deduplicationFunc string) storage.SeriesSet {
	// TODO: remove dependency on knowing whether it is a counter.
	s := &dedupSeriesSet{set: set, isCounter: isCounter(f), f: f, deduplicationFunc: deduplicationFunc}
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
	s.replicas = s.replicas[:0]

	// Set the label set we are currently gathering to the peek element.
	s.lset = s.peek.Labels()
	s.replicas = append(s.replicas[:0], s.peek)

	return s.next()
}

func (s *dedupSeriesSet) next() bool {
	// Peek the next series to see whether it's a replica for the current series.
	s.ok = s.set.Next()
	if !s.ok {
		// There's no next series, the current replicas are the last element.
		return len(s.replicas) > 0
	}
	s.peek = s.set.At()
	nextLset := s.peek.Labels()

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

	if s.deduplicationFunc == AlgorithmChain {
		return seriesWithLabels{Series: storage.ChainedSeriesMerge(repl...), lset: s.lset}
	} else {
		return newDedupSeries(s.lset, repl, s.f)
	}
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
	lset     labels.Labels
	replicas []storage.Series

	isCounter bool
	f         string
}

func newDedupSeries(lset labels.Labels, replicas []storage.Series, f string) *dedupSeries {
	return &dedupSeries{lset: lset, isCounter: isCounter(f), replicas: replicas, f: f}
}

func (s *dedupSeries) Labels() labels.Labels {
	return s.lset
}

func (s *dedupSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
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

	return it
}

// adjustableSeriesIterator iterates over the data of a time series and allows to adjust current value based on
// given lastValue iterated.
type adjustableSeriesIterator interface {
	chunkenc.Iterator

	// adjustAtValue allows to adjust value by implementation if needed knowing the last value. This is used by counter
	// implementation which can adjust for obsolete counter value.
	adjustAtValue(lastFloatValue float64)

	// adjustAtValueFloatHistogram is the float-histogram counterpart of adjustAtValue. It lets a counter
	// implementation adjust an obsolete counter-histogram value knowing the last float histogram returned.
	adjustAtValueFloatHistogram(lastFloatHistogram *histogram.FloatHistogram)
}

type noopAdjustableSeriesIterator struct {
	chunkenc.Iterator
}

func (it noopAdjustableSeriesIterator) adjustAtValue(float64) {}

func (it noopAdjustableSeriesIterator) adjustAtValueFloatHistogram(*histogram.FloatHistogram) {}

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

	// histErrAdjust is the float-histogram version of errAdjust: a correction
	// added to every float histogram we return so a replica with an obsolete
	// counter value does not look like a reset. nil until first needed.
	histErrAdjust *histogram.FloatHistogram
}

func (it *counterErrAdjustSeriesIterator) adjustAtValue(lastFloatValue float64) {
	_, v := it.At()
	if lastFloatValue > v {
		// This replica has obsolete value (did not see the correct "end" of counter value before app restart). Adjust.
		it.errAdjust += lastFloatValue - v
	}
}

func (it *counterErrAdjustSeriesIterator) adjustAtValueFloatHistogram(lastFloatHistogram *histogram.FloatHistogram) {
	if lastFloatHistogram == nil {
		return
	}
	_, v := it.AtFloatHistogram(nil)
	// If this replica's count is below the last one we returned, its value is
	// obsolete and would look like a reset. Add the full difference (every bucket,
	// not just the count) so no bucket drops below the last histogram.
	if lastFloatHistogram.Count > v.Count {
		delta, _, _, err := lastFloatHistogram.Copy().Sub(v)
		if err != nil {
			// Schemas or buckets do not match; leave the value as-is instead of
			// returning a broken histogram.
			return
		}
		// The delta is a correction, not a real counter, so mark it a gauge. That
		// keeps the accumulation and later Add from changing reset hints.
		delta.CounterResetHint = histogram.GaugeType
		if it.histErrAdjust == nil {
			it.histErrAdjust = delta
			return
		}
		if sum, _, _, err := it.histErrAdjust.Add(delta); err == nil {
			it.histErrAdjust = sum
		}
	}
}

func (it *counterErrAdjustSeriesIterator) At() (int64, float64) {
	t, v := it.Iterator.At()
	return t, v + it.errAdjust
}

func (it *counterErrAdjustSeriesIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	t, v := it.Iterator.AtFloatHistogram(fh)
	if it.histErrAdjust == nil {
		return t, v
	}
	adjusted, _, _, err := v.Copy().Add(it.histErrAdjust)
	if err != nil {
		// Does not match the accumulated correction; return the raw value.
		return t, v
	}
	// Keep the real sample's reset hint; the gauge correction must not turn the
	// counter histogram into a gauge.
	adjusted.CounterResetHint = v.CounterResetHint
	return t, adjusted
}

type dedupSeriesIterator struct {
	a, b adjustableSeriesIterator

	aval, bval chunkenc.ValueType

	// lastT is the time of the last sample we returned.
	lastT    int64
	lastIter chunkenc.Iterator

	// step is the interval between the primary replica's samples, used to detect
	// gaps. It is 0 until we can determine it.
	step int64

	// lastPrimaryT is the time of the last sample taken from the primary replica
	// (gap fills from the secondary do not change it).
	lastPrimaryT int64

	// primaryA is true while replica A is the primary.
	primaryA bool
	useA     bool

	// firstSecondaryT is the secondary replica's first sample time. While we've
	// only got a single sample from the primary so far we don't know the scrape
	// interval, so we approximate it from how far the secondary has moved past
	// firstSecondaryT.
	firstSecondaryT int64
}

func newDedupSeriesIterator(a, b adjustableSeriesIterator) *dedupSeriesIterator {
	return &dedupSeriesIterator{
		a:               a,
		b:               b,
		lastT:           math.MinInt64,
		lastPrimaryT:    math.MinInt64,
		lastIter:        a,
		primaryA:        true,
		useA:            true,
		firstSecondaryT: math.MinInt64,
		aval:            chunkenc.ValNone,
		bval:            chunkenc.ValNone,
	}
}

func (it *dedupSeriesIterator) Next() chunkenc.ValueType {
	lastFloatVal, isFloatVal := it.lastFloatVal()
	lastFloatHist, isFloatHistVal := it.lastFloatHistogramVal()
	lastUseA := it.useA
	defer func() {
		if it.useA == lastUseA {
			return
		}
		// We switched replicas, so make sure the new replica's value continues from
		// the last one we returned instead of looking like a counter reset.
		if isFloatVal {
			it.adjustAtValue(lastFloatVal)
		} else if isFloatHistVal {
			it.adjustAtValueFloatHistogram(lastFloatHist)
		}
	}()

	// Advance both iterators to the next sample after the last one we returned.
	if it.lastT == math.MinInt64 || it.aval != chunkenc.ValNone {
		it.aval = it.a.Seek(it.lastT + 1)
	}
	if it.lastT == math.MinInt64 || it.bval != chunkenc.ValNone {
		it.bval = it.b.Seek(it.lastT + 1)
	}

	// First sample: pick the replica that starts earlier as the primary.
	if it.lastT == math.MinInt64 {
		it.primaryA = it.aval != chunkenc.ValNone &&
			(it.bval == chunkenc.ValNone || it.a.AtT() <= it.b.AtT())
		// Remember the secondary's first timestamp so we can estimate its interval
		// while we've only got a single sample from the primary.
		if it.primaryA && it.bval != chunkenc.ValNone {
			it.firstSecondaryT = it.b.AtT()
		} else if !it.primaryA && it.aval != chunkenc.ValNone {
			it.firstSecondaryT = it.a.AtT()
		}
		return it.returnPrimary()
	}

	primary, secondary := it.a, it.b
	pval, sval := it.aval, it.bval
	if !it.primaryA {
		primary, secondary = it.b, it.a
		pval, sval = it.bval, it.aval
	}

	// There are no more samples in the primary, so the other one becomes the
	// primary. What we do with its next sample depends on how far it is from the
	// last one we returned:
	//   - Within half a step: too close, treat it as a duplicate and skip it.
	//   - Further out: a real sample, so keep it. The other replica is the only
	//     source now, and dropping it would lose data.
	//
	// Half a step is a rough cut-off, so a real sample that close to the old
	// primary's last one is dropped as a duplicate. We accept that: we can't tell
	// the two apart that close, and a tight cut-off avoids an oddly-spaced sample
	// right after the switch.
	if pval == chunkenc.ValNone {
		it.primaryA = !it.primaryA
		for sval != chunkenc.ValNone && it.step > 0 && secondary.AtT() < it.lastT+it.step/2 {
			sval = secondary.Seek(secondary.AtT() + 1)
		}
		if it.primaryA {
			it.aval = sval
		} else {
			it.bval = sval
		}
		return it.returnPrimary()
	}

	// If the primary replica has a gap, and the other replica has samples in it,
	// use them to fill the gap.
	pt := primary.AtT()
	if it.step == 0 && it.firstSecondaryT != math.MinInt64 && sval != chunkenc.ValNone {
		// We've only got a single sample from the primary so far, so we don't know
		// the scrape interval yet (step is 0). Estimate it from the secondary.
		est := secondary.AtT() - it.firstSecondaryT
		if est == 0 && secondary.AtT() < pt {
			// est is 0 when the other replica is still on its first sample, which
			// happens when it starts a bit later than the primary. We don't know the
			// step yet, so look at its next sample to measure the gap between the two.
			// Seek moves the iterator forward and some iterators (e.g. XOR chunks)
			// can't go back, so only do this when this sample is inside the gap and we
			// will use it. Otherwise we would skip past it and lose it.
			if nval := secondary.Seek(secondary.AtT() + 1); nval != chunkenc.ValNone {
				est = secondary.AtT() - it.firstSecondaryT
				sval = nval
			}
		}
		if est > 0 {
			it.step = est
		}
	}
	if it.step > 0 && pt-it.lastT >= 2*it.step && sval != chunkenc.ValNone {
		st := secondary.AtT()
		// Skip the other replica's samples that are too close to the last one we
		// returned; they would just duplicate it.
		for sval != chunkenc.ValNone && st < it.lastT+it.step {
			sval = secondary.Seek(st + 1)
			if sval == chunkenc.ValNone {
				break
			}
			st = secondary.AtT()
		}
		// Only use the sample if it sits in the gap, more than half a step before the
		// primary replica's next sample. A full step would reject an offset replica's
		// sample that still usefully fills a single-sample gap.
		if sval != chunkenc.ValNone && st < pt-it.step/2 {
			if it.primaryA {
				it.bval = sval
			} else {
				it.aval = sval
			}
			it.useA = !it.primaryA
			it.lastIter = secondary
			it.lastT = st
			return sval
		}
	}

	return it.returnPrimary()
}

// returnPrimary returns the current sample of the primary replica, updates step,
// and moves lastT forward.
func (it *dedupSeriesIterator) returnPrimary() chunkenc.ValueType {
	primary := it.a
	pval := it.aval
	if !it.primaryA {
		primary = it.b
		pval = it.bval
	}
	if pval == chunkenc.ValNone {
		return chunkenc.ValNone
	}
	pt := primary.AtT()
	if it.lastPrimaryT != math.MinInt64 {
		d := pt - it.lastPrimaryT
		// Only update step when two samples are a normal distance apart. A bigger
		// jump is a real gap, which would throw step off.
		if it.step == 0 || d < 2*it.step {
			it.step = d
		}
	}
	it.useA = it.primaryA
	it.lastIter = primary
	it.lastT = pt
	it.lastPrimaryT = pt
	return pval
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

// lastFloatHistogramVal returns a copy of the last histogram we returned as a
// float histogram, if it was a histogram. Integer histograms are read as float
// histograms because that is how the query engine reads them. The copy is needed
// because the iterators are advanced before the adjustment runs and may reuse
// their backing histogram.
func (it *dedupSeriesIterator) lastFloatHistogramVal() (*histogram.FloatHistogram, bool) {
	if it.useA && (it.aval == chunkenc.ValFloatHistogram || it.aval == chunkenc.ValHistogram) {
		_, fh := it.lastIter.AtFloatHistogram(nil)
		return fh.Copy(), true
	}
	if !it.useA && (it.bval == chunkenc.ValFloatHistogram || it.bval == chunkenc.ValHistogram) {
		_, fh := it.lastIter.AtFloatHistogram(nil)
		return fh.Copy(), true
	}
	return nil, false
}

func (it *dedupSeriesIterator) adjustAtValue(lastFloatValue float64) {
	if it.aval == chunkenc.ValFloat {
		it.a.adjustAtValue(lastFloatValue)
	}
	if it.bval == chunkenc.ValFloat {
		it.b.adjustAtValue(lastFloatValue)
	}
}

func (it *dedupSeriesIterator) adjustAtValueFloatHistogram(lastFloatHistogram *histogram.FloatHistogram) {
	if it.aval == chunkenc.ValFloatHistogram || it.aval == chunkenc.ValHistogram {
		it.a.adjustAtValueFloatHistogram(lastFloatHistogram)
	}
	if it.bval == chunkenc.ValFloatHistogram || it.bval == chunkenc.ValHistogram {
		it.b.adjustAtValueFloatHistogram(lastFloatHistogram)
	}
}

func (it *dedupSeriesIterator) Seek(t int64) chunkenc.ValueType {
	if it.lastT == math.MinInt64 {
		if it.Next() == chunkenc.ValNone {
			return chunkenc.ValNone
		}
	}
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

func (it *dedupSeriesIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	return it.lastIter.AtHistogram(h)
}

func (it *dedupSeriesIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return it.lastIter.AtFloatHistogram(fh)
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

func (it *boundedSeriesIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	return it.it.AtHistogram(h)
}

func (it *boundedSeriesIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return it.it.AtFloatHistogram(fh)
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
