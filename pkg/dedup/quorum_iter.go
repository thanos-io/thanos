// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dedup

import (
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// quorumSeries is a storage.Series that implements quorum algorithm.
// when replicas has conflict values at the same timestamp, the value in majority replica will be selected.
type quorumSeries struct {
	lset     labels.Labels
	replicas []storage.Series

	isCounter bool
}

func NewQuorumSeries(lset labels.Labels, replicas []storage.Series, f string) storage.Series {
	return &quorumSeries{
		lset:     lset,
		replicas: replicas,

		isCounter: isCounter(f),
	}
}

func (m *quorumSeries) Labels() labels.Labels {
	return m.lset
}
func (m *quorumSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	iters := make([]adjustableSeriesIterator, 0, len(m.replicas))
	oks := make([]bool, 0, len(m.replicas))
	for _, r := range m.replicas {
		var it adjustableSeriesIterator
		if m.isCounter {
			it = &counterErrAdjustSeriesIterator{Iterator: r.Iterator(nil)}
		} else {
			it = &noopAdjustableSeriesIterator{Iterator: r.Iterator(nil)}
		}
		ok := it.Next() != chunkenc.ValNone // iterate to the first value.
		iters = append(iters, it)
		oks = append(oks, ok)
	}
	return &quorumSeriesIterator{
		iters:    iters,
		oks:      oks,
		lastT:    math.MinInt64,
		lastIter: nil, // behavior is undefined if At() is called before Next(), here we panic if it happens.
	}
}

type quorumValuePicker struct {
	currentValue float64
	cnt          int
}

func NewQuorumValuePicker(v float64) *quorumValuePicker {
	return &quorumValuePicker{
		currentValue: v,
		cnt:          1,
	}
}

// Return true if this is the new majority value.
func (q *quorumValuePicker) addValue(v float64) bool {
	if q.currentValue == v {
		q.cnt++
	} else {
		q.cnt--
		if q.cnt == 0 {
			q.currentValue = v
			q.cnt = 1
			return true
		}
	}
	return false
}

type quorumSeriesIterator struct {
	iters []adjustableSeriesIterator
	oks   []bool

	lastT    int64
	lastV    float64
	lastIter adjustableSeriesIterator
}

func (m *quorumSeriesIterator) Next() chunkenc.ValueType {
	// m.lastIter points to the last iterator that has the latest timestamp.
	// m.lastT always aligns with m.lastIter unless when m.lastIter is nil.
	// m.lastIter is nil only in the following cases:
	//   1. Next()/Seek() is never called. m.lastT is math.MinInt64 in this case.
	//   2. The iterator runs out of values. m.lastT is the last timestamp in this case.
	minT := int64(math.MaxInt64)
	var lastIter adjustableSeriesIterator
	quoramValue := NewQuorumValuePicker(0.0)
	for i, it := range m.iters {
		if !m.oks[i] {
			continue
		}
		// apply penalty to avoid selecting samples too close
		m.oks[i] = it.Seek(m.lastT+initialPenalty) != chunkenc.ValNone
		// The it.Seek() call above should guarantee that it.AtT() > m.lastT.
		if m.oks[i] {
			// adjust the current value for counter functions to avoid unexpected resets
			it.adjustAtValue(m.lastV)
			t, v := it.At()
			if t < minT {
				minT = t
				lastIter = it
				quoramValue = NewQuorumValuePicker(v)
			} else if t == minT {
				if quoramValue.addValue(v) {
					lastIter = it
				}
			}
		}
	}
	m.lastIter = lastIter
	if m.lastIter == nil {
		return chunkenc.ValNone
	}
	m.lastV = quoramValue.currentValue
	m.lastT = minT
	return chunkenc.ValFloat
}

func (m *quorumSeriesIterator) Seek(t int64) chunkenc.ValueType {
	// Don't use underlying Seek, but iterate over next to not miss gaps.
	for m.lastT < t && m.Next() != chunkenc.ValNone {
	}
	// Don't call m.Next() again!
	if m.lastIter == nil {
		return chunkenc.ValNone
	}
	return chunkenc.ValFloat
}

func (m *quorumSeriesIterator) At() (t int64, v float64) {
	return m.lastIter.At()
}

func (m *quorumSeriesIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	return m.lastIter.AtHistogram(h)
}

func (m *quorumSeriesIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return m.lastIter.AtFloatHistogram(fh)
}

func (m *quorumSeriesIterator) AtT() int64 {
	return m.lastT
}

// Err All At() funcs should panic if called after Next() or Seek() return ValNone.
// Only Err() should return nil even after Next() or Seek() return ValNone.
func (m *quorumSeriesIterator) Err() error {
	if m.lastIter == nil {
		return nil
	}
	return m.lastIter.Err()
}
