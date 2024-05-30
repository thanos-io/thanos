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

const UseMergedSeries = "use_merged_series"

// mergedSeries is a storage.Series that implements a simple merge sort algorithm.
// when replicas has conflict values at the same timestamp, the first replica will be selected.
type mergedSeries struct {
	lset     labels.Labels
	replicas []storage.Series
}

func NewMergedSeries(lset labels.Labels, replicas []storage.Series) storage.Series {
	return &mergedSeries{
		lset:     lset,
		replicas: replicas,
	}
}

func (m *mergedSeries) Labels() labels.Labels {
	return m.lset
}
func (m *mergedSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	iters := make([]chunkenc.Iterator, 0, len(m.replicas))
	oks := make([]bool, 0, len(m.replicas))
	for _, r := range m.replicas {
		it := r.Iterator(nil)
		ok := it.Next() != chunkenc.ValNone // iterate to the first value.
		iters = append(iters, it)
		oks = append(oks, ok)
	}
	return &mergedSeriesIterator{
		iters:    iters,
		oks:      oks,
		lastT:    math.MinInt64,
		lastIter: nil, // behavior is undefined if At() is called before Next(), here we panic if it happens.
	}
}

type mergedSeriesIterator struct {
	iters []chunkenc.Iterator
	oks   []bool

	lastT    int64
	lastIter chunkenc.Iterator
}

func (m *mergedSeriesIterator) Next() chunkenc.ValueType {
	return m.Seek(m.lastT + initialPenalty) // apply penalty to avoid selecting samples too close
}

func (m *mergedSeriesIterator) Seek(t int64) chunkenc.ValueType {
	if len(m.iters) == 0 {
		return chunkenc.ValNone
	}

	picked := int64(math.MaxInt64)
	for i, it := range m.iters {
		if !m.oks[i] {
			continue
		}
		if it == m.lastIter || it.AtT() <= m.lastT {
			m.oks[i] = it.Seek(t) != chunkenc.ValNone // move forward for last iterator.
			if !m.oks[i] {
				continue
			}
		}
		currT := it.AtT()
		if currT >= t && currT < picked {
			picked = currT
			m.lastIter = it
		}
	}
	if picked == math.MaxInt64 {
		return chunkenc.ValNone
	}
	m.lastT = picked
	return chunkenc.ValFloat
}
func (m *mergedSeriesIterator) At() (t int64, v float64) {
	return m.lastIter.At()
}

func (it *mergedSeriesIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	return it.lastIter.AtHistogram(h)
}

func (it *mergedSeriesIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return it.lastIter.AtFloatHistogram(fh)
}

func (it *mergedSeriesIterator) AtT() int64 {
	return it.lastT
}

func (m *mergedSeriesIterator) Err() error {
	return m.lastIter.Err()
}
