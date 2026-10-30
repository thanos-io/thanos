// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package testiters

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type HistogramPair struct {
	T int64
	H *histogram.Histogram
}

type HistogramIterator struct {
	l []*HistogramPair
	i int
}

func NewHistogramIterator(l []*HistogramPair) *HistogramIterator {
	return &HistogramIterator{l: l, i: -1}
}

func (it *HistogramIterator) Err() error {
	return nil
}

func (it *HistogramIterator) Next() chunkenc.ValueType {
	if it.i >= len(it.l)-1 {
		return chunkenc.ValNone
	}
	it.i++
	return chunkenc.ValHistogram
}

func (it *HistogramIterator) Seek(ts int64) chunkenc.ValueType {
	for {
		if it.i >= len(it.l) {
			return chunkenc.ValNone
		}
		if it.i != -1 && it.l[it.i].T >= ts {
			return chunkenc.ValHistogram
		}
		it.i++
	}
}

func (it *HistogramIterator) At() (t int64, v float64) {
	panic("not implemented")
}

func (it *HistogramIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return it.l[it.i].T, it.l[it.i].H
}

func (it *HistogramIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return it.l[it.i].T, it.l[it.i].H.ToFloat(nil)
}

func (it *HistogramIterator) AtST() int64 { return 0 }

func (it *HistogramIterator) AtT() int64 {
	return it.l[it.i].T
}

type FloatHistogramPair struct {
	T  int64
	FH *histogram.FloatHistogram
}

type FloatHistogramIterator struct {
	l []*FloatHistogramPair
	i int
}

func NewFloatHistogramIterator(l []*FloatHistogramPair) *FloatHistogramIterator {
	return &FloatHistogramIterator{l: l, i: -1}
}

func (it *FloatHistogramIterator) Err() error {
	return nil
}

func (it *FloatHistogramIterator) Next() chunkenc.ValueType {
	if it.i >= len(it.l)-1 {
		return chunkenc.ValNone
	}
	it.i++
	return chunkenc.ValFloatHistogram
}

func (it *FloatHistogramIterator) Seek(ts int64) chunkenc.ValueType {
	for {
		if it.i >= len(it.l) {
			return chunkenc.ValNone
		}
		if it.i != -1 && it.l[it.i].T >= ts {
			return chunkenc.ValFloatHistogram
		}
		it.i++
	}
}

func (it *FloatHistogramIterator) At() (t int64, v float64) {
	panic("not implemented")
}

func (it *FloatHistogramIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	panic("not implemented")
}

func (it *FloatHistogramIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return it.l[it.i].T, it.l[it.i].FH
}

func (it *FloatHistogramIterator) AtST() int64 { return 0 }

func (it *FloatHistogramIterator) AtT() int64 {
	return it.l[it.i].T
}
