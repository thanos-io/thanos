// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

// Some of the code in this file was adapted from Prometheus (https://github.com/prometheus/prometheus).
// The original license header is included below:
//
// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package series

import (
	"sort"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// ConcreteSeriesSet implements storage.SeriesSet.
type ConcreteSeriesSet struct {
	cur    int
	series []storage.Series
}

// NewConcreteSeriesSet instantiates an in-memory series set from a series
// Series will be sorted by labels.
func NewConcreteSeriesSet(series []storage.Series) storage.SeriesSet {
	sort.Sort(byLabels(series))
	return &ConcreteSeriesSet{
		cur:    -1,
		series: series,
	}
}

// Next iterates through a series set and implements storage.SeriesSet.
func (c *ConcreteSeriesSet) Next() bool {
	c.cur++
	return c.cur < len(c.series)
}

// At returns the current series and implements storage.SeriesSet.
func (c *ConcreteSeriesSet) At() storage.Series {
	return c.series[c.cur]
}

// Err implements storage.SeriesSet.
func (c *ConcreteSeriesSet) Err() error {
	return nil
}

// Warnings implements storage.SeriesSet.
func (c *ConcreteSeriesSet) Warnings() storage.Warnings {
	return nil
}

// ConcreteSeries implements storage.Series.
type ConcreteSeries struct {
	labels  labels.Labels
	samples []model.SamplePair
}

// NewConcreteSeries instantiates an in memory series from a list of samples & labels
func NewConcreteSeries(ls labels.Labels, samples []model.SamplePair) *ConcreteSeries {
	return &ConcreteSeries{
		labels:  ls,
		samples: samples,
	}
}

// Labels implements storage.Series
func (c *ConcreteSeries) Labels() labels.Labels {
	return c.labels
}

// Iterator implements storage.Series
func (c *ConcreteSeries) Iterator() chunkenc.Iterator {
	return NewConcreteSeriesIterator(c)
}

// concreteSeriesIterator implements chunkenc.Iterator.
type concreteSeriesIterator struct {
	cur    int
	series *ConcreteSeries
}

// NewConcreteSeriesIterator instaniates an in memory chunkenc.Iterator
func NewConcreteSeriesIterator(series *ConcreteSeries) chunkenc.Iterator {
	return &concreteSeriesIterator{
		cur:    -1,
		series: series,
	}
}

// TODO(rabenhorst): Native histogram support needs to be added, float type is hardcoded.
func (c *concreteSeriesIterator) Seek(t int64) chunkenc.ValueType {
	c.cur = sort.Search(len(c.series.samples), func(n int) bool {
		return c.series.samples[n].Timestamp >= model.Time(t)
	})

	if c.cur < len(c.series.samples) {
		return chunkenc.ValFloat
	}

	return chunkenc.ValNone
}

func (c *concreteSeriesIterator) At() (t int64, v float64) {
	s := c.series.samples[c.cur]
	return int64(s.Timestamp), float64(s.Value)
}

// TODO(rabenhorst): Needs to be implemented for native histogram support.
func (c *concreteSeriesIterator) AtHistogram() (int64, *histogram.Histogram) {
	panic("not implemented")
}

func (c *concreteSeriesIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	panic("not implemented")
}

func (c *concreteSeriesIterator) AtT() int64 {
	t, _ := c.At()
	return t
}

func (c *concreteSeriesIterator) Next() chunkenc.ValueType {
	c.cur++

	if c.cur < len(c.series.samples) {
		return chunkenc.ValFloat
	}

	return chunkenc.ValNone
}

func (c *concreteSeriesIterator) Err() error {
	return nil
}

// NewErrIterator instantiates an errIterator
func NewErrIterator(err error) chunkenc.Iterator {
	return errIterator{err}
}

// errIterator implements chunkenc.Iterator, just returning an error.
type errIterator struct {
	err error
}

func (errIterator) Seek(int64) chunkenc.ValueType {
	return chunkenc.ValNone
}

func (errIterator) Next() chunkenc.ValueType {
	return chunkenc.ValNone
}

func (errIterator) At() (t int64, v float64) {
	return 0, 0
}

func (errIterator) AtHistogram() (int64, *histogram.Histogram) {
	return 0, nil
}

func (errIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	return 0, nil
}

func (errIterator) AtT() int64 {
	return 0
}

func (e errIterator) Err() error {
	return e.err
}

// MatrixToSeriesSet creates a storage.SeriesSet from a model.Matrix
// Series will be sorted by labels.
func MatrixToSeriesSet(m model.Matrix) storage.SeriesSet {
	series := make([]storage.Series, 0, len(m))
	for _, ss := range m {
		series = append(series, &ConcreteSeries{
			labels:  metricToLabels(ss.Metric),
			samples: ss.Values,
		})
	}
	return NewConcreteSeriesSet(series)
}

func metricToLabels(m model.Metric) labels.Labels {
	ls := make(labels.Labels, 0, len(m))
	for k, v := range m {
		ls = append(ls, labels.Label{
			Name:  string(k),
			Value: string(v),
		})
	}
	// PromQL expects all labels to be sorted! In general, anyone constructing
	// a labels.Labels list is responsible for sorting it during construction time.
	sort.Sort(ls)
	return ls
}

type byLabels []storage.Series

func (b byLabels) Len() int           { return len(b) }
func (b byLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byLabels) Less(i, j int) bool { return labels.Compare(b[i].Labels(), b[j].Labels()) < 0 }

type DeletedSeriesIterator struct {
	itr              chunkenc.Iterator
	deletedIntervals []model.Interval
}

func NewDeletedSeriesIterator(itr chunkenc.Iterator, deletedIntervals []model.Interval) chunkenc.Iterator {
	return &DeletedSeriesIterator{
		itr:              itr,
		deletedIntervals: deletedIntervals,
	}
}

// TODO(rabenhorst): Native histogram support needs to be added, float type is hardcoded.
func (d DeletedSeriesIterator) Seek(t int64) chunkenc.ValueType {
	if valueType := d.itr.Seek(t); valueType == chunkenc.ValNone {
		return valueType
	}

	seekedTs, _ := d.itr.At()
	if d.isDeleted(seekedTs) {
		// point we have seeked into is deleted, Next() should find a new non-deleted sample which is after t and seekedTs
		return d.Next()
	}

	return chunkenc.ValFloat
}

func (d DeletedSeriesIterator) At() (t int64, v float64) {
	return d.itr.At()
}

// TODO(rabenhorst): Needs to be implemented for native histogram support.
func (d DeletedSeriesIterator) AtHistogram() (int64, *histogram.Histogram) {
	panic("not implemented")
}

func (d DeletedSeriesIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	panic("not implemented")
}

func (d DeletedSeriesIterator) AtT() int64 {
	t, _ := d.itr.At()
	return t
}

func (d DeletedSeriesIterator) Next() chunkenc.ValueType {
	for valueType := d.itr.Next(); valueType != chunkenc.ValNone; valueType = d.itr.Next() {
		ts, _ := d.itr.At()

		if d.isDeleted(ts) {
			continue
		}
		return valueType
	}
	return chunkenc.ValNone
}

func (d DeletedSeriesIterator) Err() error {
	return d.itr.Err()
}

// isDeleted removes intervals which are past ts while checking for whether ts happens to be in one of the deleted intervals
func (d *DeletedSeriesIterator) isDeleted(ts int64) bool {
	mts := model.Time(ts)

	for _, interval := range d.deletedIntervals {
		if mts > interval.End {
			d.deletedIntervals = d.deletedIntervals[1:]
			continue
		} else if mts < interval.Start {
			return false
		}

		return true
	}

	return false
}
