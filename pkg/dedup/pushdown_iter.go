// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dedup

import (
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// PushdownMarker is a label that gets attached on pushed down series so that
// the receiver would be able to handle them in potentially special way.
var PushdownMarker = labels.Label{Name: "__thanos_pushed_down", Value: "true"}

type pushdownSeriesIterator struct {
	a, b         chunkenc.Iterator
	aval, bval   chunkenc.ValueType
	aused, bused bool

	function func(float64, float64) float64
}

// newPushdownSeriesIterator constructs a new iterator that steps through both
// series and performs the following algorithm:
// * If both timestamps match up then the function is applied on them;
// * If one of the series has a gap then the other one is used until the timestamps match up.
// It is guaranteed that stepping through both of them that the timestamps will match eventually
// because the samples have been processed by a PromQL engine.
func newPushdownSeriesIterator(a, b chunkenc.Iterator, function string) *pushdownSeriesIterator {
	var fn func(float64, float64) float64
	switch function {
	case "max", "max_over_time":
		fn = math.Max
	case "min", "min_over_time":
		fn = math.Min
	default:
		panic(fmt.Errorf("unsupported function %s passed", function))
	}
	return &pushdownSeriesIterator{
		a: a, b: b, function: fn, aused: true, bused: true,
	}
}

func (it *pushdownSeriesIterator) Next() chunkenc.ValueType {
	// Push A if we've used A before. Push B if we've used B before.
	// Push both if we've used both before.
	switch {
	case !it.aused && !it.bused:
		return chunkenc.ValNone
	case it.aused && !it.bused:
		it.aval = it.a.Next()
	case !it.aused && it.bused:
		it.bval = it.b.Next()
	case it.aused && it.bused:
		it.aval = it.a.Next()
		it.bval = it.b.Next()
	}
	it.aused = false
	it.bused = false

	if it.aval != chunkenc.ValNone {
		return it.aval
	}

	if it.bval != chunkenc.ValNone {
		return it.bval
	}

	return chunkenc.ValNone
}

func (it *pushdownSeriesIterator) At() (int64, float64) {

	var timestamp int64
	var val float64

	if it.aval != chunkenc.ValNone && it.bval != chunkenc.ValNone {
		ta, va := it.a.At()
		tb, vb := it.b.At()
		if ta == tb {
			val = it.function(va, vb)
			timestamp = ta
			it.aused = true
			it.bused = true
		} else {
			if ta < tb {
				timestamp = ta
				val = va
				it.aused = true
			} else {
				timestamp = tb
				val = vb
				it.bused = true
			}
		}
	} else if it.aval != chunkenc.ValNone {
		ta, va := it.a.At()
		val = va
		timestamp = ta
		it.aused = true
	} else {
		tb, vb := it.b.At()
		val = vb
		timestamp = tb
		it.bused = true
	}

	return timestamp, val
}

// TODO(rabenhorst): Needs to be implemented for native histogram support.
func (it *pushdownSeriesIterator) AtHistogram() (int64, *histogram.Histogram) {
	panic("not implemented")
}

func (it *pushdownSeriesIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	panic("not implemented")
}

func (it *pushdownSeriesIterator) AtT() int64 {
	t := it.a.AtT()
	return t
}

func (it *pushdownSeriesIterator) Seek(t int64) chunkenc.ValueType {
	for {
		ts := it.AtT()
		if ts >= t {
			return chunkenc.ValFloat
		}
		if it.Next() == chunkenc.ValNone {
			return chunkenc.ValNone
		}
	}
}

func (it *pushdownSeriesIterator) Err() error {
	if it.a.Err() != nil {
		return it.a.Err()
	}
	return it.b.Err()
}
