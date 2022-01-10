// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dedup

import (
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// PushdownMarker is a label that gets attached on pushed down series so that
// the receiver would be able to handle them in potentially special way.
var PushdownMarker = labels.Label{Name: "__thanos_pushed_down", Value: "true"}

type pushdownSeriesIterator struct {
	a, b     chunkenc.Iterator
	aok, bok bool

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
		a: a, b: b, function: fn,
	}
}

func (it *pushdownSeriesIterator) Next() bool {
	it.aok = it.a.Next()
	it.bok = it.b.Next()
	return it.aok || it.bok
}

func (it *pushdownSeriesIterator) At() (int64, float64) {
	ta, va := it.a.At()
	tb, vb := it.b.At()

	var timestamp int64
	var val float64

	if it.aok && it.bok {
		if ta == tb {
			val = it.function(va, vb)
			timestamp = ta
		} else {
			if ta < tb {
				timestamp = ta
				val = va
			} else {
				timestamp = tb
				val = vb
			}
		}
	} else if it.aok {
		val = va
		timestamp = ta
	} else {
		val = vb
		timestamp = tb
	}

	return timestamp, val
}

func (it *pushdownSeriesIterator) Seek(t int64) bool {
	seekA := it.a.Seek(t)
	seekB := it.b.Seek(t)
	return seekA || seekB
}

func (it *pushdownSeriesIterator) Err() error {
	if it.a.Err() != nil {
		return it.a.Err()
	}
	return it.b.Err()
}
