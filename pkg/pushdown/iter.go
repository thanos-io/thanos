// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pushdown

import (
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type pushdownSeriesSet struct {
	set           storage.SeriesSet
	replicaLabels map[string]struct{}

	replicas []storage.Series
	lset     labels.Labels
	peek     storage.Series
	ok       bool

	function string
}

func NewSeriesSet(set storage.SeriesSet, replicaLabels map[string]struct{}, function string) storage.SeriesSet {
	s := &pushdownSeriesSet{set: set, replicaLabels: replicaLabels, function: function}
	s.ok = s.set.Next()
	if s.ok {
		s.peek = s.set.At()
	}
	return s
}

func (s *pushdownSeriesSet) Next() bool {
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
func (s *pushdownSeriesSet) peekLset() labels.Labels {
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

func (s *pushdownSeriesSet) next() bool {
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

func (s *pushdownSeriesSet) At() storage.Series {
	if len(s.replicas) == 1 {
		return seriesWithLabels{Series: s.replicas[0], lset: s.lset}
	}
	// Clients may store the series, so we must make a copy of the slice before advancing.
	repl := make([]storage.Series, len(s.replicas))
	copy(repl, s.replicas)
	return newPushdownSeries(s.lset, repl, s.function)
}

func (s *pushdownSeriesSet) Err() error {
	return s.set.Err()
}

func (s *pushdownSeriesSet) Warnings() storage.Warnings {
	return s.set.Warnings()
}

type seriesWithLabels struct {
	storage.Series
	lset labels.Labels
}

func (s seriesWithLabels) Labels() labels.Labels { return s.lset }

type pushdownSeries struct {
	lset     labels.Labels
	replicas []storage.Series
	function string
}

func newPushdownSeries(lset labels.Labels, replicas []storage.Series, function string) *pushdownSeries {
	return &pushdownSeries{lset: lset, function: function, replicas: replicas}
}

func (s *pushdownSeries) Labels() labels.Labels {
	return s.lset
}

func (s *pushdownSeries) Iterator() chunkenc.Iterator {
	var it chunkenc.Iterator = s.replicas[0].Iterator()

	for _, o := range s.replicas[1:] {
		it = newPushdownSeriesIterator(it, o.Iterator(), s.function)
	}
	return it
}

type pushdownSeriesIterator struct {
	a, b     chunkenc.Iterator
	aok, bok bool

	function func(float64, float64) float64
}

func newPushdownSeriesIterator(a, b chunkenc.Iterator, function string) *pushdownSeriesIterator {
	var fn func(float64, float64) float64
	switch function {
	case "max", "max_over_time":
		fn = math.Max
	case "min", "min_over_time":
		fn = math.Min
	default:
		panic(fmt.Sprintf("unsupported function %s passed", function))
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

// These should have identical timestamps. Just need to apply the function here.
func (it *pushdownSeriesIterator) At() (int64, float64) {
	ta, va := it.a.At()
	tb, vb := it.b.At()

	var timestamp int64
	var val float64
	if it.aok && it.bok {
		val = it.function(va, vb)
		if ta != tb {
			panic("BUG: expected timestamps in pushed down queries to be aligned to the same step")
		}
		timestamp = ta
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

func (it *pushdownSeriesIterator) Err() error {
	if it.a.Err() != nil {
		return it.a.Err()
	}
	return it.b.Err()
}
