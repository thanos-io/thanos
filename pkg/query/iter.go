// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// storeSeriesSet implements a storepb SeriesSet against a list of storepb.Series.
type storeSeriesSet struct {
	// TODO(bwplotka): Don't buffer all, we have to buffer single series (to sort and dedup chunks), but nothing more.
	series []storepb.Series
	i      int
}

func newStoreSeriesSet(s []storepb.Series) *storeSeriesSet {
	return &storeSeriesSet{series: s, i: -1}
}

func (s *storeSeriesSet) Next() bool {
	if s.i >= len(s.series)-1 {
		return false
	}
	s.i++
	return true
}

func (storeSeriesSet) Err() error {
	return nil
}

func (s storeSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	return s.series[s.i].PromLabels(), s.series[s.i].Chunks
}

type lazySeriesSet struct {
	create func() (s storage.SeriesSet, ok bool)

	set storage.SeriesSet
}

func (c *lazySeriesSet) Next() bool {
	if c.set != nil {
		return c.set.Next()
	}

	var ok bool
	c.set, ok = c.create()
	return ok
}

func (c *lazySeriesSet) Err() error {
	if c.set != nil {
		return c.set.Err()
	}
	return nil
}

func (c *lazySeriesSet) At() storage.Series {
	if c.set != nil {
		return c.set.At()
	}
	return nil
}

func (c *lazySeriesSet) Warnings() storage.Warnings {
	if c.set != nil {
		return c.set.Warnings()
	}
	return nil
}
