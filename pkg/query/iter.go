// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// promSeriesSet implements the SeriesSet interface of the Prometheus storage
// package on top of our storepb SeriesSet.
type promSeriesSet struct {
	set  storepb.SeriesSet
	done bool

	mint, maxt int64
	aggrs      []storepb.Aggr
	initiated  bool

	currLset   labels.Labels
	currChunks []storepb.AggrChunk

	warns storage.Warnings
}

func (s *promSeriesSet) Next() bool {
	if !s.initiated {
		s.initiated = true
		s.done = s.set.Next()
	}

	if !s.done {
		return false
	}

	// storage.Series are more strict then SeriesSet:
	// * It requires storage.Series to iterate over full series.
	s.currLset, s.currChunks = s.set.At()
	for {
		s.done = s.set.Next()
		if !s.done {
			break
		}
		nextLset, nextChunks := s.set.At()
		if labels.Compare(s.currLset, nextLset) != 0 {
			break
		}
		s.currChunks = append(s.currChunks, nextChunks...)
	}

	// Samples (so chunks as well) have to be sorted by time.
	// TODO(bwplotka): Benchmark if we can do better.
	// For example we could iterate in above loop and write our own binary search based insert sort.
	// We could also remove duplicates in same loop.
	sort.Slice(s.currChunks, func(i, j int) bool {
		return s.currChunks[i].MinTime < s.currChunks[j].MinTime
	})

	// Proxy handles duplicates between different series, let's handle duplicates within single series now as well.
	// We don't need to decode those.
	s.currChunks = removeExactDuplicates(s.currChunks)
	return true
}

// removeExactDuplicates returns chunks without 1:1 duplicates.
// NOTE: input chunks has to be sorted by minTime.
func removeExactDuplicates(chks []storepb.AggrChunk) []storepb.AggrChunk {
	if len(chks) <= 1 {
		return chks
	}

	ret := make([]storepb.AggrChunk, 0, len(chks))
	ret = append(ret, chks[0])

	for _, c := range chks[1:] {
		if ret[len(ret)-1].Compare(c) == 0 {
			continue
		}
		ret = append(ret, c)
	}
	return ret
}

func (s *promSeriesSet) At() storage.Series {
	if !s.initiated || s.set.Err() != nil {
		return nil
	}
	return newChunkSeries(s.currLset, s.currChunks, s.mint, s.maxt, s.aggrs)
}

func (s *promSeriesSet) Err() error {
	return s.set.Err()
}

func (s *promSeriesSet) Warnings() storage.Warnings {
	return s.warns
}

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

// chunkSeries implements storage.Series for a series on storepb types.
type chunkSeries struct {
	lset       labels.Labels
	chunks     []storepb.AggrChunk
	mint, maxt int64
	aggrs      []storepb.Aggr
}

// newChunkSeries allows to iterate over samples for each sorted and non-overlapped chunks.
func newChunkSeries(lset labels.Labels, chunks []storepb.AggrChunk, mint, maxt int64, aggrs []storepb.Aggr) *chunkSeries {
	return &chunkSeries{
		lset:   lset,
		chunks: chunks,
		mint:   mint,
		maxt:   maxt,
		aggrs:  aggrs,
	}
}

func (s *chunkSeries) Labels() labels.Labels {
	return s.lset
}

func (s *chunkSeries) Iterator() chunkenc.Iterator {
	var sit chunkenc.Iterator
	its := make([]chunkenc.Iterator, 0, len(s.chunks))

	if len(s.aggrs) == 1 {
		switch s.aggrs[0] {
		case storepb.Aggr_COUNT:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Count, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case storepb.Aggr_SUM:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Sum, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case storepb.Aggr_MIN:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Min, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case storepb.Aggr_MAX:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Max, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case storepb.Aggr_COUNTER:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Counter, c.Raw))
			}
			// TODO(bwplotka): This breaks resets function. See https://github.com/thanos-io/thanos/issues/3644
			sit = downsample.NewApplyCounterResetsIterator(its...)
		default:
			return errSeriesIterator{err: errors.Errorf("unexpected result aggregate type %v", s.aggrs)}
		}
		return dedup.NewBoundedSeriesIterator(sit, s.mint, s.maxt)
	}

	if len(s.aggrs) != 2 {
		return errSeriesIterator{err: errors.Errorf("unexpected result aggregate type %v", s.aggrs)}
	}

	switch {
	case s.aggrs[0] == storepb.Aggr_SUM && s.aggrs[1] == storepb.Aggr_COUNT,
		s.aggrs[0] == storepb.Aggr_COUNT && s.aggrs[1] == storepb.Aggr_SUM:

		for _, c := range s.chunks {
			if c.Raw != nil {
				its = append(its, getFirstIterator(c.Raw))
			} else {
				sum, cnt := getFirstIterator(c.Sum), getFirstIterator(c.Count)
				its = append(its, downsample.NewAverageChunkIterator(cnt, sum))
			}
		}
		sit = newChunkSeriesIterator(its)
	default:
		return errSeriesIterator{err: errors.Errorf("unexpected result aggregate type %v", s.aggrs)}
	}
	return dedup.NewBoundedSeriesIterator(sit, s.mint, s.maxt)
}

func getFirstIterator(cs ...*storepb.Chunk) chunkenc.Iterator {
	for _, c := range cs {
		if c == nil {
			continue
		}
		chk, err := chunkenc.FromData(chunkEncoding(c.Type), c.Data)
		if err != nil {
			return errSeriesIterator{err}
		}
		return chk.Iterator(nil)
	}
	return errSeriesIterator{errors.New("no valid chunk found")}
}

func chunkEncoding(e storepb.Chunk_Encoding) chunkenc.Encoding {
	switch e {
	case storepb.Chunk_XOR:
		return chunkenc.EncXOR
	}
	return 255 // Invalid.
}

type errSeriesIterator struct {
	err error
}

func (errSeriesIterator) Seek(int64) bool      { return false }
func (errSeriesIterator) Next() bool           { return false }
func (errSeriesIterator) At() (int64, float64) { return 0, 0 }
func (it errSeriesIterator) Err() error        { return it.err }

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type chunkSeriesIterator struct {
	chunks []chunkenc.Iterator
	i      int
}

func newChunkSeriesIterator(cs []chunkenc.Iterator) chunkenc.Iterator {
	if len(cs) == 0 {
		// This should not happen. StoreAPI implementations should not send empty results.
		return errSeriesIterator{err: errors.Errorf("store returned an empty result")}
	}
	return &chunkSeriesIterator{chunks: cs}
}

func (it *chunkSeriesIterator) Seek(t int64) (ok bool) {
	// We generally expect the chunks already to be cut down
	// to the range we are interested in. There's not much to be gained from
	// hopping across chunks so we just call next until we reach t.
	for {
		ct, _ := it.At()
		if ct >= t {
			return true
		}
		if !it.Next() {
			return false
		}
	}
}

func (it *chunkSeriesIterator) At() (t int64, v float64) {
	return it.chunks[it.i].At()
}

func (it *chunkSeriesIterator) Next() bool {
	lastT, _ := it.At()

	if it.chunks[it.i].Next() {
		return true
	}
	if it.Err() != nil {
		return false
	}
	if it.i >= len(it.chunks)-1 {
		return false
	}
	// Chunks are guaranteed to be ordered but not generally guaranteed to not overlap.
	// We must ensure to skip any overlapping range between adjacent chunks.
	it.i++
	return it.Seek(lastT + 1)
}

func (it *chunkSeriesIterator) Err() error {
	return it.chunks[it.i].Err()
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
