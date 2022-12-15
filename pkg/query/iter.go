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
	set  storepb.SeriesSet // We assume we have full series by series messages without duplicates. This is assured by ProxyStore heap.
	done bool

	mint, maxt int64
	aggrs      []storepb.Aggr

	currLset labels.Labels
	curr     storage.Series

	warns storage.Warnings
	dedup bool
	err   error
}

func newPromSeriesSet(mint, maxt int64, set storepb.SeriesSet, aggrs []storepb.Aggr, warns storage.Warnings, dedup bool) *promSeriesSet {
	return &promSeriesSet{
		mint:  mint,
		maxt:  maxt,
		set:   set,
		aggrs: aggrs,
		warns: warns,
		dedup: dedup,
		done:  set.Next(),
	}
}

func (s *promSeriesSet) Next() bool {
	if s.done || s.Err() != nil {
		return false
	}

	var currChunks []storepb.AggrChunk
	// TODO(bwplotka): Assume it's done and remove...
	s.currLset, currChunks = s.set.At()
	for {
		s.done = s.set.Next()
		if !s.done {
			break
		}
		nextLset, nextChunks := s.set.At()
		if labels.Compare(s.currLset, nextLset) != 0 {
			break
		}
		currChunks = append(currChunks, nextChunks...)
	}

	// Samples (so chunks as well) have to be sorted by time.
	// TODO(bwplotka): Benchmark if we can do better.
	// For example, we could iterate in above loop and write our own binary search based insert sort.
	// We could also remove duplicates in same loop.
	sort.Slice(currChunks, func(i, j int) bool {
		return currChunks[i].MinTime < currChunks[j].MinTime
	})

	// TODO: To remove.
	// Proxy handles some exact duplicates in chunk between different series, let's handle duplicates within single series now as well.
	// We don't need to decode those.
	currChunks = removeExactDuplicates(currChunks)

	currChunkIters, err := aggrChunksToIterators(currChunks, s.aggrs)
	if err != nil {
		s.err = err
		return false
	}

	if s.dedup {
		dedup.NewChunkSeriesMerger

		// TODO(bwplotka): Add pushdown support.
		return dedup.NewDedupChunksSeries(s.currLset, s.currChunks, s.aggrs)
	}

	s.curr = &storage.SeriesEntry{
		Lset: s.currLset,
		SampleIteratorFn: func() chunkenc.Iterator {
			// TODO(bwplotka): Compare with storage.NewChainSampleIterator and benchmark.
			return dedup.NewBoundedSeriesIterator(newChunkSeriesIterator(currChunkIters), s.mint, s.maxt)
		},
	}
	return true
}

// removeExactDuplicates returns chunks without 1:1 duplicates.
// NOTE: input chunks has to be sorted by minTime.
func removeExactDuplicates(chks []storepb.AggrChunk) []storepb.AggrChunk {
	if len(chks) <= 1 {
		return chks
	}
	head := 0
	for i, c := range chks[1:] {
		if chks[head].Compare(c) == 0 {
			continue
		}
		head++
		if i+1 == head {
			// `chks[head] == chks[i+1] == c` so this is a no-op.
			// This way we get no copies in case the input had no duplicates.
			continue
		}
		chks[head] = c
	}
	return chks[:head+1]
}

func (s *promSeriesSet) At() storage.Series {
	if s.Err() != nil {
		return nil
	}

	return s.curr
}

func (s *promSeriesSet) Err() error {
	if s.err != nil {
		return s.err
	}
	return s.set.Err()
}

func (s *promSeriesSet) Warnings() storage.Warnings {
	return s.warns
}

// storeSeriesSet implements a storepb SeriesSet against a list of storepb.Series.
type storeSeriesSet struct {
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

func aggrChunkToIterator(c storepb.AggrChunk, aggrs storepb.Aggrs) chunkenc.Iterator {
	if len(aggrs) == 1 {
		switch aggrs[0] {
		case storepb.Aggr_COUNT:
			return getFirstIterator(c.Count, c.Raw)
		case storepb.Aggr_SUM:
			return getFirstIterator(c.Sum, c.Raw)
		case storepb.Aggr_MIN:
			return getFirstIterator(c.Min, c.Raw)
		case storepb.Aggr_MAX:
			return getFirstIterator(c.Max, c.Raw)
		case storepb.Aggr_COUNTER:
			return getFirstIterator(c.Counter, c.Raw)

			// TODO(bwplotka): This breaks resets function. See https://github.com/thanos-io/thanos/issues/3644
			//sit = downsample.NewApplyCounterResetsIterator(its...)
		default:
			return errSeriesIterator{errors.Errorf("unexpected result aggregate type %v", aggrs)}
		}
	}

	if len(aggrs) != 2 {
		return errSeriesIterator{errors.Errorf("unexpected result aggregate type %v", aggrs)}
	}

	switch {
	case aggrs[0] == storepb.Aggr_SUM && aggrs[1] == storepb.Aggr_COUNT,
		aggrs[0] == storepb.Aggr_COUNT && aggrs[1] == storepb.Aggr_SUM:
		if c.Raw != nil {
			return getFirstIterator(c.Raw)
		} else {
			sum, cnt := getFirstIterator(c.Sum), getFirstIterator(c.Count)
			return downsample.NewAverageChunkIterator(cnt, sum)
		}
	default:
		return errSeriesIterator{errors.Errorf("unexpected result aggregate type %v", aggrs)}
	}
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
// When overlapping chunks are passed, chunk series return random sample from set of overlapping chunks.
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
