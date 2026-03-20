// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dedup

import (
	"bytes"
	"container/heap"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// NewChunkSeriesMerger merges several chunk series into one.
// Deduplication is based on penalty based deduplication algorithm without handling counter reset.
func NewChunkSeriesMerger() storage.VerticalChunkSeriesMergeFunc {
	return func(series ...storage.ChunkSeries) storage.ChunkSeries {
		if len(series) == 0 {
			return nil
		}
		return &storage.ChunkSeriesEntry{
			Lset: series[0].Labels(),
			ChunkIteratorFn: func(iterator chunks.Iterator) chunks.Iterator {
				iterators := make([]chunks.Iterator, 0, len(series))
				for _, s := range series {
					iterators = append(iterators, s.Iterator(nil))
				}
				return &dedupChunksIterator{
					iterators: iterators,
				}
			},
		}
	}
}

type dedupChunksIterator struct {
	iterators []chunks.Iterator
	h         chunkIteratorHeap

	err  error
	curr chunks.Meta
}

func (d *dedupChunksIterator) At() chunks.Meta {
	return d.curr
}

// Next method is almost the same as https://github.com/prometheus/prometheus/blob/v2.27.1/storage/merge.go#L615.
// The difference is that it handles both XOR/Histogram/FloatHistogram and Aggr chunk Encoding.
func (d *dedupChunksIterator) Next() bool {
	if d.h == nil {
		for _, iter := range d.iterators {
			if iter.Next() {
				heap.Push(&d.h, iter)
			}
		}
	}
	if len(d.h) == 0 {
		return false
	}

	iter := heap.Pop(&d.h).(chunks.Iterator)
	d.curr = iter.At()
	if iter.Next() {
		heap.Push(&d.h, iter)
	}

	var (
		om       = newOverlappingMerger()
		oMaxTime = d.curr.MaxTime
		prev     = d.curr
	)

	// Detect overlaps to compact.
	for len(d.h) > 0 {
		// Get the next oldest chunk by min, then max time.
		next := d.h[0].At()
		if next.MinTime > oMaxTime {
			// No overlap with current one.
			break
		}

		if next.MinTime == prev.MinTime &&
			next.MaxTime == prev.MaxTime &&
			bytes.Equal(next.Chunk.Bytes(), prev.Chunk.Bytes()) {
			// 1:1 duplicates, skip it.
		} else {
			// We operate on same series, so labels does not matter here.
			om.addChunk(next)

			if next.MaxTime > oMaxTime {
				oMaxTime = next.MaxTime
			}
			prev = next
		}

		iter := heap.Pop(&d.h).(chunks.Iterator)
		if iter.Next() {
			heap.Push(&d.h, iter)
		}
	}
	if om.empty() {
		return true
	}

	iter = om.iterator(d.curr)
	if !iter.Next() {
		if d.err = iter.Err(); d.err != nil {
			return false
		}
		panic("unexpected seriesToChunkEncoder lack of iterations")
	}
	d.curr = iter.At()
	if iter.Next() {
		heap.Push(&d.h, iter)
	}
	return true
}

func (d *dedupChunksIterator) Err() error {
	return d.err
}

type chunkIteratorHeap []chunks.Iterator

func (h chunkIteratorHeap) Len() int      { return len(h) }
func (h chunkIteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h chunkIteratorHeap) Less(i, j int) bool {
	at := h[i].At()
	bt := h[j].At()
	if at.MinTime == bt.MinTime {
		return at.MaxTime < bt.MaxTime
	}
	return at.MinTime < bt.MinTime
}

func (h *chunkIteratorHeap) Push(x any) {
	*h = append(*h, x.(chunks.Iterator))
}

func (h *chunkIteratorHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type overlappingMerger struct {
	xorIterators       []chunkenc.Iterator
	histIterators      []chunkenc.Iterator
	floatHistIterators []chunkenc.Iterator
	aggrIterators      [5][]chunkenc.Iterator

	samplesMergeFunc func(a, b chunkenc.Iterator) chunkenc.Iterator
}

func newOverlappingMerger() *overlappingMerger {
	return &overlappingMerger{
		samplesMergeFunc: func(a, b chunkenc.Iterator) chunkenc.Iterator {
			return newDedupSeriesIterator(
				noopAdjustableSeriesIterator{a},
				noopAdjustableSeriesIterator{b},
			)
		},
	}
}

func (o *overlappingMerger) addChunk(chk chunks.Meta) {
	switch chk.Chunk.Encoding() {
	case chunkenc.EncXOR:
		o.xorIterators = append(o.xorIterators, chk.Chunk.Iterator(nil))
	case chunkenc.EncFloatHistogram:
		o.floatHistIterators = append(o.floatHistIterators, chk.Chunk.Iterator(nil))
	case chunkenc.EncHistogram:
		o.histIterators = append(o.histIterators, chk.Chunk.Iterator(nil))
	case downsample.ChunkEncAggr:
		aggrChk := chk.Chunk.(*downsample.AggrChunk)
		for i := downsample.AggrCount; i <= downsample.AggrCounter; i++ {
			if c, err := aggrChk.Get(i); err == nil {
				o.aggrIterators[i] = append(o.aggrIterators[i], c.Iterator(nil))
			}
		}
	case chunkenc.EncNone:
	default:
		// exhausted options for chunk
		return
	}
}

func (o *overlappingMerger) empty() bool {
	if len(o.xorIterators) > 0 || len(o.histIterators) > 0 || len(o.floatHistIterators) > 0 {
		return false
	}
	return len(o.aggrIterators[downsample.AggrCount]) == 0
}

// Return a chunk iterator based on the encoding of base chunk.
func (o *overlappingMerger) iterator(baseChk chunks.Meta) chunks.Iterator {
	var it chunkenc.Iterator
	switch baseChk.Chunk.Encoding() {
	case chunkenc.EncXOR:
		// If XOR encoding, we need to deduplicate the samples and re-encode them to chunks.
		return storage.NewSeriesToChunkEncoder(&storage.SeriesEntry{
			SampleIteratorFn: func(_ chunkenc.Iterator) chunkenc.Iterator {
				it = baseChk.Chunk.Iterator(nil)
				for _, i := range o.xorIterators {
					it = o.samplesMergeFunc(it, i)
				}
				return it
			}}).Iterator(nil)

	case chunkenc.EncHistogram:
		return storage.NewSeriesToChunkEncoder(&storage.SeriesEntry{
			SampleIteratorFn: func(_ chunkenc.Iterator) chunkenc.Iterator {
				it = baseChk.Chunk.Iterator(nil)
				for _, i := range o.histIterators {
					it = o.samplesMergeFunc(it, i)
				}
				return it
			},
		}).Iterator(nil)

	case chunkenc.EncFloatHistogram:
		return storage.NewSeriesToChunkEncoder(&storage.SeriesEntry{
			SampleIteratorFn: func(_ chunkenc.Iterator) chunkenc.Iterator {
				it = baseChk.Chunk.Iterator(nil)
				for _, i := range o.floatHistIterators {
					it = o.samplesMergeFunc(it, i)
				}
				return it
			},
		}).Iterator(nil)

	case downsample.ChunkEncAggr:
		// If Aggr encoding, each aggregated chunks need to be expanded and deduplicated,
		// then re-encoded into Aggr chunks.
		aggrChk := baseChk.Chunk.(*downsample.AggrChunk)
		samplesIter := [5]chunkenc.Iterator{}
		for i := downsample.AggrCount; i <= downsample.AggrCounter; i++ {
			if c, err := aggrChk.Get(i); err == nil {
				o.aggrIterators[i] = append(o.aggrIterators[i], c.Iterator(nil))
			}

			if len(o.aggrIterators[i]) > 0 {
				for _, j := range o.aggrIterators[i][1:] {
					o.aggrIterators[i][0] = o.samplesMergeFunc(o.aggrIterators[i][0], j)
				}
				samplesIter[i] = o.aggrIterators[i][0]
			} else {
				samplesIter[i] = nil
			}
		}

		return newAggrChunkIterator(samplesIter)
	default:
		return nil
	}
}

type aggrChunkIterator struct {
	iters        [5]chunkenc.Iterator
	curr         chunks.Meta
	countChkIter chunks.Iterator
	reuseIter    chunkenc.Iterator

	err error
}

func newAggrChunkIterator(iters [5]chunkenc.Iterator) chunks.Iterator {
	return &aggrChunkIterator{
		iters: iters,
		countChkIter: storage.NewSeriesToChunkEncoder(&storage.SeriesEntry{
			SampleIteratorFn: func(_ chunkenc.Iterator) chunkenc.Iterator {
				return iters[downsample.AggrCount]
			},
		}).Iterator(nil),
	}
}

func (a *aggrChunkIterator) Next() bool {
	if !a.countChkIter.Next() {
		if err := a.countChkIter.Err(); err != nil {
			a.err = err
		}
		return false
	}

	countChk := a.countChkIter.At()
	mint := countChk.MinTime
	maxt := countChk.MaxTime

	var (
		chks [5]chunkenc.Chunk
		chk  *chunks.Meta
		err  error
	)

	chks[downsample.AggrCount] = countChk.Chunk
	for i := downsample.AggrSum; i <= downsample.AggrCounter; i++ {
		chk, a.reuseIter, err = a.toChunk(a.reuseIter, i, mint, maxt)
		if err != nil {
			a.err = err
			return false
		}
		if chk != nil {
			chks[i] = chk.Chunk
		}
	}

	a.curr = chunks.Meta{
		MinTime: mint,
		MaxTime: maxt,
		Chunk:   downsample.EncodeAggrChunk(chks),
	}
	return true
}

func (a *aggrChunkIterator) At() chunks.Meta {
	return a.curr
}

func (a *aggrChunkIterator) Err() error {
	return a.err
}

func (a *aggrChunkIterator) toChunk(it chunkenc.Iterator, at downsample.AggrType, minTime, maxTime int64) (*chunks.Meta, chunkenc.Iterator, error) {
	if a.iters[at] == nil {
		return nil, it, nil
	}
	c := chunkenc.NewXORChunk()
	appender, err := c.Appender()
	if err != nil {
		return nil, it, err
	}

	it = NewBoundedSeriesIterator(it, a.iters[at], minTime, maxTime)

	var (
		lastT int64
		lastV float64
	)
	for it.Next() != chunkenc.ValNone {
		lastT, lastV = it.At()
		appender.Append(lastT, lastV)
	}
	if err := it.Err(); err != nil {
		return nil, it, err
	}

	// No sample in the required time range.
	if lastT == 0 && lastV == 0 {
		return nil, it, nil
	}

	// Encode last sample for AggrCounter.
	if at == downsample.AggrCounter {
		appender.Append(lastT, lastV)
	}

	return &chunks.Meta{
		MinTime: minTime,
		MaxTime: maxTime,
		Chunk:   c,
	}, it, nil
}
