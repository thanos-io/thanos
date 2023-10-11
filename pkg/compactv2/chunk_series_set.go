// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compactv2

import (
	"context"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/pkg/block"
)

type lazyPopulateChunkSeriesSet struct {
	sReader seriesReader

	all index.Postings

	bufChks []chunks.Meta
	bufLbls labels.ScratchBuilder

	curr *storage.ChunkSeriesEntry
	err  error
}

func newLazyPopulateChunkSeriesSet(sReader seriesReader, all index.Postings) *lazyPopulateChunkSeriesSet {
	return &lazyPopulateChunkSeriesSet{sReader: sReader, all: all}
}

func (s *lazyPopulateChunkSeriesSet) Next() bool {
	for s.all.Next() {
		if err := s.sReader.ir.Series(s.all.At(), &s.bufLbls, &s.bufChks); err != nil {
			// Postings may be stale. Skip if no underlying series exists.
			if errors.Cause(err) == storage.ErrNotFound {
				continue
			}
			s.err = errors.Wrapf(err, "get series %d", s.all.At())
			return false
		}

		if len(s.bufChks) == 0 {
			continue
		}

		for i := range s.bufChks {
			s.bufChks[i].Chunk = &lazyPopulatableChunk{cr: s.sReader.cr, m: &s.bufChks[i]}
		}
		s.curr = &storage.ChunkSeriesEntry{
			Lset: s.bufLbls.Labels(),
			ChunkIteratorFn: func(_ chunks.Iterator) chunks.Iterator {
				return storage.NewListChunkSeriesIterator(s.bufChks...)
			},
		}
		return true
	}
	return false
}

func (s *lazyPopulateChunkSeriesSet) At() storage.ChunkSeries {
	return s.curr
}

func (s *lazyPopulateChunkSeriesSet) Err() error {
	if s.err != nil {
		return s.err
	}
	return s.all.Err()
}

func (s *lazyPopulateChunkSeriesSet) Warnings() annotations.Annotations { return nil }

type lazyPopulatableChunk struct {
	m *chunks.Meta

	cr tsdb.ChunkReader

	populated chunkenc.Chunk
}

type errChunkIterator struct{ err error }

func (e errChunkIterator) Seek(int64) chunkenc.ValueType { return chunkenc.ValNone }
func (e errChunkIterator) At() (int64, float64)          { return 0, 0 }

// TODO(rabenhorst): Needs to be implemented for native histogram support.
func (e errChunkIterator) AtHistogram() (int64, *histogram.Histogram) { panic("not implemented") }
func (e errChunkIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	panic("not implemented")
}
func (e errChunkIterator) AtT() int64               { return 0 }
func (e errChunkIterator) Next() chunkenc.ValueType { return chunkenc.ValNone }
func (e errChunkIterator) Err() error               { return e.err }

type errChunk struct{ err errChunkIterator }

func (e errChunk) Bytes() []byte                                { return nil }
func (e errChunk) Encoding() chunkenc.Encoding                  { return chunkenc.EncXOR }
func (e errChunk) Appender() (chunkenc.Appender, error)         { return nil, e.err.err }
func (e errChunk) Iterator(chunkenc.Iterator) chunkenc.Iterator { return e.err }
func (e errChunk) NumSamples() int                              { return 0 }
func (e errChunk) Compact()                                     {}

func (l *lazyPopulatableChunk) populate() {
	// TODO(bwplotka): In most cases we don't need to parse anything, just copy. Extend reader/writer for this.
	var err error
	l.populated, err = l.cr.Chunk(*l.m)
	if err != nil {
		l.m.Chunk = errChunk{err: errChunkIterator{err: errors.Wrapf(err, "cannot populate chunk %d", l.m.Ref)}}
		return
	}

	l.m.Chunk = l.populated
}

func (l *lazyPopulatableChunk) Bytes() []byte {
	if l.populated == nil {
		l.populate()
	}
	return l.populated.Bytes()
}

func (l *lazyPopulatableChunk) Encoding() chunkenc.Encoding {
	if l.populated == nil {
		l.populate()
	}
	return l.populated.Encoding()
}

func (l *lazyPopulatableChunk) Appender() (chunkenc.Appender, error) {
	if l.populated == nil {
		l.populate()
	}
	return l.populated.Appender()
}

func (l *lazyPopulatableChunk) Iterator(iterator chunkenc.Iterator) chunkenc.Iterator {
	if l.populated == nil {
		l.populate()
	}
	return l.populated.Iterator(iterator)
}

func (l *lazyPopulatableChunk) NumSamples() int {
	if l.populated == nil {
		l.populate()
	}
	return l.populated.NumSamples()
}

func (l *lazyPopulatableChunk) Compact() {
	if l.populated == nil {
		l.populate()
	}
	l.populated.Compact()
}

func (w *Compactor) write(ctx context.Context, symbols index.StringIter, populatedSet storage.ChunkSeriesSet, sWriter block.SeriesWriter, p ProgressLogger) error {
	var (
		chks []chunks.Meta
		ref  storage.SeriesRef
	)

	for symbols.Next() {
		if err := sWriter.AddSymbol(symbols.At()); err != nil {
			return errors.Wrap(err, "add symbol")
		}
	}
	if err := symbols.Err(); err != nil {
		return errors.Wrap(err, "symbols")
	}

	// Iterate over all sorted chunk series.
	for populatedSet.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		s := populatedSet.At()
		chksIter := s.Iterator(nil)
		chks = chks[:0]
		for chksIter.Next() {
			// We are not iterating in streaming way over chunk as it's more efficient to do bulk write for index and
			// chunk file purposes.
			chks = append(chks, chksIter.At())
		}

		if chksIter.Err() != nil {
			return errors.Wrap(chksIter.Err(), "chunk iter")
		}

		// Skip the series with all deleted chunks.
		if len(chks) == 0 {
			// All series will be ignored.
			p.SeriesProcessed()
			continue
		}

		if err := sWriter.WriteChunks(chks...); err != nil {
			return errors.Wrap(err, "write chunks")
		}
		if err := sWriter.AddSeries(ref, s.Labels(), chks...); err != nil {
			return errors.Wrap(err, "add series")
		}
		for _, chk := range chks {
			// ChunkPool is used by tsdb.OpenBlock BlockReader.
			if err := w.chunkPool.Put(chk.Chunk); err != nil {
				return errors.Wrap(err, "put chunk")
			}
		}
		ref++
		p.SeriesProcessed()
	}
	if populatedSet.Err() != nil {
		return errors.Wrap(populatedSet.Err(), "iterate populated chunk series set")
	}

	return nil
}
