// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compactv2

import (
	"math"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type Modifier interface {
	Modify(sym index.StringIter, set storage.ChunkSeriesSet, log ChangeLogger, p ProgressLogger) (index.StringIter, storage.ChunkSeriesSet)
}

type DeletionModifier struct {
	deletions []metadata.DeletionRequest
}

func WithDeletionModifier(deletions ...metadata.DeletionRequest) *DeletionModifier {
	return &DeletionModifier{deletions: deletions}
}

func (d *DeletionModifier) Modify(sym index.StringIter, set storage.ChunkSeriesSet, log ChangeLogger, p ProgressLogger) (index.StringIter, storage.ChunkSeriesSet) {
	// TODO(bwplotka): Modify symbols as well. Otherwise large string will be kept forever.
	// This is however what Prometheus already does. It does not increase index size too much though.
	// This needs a bit of work due to sorting and tracking required to rebuild them.pp

	return sym, &delModifierSeriesSet{
		d: d,

		ChunkSeriesSet: set,
		log:            log,
		p:              p,
	}
}

type delModifierSeriesSet struct {
	storage.ChunkSeriesSet

	d   *DeletionModifier
	log ChangeLogger
	p   ProgressLogger

	curr *storage.ChunkSeriesEntry
	err  error
}

func (d *delModifierSeriesSet) Next() bool {
SeriesLoop:
	for d.ChunkSeriesSet.Next() {
		s := d.ChunkSeriesSet.At()
		lbls := s.Labels()

		var intervals tombstones.Intervals
	DeletionsLoop:
		for _, deletions := range d.d.deletions {
			for _, m := range deletions.Matchers {
				v := lbls.Get(m.Name)

				// Only if all matchers in the deletion request are matched can we proceed to deletion.
				if v == "" || !m.Matches(v) {
					continue DeletionsLoop
				}
			}
			if len(deletions.Intervals) > 0 {
				for _, in := range deletions.Intervals {
					intervals = intervals.Add(in)
				}
				continue
			}

			// Special case: Delete whole series.
			chksIter := s.Iterator()
			var chks []chunks.Meta
			for chksIter.Next() {
				chks = append(chks, chksIter.At())
			}
			if d.err = chksIter.Err(); d.err != nil {
				return false
			}

			var deleted tombstones.Intervals
			if len(chks) > 0 {
				deleted = deleted.Add(tombstones.Interval{Mint: chks[0].MinTime, Maxt: chks[len(chks)-1].MaxTime})
			}
			d.log.DeleteSeries(lbls, deleted)
			d.p.SeriesProcessed()
			continue SeriesLoop
		}

		d.curr = &storage.ChunkSeriesEntry{
			Lset: lbls,
			ChunkIteratorFn: func() chunks.Iterator {
				return NewDelGenericSeriesIterator(s.Iterator(), intervals, func(intervals tombstones.Intervals) {
					d.log.DeleteSeries(lbls, intervals)
				}).ToChunkSeriesIterator()
			},
		}
		return true
	}
	return false
}

// intersection returns intersection between interval and range of intervals.
func intersection(i tombstones.Interval, dranges tombstones.Intervals) tombstones.Intervals {
	var ret tombstones.Intervals
	for _, r := range dranges {
		isLeftIn := r.Mint <= i.Maxt
		isRightIn := i.Mint <= r.Maxt
		if !isLeftIn || !isRightIn {
			continue
		}
		intersection := tombstones.Interval{Mint: r.Mint, Maxt: r.Maxt}
		if intersection.Mint < i.Mint {
			intersection.Mint = i.Mint
		}
		if intersection.Maxt > i.Maxt {
			intersection.Maxt = i.Maxt
		}
		ret = ret.Add(intersection)
	}
	return ret
}

func (d *delModifierSeriesSet) At() storage.ChunkSeries {
	return d.curr
}

func (d *delModifierSeriesSet) Err() error {
	if d.err != nil {
		return d.err
	}
	return d.ChunkSeriesSet.Err()
}

func (d *delModifierSeriesSet) Warnings() storage.Warnings {
	return d.ChunkSeriesSet.Warnings()
}

type delGenericSeriesIterator struct {
	chks chunks.Iterator

	err       error
	bufIter   *tsdb.DeletedIterator
	intervals tombstones.Intervals

	currDelIter chunkenc.Iterator
	currChkMeta chunks.Meta
	logDelete   func(intervals tombstones.Intervals)
	deleted     tombstones.Intervals
}

func NewDelGenericSeriesIterator(
	chks chunks.Iterator,
	intervals tombstones.Intervals,
	logDelete func(intervals tombstones.Intervals),
) *delGenericSeriesIterator {
	return &delGenericSeriesIterator{
		chks:      chks,
		bufIter:   &tsdb.DeletedIterator{},
		intervals: intervals,
		logDelete: logDelete,
	}
}

func (d *delGenericSeriesIterator) next() (ok bool) {
	if d.err != nil {
		return false
	}

	for d.chks.Next() {
		d.currChkMeta = d.chks.At()

		if chk := (tombstones.Interval{Mint: d.currChkMeta.MinTime, Maxt: d.currChkMeta.MaxTime}); chk.IsSubrange(d.intervals) {
			d.deleted = d.deleted.Add(chk)
			continue
		}
		d.bufIter.Intervals = d.bufIter.Intervals[:0]
		for _, interval := range d.intervals {
			if d.currChkMeta.OverlapsClosedInterval(interval.Mint, interval.Maxt) {
				d.bufIter.Intervals = d.bufIter.Intervals.Add(interval)
			}
		}
		if len(d.bufIter.Intervals) == 0 {
			d.currDelIter = nil
			return true
		}

		for _, del := range intersection(tombstones.Interval{Mint: d.currChkMeta.MinTime, Maxt: d.currChkMeta.MaxTime}, d.bufIter.Intervals) {
			d.deleted = d.deleted.Add(del)
		}

		// We don't want full chunk, take just part of it.
		d.bufIter.Iter = d.currChkMeta.Chunk.Iterator(nil)
		d.currDelIter = d.bufIter
		return true
	}
	if len(d.deleted) > 0 {
		d.logDelete(d.deleted)
	}
	return false
}

func (d *delGenericSeriesIterator) Err() error {
	if d.err != nil {
		return d.err
	}
	return d.chks.Err()
}

func (d *delGenericSeriesIterator) ToSeriesIterator() chunkenc.Iterator {
	return &delSeriesIterator{delGenericSeriesIterator: d}
}
func (d *delGenericSeriesIterator) ToChunkSeriesIterator() chunks.Iterator {
	return &delChunkSeriesIterator{delGenericSeriesIterator: d}
}

// delSeriesIterator allows to iterate over samples for the single series.
type delSeriesIterator struct {
	*delGenericSeriesIterator

	curr chunkenc.Iterator
}

func (p *delSeriesIterator) Next() bool {
	if p.curr != nil && p.curr.Next() {
		return true
	}

	for p.next() {
		if p.currDelIter != nil {
			p.curr = p.currDelIter
		} else {
			p.curr = p.currChkMeta.Chunk.Iterator(nil)
		}
		if p.curr.Next() {
			return true
		}
	}
	return false
}

func (p *delSeriesIterator) Seek(t int64) bool {
	if p.curr != nil && p.curr.Seek(t) {
		return true
	}
	for p.Next() {
		if p.curr.Seek(t) {
			return true
		}
	}
	return false
}

func (p *delSeriesIterator) At() (int64, float64) { return p.curr.At() }

func (p *delSeriesIterator) Err() error {
	if err := p.delGenericSeriesIterator.Err(); err != nil {
		return err
	}
	if p.curr != nil {
		return p.curr.Err()
	}
	return nil
}

type delChunkSeriesIterator struct {
	*delGenericSeriesIterator

	curr chunks.Meta
}

func (p *delChunkSeriesIterator) Next() bool {
	if !p.next() {
		return false
	}

	p.curr = p.currChkMeta
	if p.currDelIter == nil {
		return true
	}

	// Re-encode the chunk if iterator is provider. This means that it has some samples to be deleted or chunk is opened.
	newChunk := chunkenc.NewXORChunk()
	app, err := newChunk.Appender()
	if err != nil {
		p.err = err
		return false
	}

	if !p.currDelIter.Next() {
		if err := p.currDelIter.Err(); err != nil {
			p.err = errors.Wrap(err, "iterate chunk while re-encoding")
			return false
		}

		// Empty chunk, this should not happen, as we assume full deletions being filtered before this iterator.
		p.err = errors.Wrap(err, "populateWithDelChunkSeriesIterator: unexpected empty chunk found while rewriting chunk")
		return false
	}

	t, v := p.currDelIter.At()
	p.curr.MinTime = t
	app.Append(t, v)

	for p.currDelIter.Next() {
		t, v = p.currDelIter.At()
		app.Append(t, v)
	}
	if err := p.currDelIter.Err(); err != nil {
		p.err = errors.Wrap(err, "iterate chunk while re-encoding")
		return false
	}

	p.curr.Chunk = newChunk
	p.curr.MaxTime = t
	return true
}

func (p *delChunkSeriesIterator) At() chunks.Meta { return p.curr }

type RelabelModifier struct {
	relabels []*relabel.Config
}

func WithRelabelModifier(relabels ...*relabel.Config) *RelabelModifier {
	return &RelabelModifier{relabels: relabels}
}

func (d *RelabelModifier) Modify(_ index.StringIter, set storage.ChunkSeriesSet, log ChangeLogger, p ProgressLogger) (index.StringIter, storage.ChunkSeriesSet) {
	// Gather symbols.
	symbols := make(map[string]struct{})
	chunkSeriesMap := make(map[string]*mergeChunkSeries)

	for set.Next() {
		s := set.At()
		lbls := s.Labels()
		chksIter := s.Iterator()

		// The labels have to be copied because `relabel.Process` is now overwriting the original
		// labels to same memory. This happens since Prometheus v2.39.0.
		if processedLabels := relabel.Process(lbls.Copy(), d.relabels...); len(processedLabels) == 0 {
			// Special case: Delete whole series if no labels are present.
			var (
				minT int64 = math.MaxInt64
				maxT int64 = math.MinInt64
			)
			for chksIter.Next() {
				c := chksIter.At()
				if c.MinTime < minT {
					minT = c.MinTime
				}
				if c.MaxTime > maxT {
					maxT = c.MaxTime
				}
			}

			if err := chksIter.Err(); err != nil {
				return errorOnlyStringIter{err: err}, nil
			}

			var deleted tombstones.Intervals
			// If minTime is set then there is at least one chunk.
			if minT != math.MaxInt64 {
				deleted = deleted.Add(tombstones.Interval{Mint: minT, Maxt: maxT})
			}
			log.DeleteSeries(lbls, deleted)
			p.SeriesProcessed()
		} else {
			for _, lb := range processedLabels {
				symbols[lb.Name] = struct{}{}
				symbols[lb.Value] = struct{}{}
			}

			lbStr := processedLabels.String()
			if _, ok := chunkSeriesMap[lbStr]; !ok {
				chunkSeriesMap[lbStr] = newChunkSeriesBuilder(processedLabels)
			}
			cs := chunkSeriesMap[lbStr]

			// We have to iterate over the chunks and populate them here as
			// lazyPopulateChunkSeriesSet reuses chunks and previous chunks
			// will be overwritten at set.Next() call.
			for chksIter.Next() {
				c := chksIter.At()
				cs.addIter(c.Chunk.Iterator(nil))
			}
			if err := chksIter.Err(); err != nil {
				return errorOnlyStringIter{err}, nil
			}

			if !labels.Equal(lbls, processedLabels) {
				log.ModifySeries(lbls, processedLabels)
			}
		}
	}

	symbolsSlice := make([]string, 0, len(symbols))
	for s := range symbols {
		symbolsSlice = append(symbolsSlice, s)
	}
	sort.Strings(symbolsSlice)

	chunkSeriesSet := make([]storage.ChunkSeries, 0, len(chunkSeriesMap))
	for _, chunkSeries := range chunkSeriesMap {
		chunkSeriesSet = append(chunkSeriesSet, chunkSeries)
	}
	sort.Slice(chunkSeriesSet, func(i, j int) bool {
		return labels.Compare(chunkSeriesSet[i].Labels(), chunkSeriesSet[j].Labels()) < 0
	})
	return index.NewStringListIter(symbolsSlice), newListChunkSeriesSet(chunkSeriesSet...)
}

// mergeChunkSeries build storage.ChunkSeries from several chunkenc.Iterator.
type mergeChunkSeries struct {
	lset labels.Labels
	ss   []storage.Series
}

func newChunkSeriesBuilder(lset labels.Labels) *mergeChunkSeries {
	return &mergeChunkSeries{
		lset: lset,
		ss:   make([]storage.Series, 0),
	}
}

func (s *mergeChunkSeries) addIter(iter chunkenc.Iterator) {
	s.ss = append(s.ss, &storage.SeriesEntry{
		SampleIteratorFn: func() chunkenc.Iterator {
			return iter
		},
	})
}

func (s *mergeChunkSeries) Labels() labels.Labels {
	return s.lset
}

func (s *mergeChunkSeries) Iterator() chunks.Iterator {
	if len(s.ss) == 0 {
		return nil
	}
	if len(s.ss) == 1 {
		return storage.NewSeriesToChunkEncoder(s.ss[0]).Iterator()
	}

	return storage.NewSeriesToChunkEncoder(storage.ChainedSeriesMerge(s.ss...)).Iterator()
}

type errorOnlyStringIter struct {
	err error
}

func (errorOnlyStringIter) Next() bool   { return false }
func (errorOnlyStringIter) At() string   { return "" }
func (s errorOnlyStringIter) Err() error { return s.err }

type listChunkSeriesSet struct {
	css []storage.ChunkSeries
	idx int
}

func newListChunkSeriesSet(css ...storage.ChunkSeries) storage.ChunkSeriesSet {
	return &listChunkSeriesSet{css: css, idx: -1}
}

func (s *listChunkSeriesSet) Next() bool {
	s.idx++
	return s.idx < len(s.css)
}

func (s *listChunkSeriesSet) At() storage.ChunkSeries    { return s.css[s.idx] }
func (s *listChunkSeriesSet) Err() error                 { return nil }
func (s *listChunkSeriesSet) Warnings() storage.Warnings { return nil }
