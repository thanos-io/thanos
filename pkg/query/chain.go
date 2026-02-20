package query

import (
	"math"

	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type chainedPromSeriesSet struct {
	set storepb.SeriesSet

	mint, maxt int64
	aggrs      []storepb.Aggr

	warns annotations.Annotations
}

func NewChainedPromSeriesSet(seriesSet storepb.SeriesSet, mint, maxt int64, aggrs []storepb.Aggr, warns annotations.Annotations) storage.SeriesSet {
	return &chainedPromSeriesSet{
		set:   seriesSet,
		mint:  mint,
		maxt:  maxt,
		aggrs: aggrs,
		warns: warns,
	}
}

func (s *chainedPromSeriesSet) Next() bool {
	return s.set.Next()
}

func (s *chainedPromSeriesSet) At() storage.Series {
	if s.set.Err() != nil {
		return nil
	}

	currLset, currChunks := s.set.At()
	sections := splitSections(currChunks)
	return newChainedChunkSeries(currLset, sections, s.mint, s.maxt, s.aggrs)
}

func (s *chainedPromSeriesSet) Err() error {
	return s.set.Err()
}

func (s *chainedPromSeriesSet) Warnings() annotations.Annotations {
	return s.warns
}

type section interface {
	iterator(aggr storepb.Aggr) chunkenc.Iterator
}

type aggrSection struct {
	chunk storepb.AggrChunk
}

type rawSection struct {
	chunks []storepb.AggrChunk
	mint   int64
	maxt   int64
}

func (s *aggrSection) iterator(aggr storepb.Aggr) chunkenc.Iterator {
	switch aggr {
	case storepb.Aggr_COUNT:
		return getFirstIterator(s.chunk.Count)
	case storepb.Aggr_SUM:
		return getFirstIterator(s.chunk.Sum)
	case storepb.Aggr_MIN:
		return getFirstIterator(s.chunk.Min)
	case storepb.Aggr_MAX:
		return getFirstIterator(s.chunk.Max)
	case storepb.Aggr_COUNTER:
		return getFirstIterator(s.chunk.Counter)
	}
	return nil
}

func (s *rawSection) iterator(_ storepb.Aggr) chunkenc.Iterator {
	if len(s.chunks) == 1 {
		return dedup.NewBoundedSeriesIterator(getFirstIterator(s.chunks[0].Raw), s.mint, s.maxt)
	}

	its := make([]chunkenc.Iterator, 0, len(s.chunks))
	for _, r := range s.chunks {
		its = append(its, dedup.NewBoundedSeriesIterator(getFirstIterator(r.Raw), s.mint, s.maxt))
	}

	chained := storage.ChainSampleIteratorFromIterators(nil, its)
	// TODO better way to handle inconsistency between chained and chunkSeries iterators ?
	if v := chained.Next(); v == chunkenc.ValNone {
		return nil
	}
	return chained
}

func splitSections(chunks []storepb.AggrChunk) []section {
	var aggregated []storepb.AggrChunk
	var raw []storepb.AggrChunk

	for _, chk := range chunks {
		if chk.Raw == nil {
			aggregated = append(aggregated, chk)
		} else {
			raw = append(raw, chk)
		}
	}

	aggregated = removeOverlaps(aggregated)
	groupedRaw := groupOverlappingChunks(raw)

	var sections []section
	currentRaw := 0
	var lastAggrMax int64 = math.MinInt64
	for _, aggr := range aggregated {
		for currentRaw < len(groupedRaw) && groupedRaw[currentRaw].mint < aggr.MinTime {
			sectionStart := max(groupedRaw[currentRaw].mint, lastAggrMax)
			sectionEnd := min(groupedRaw[currentRaw].maxt, aggr.MinTime-1)

			if sectionStart < sectionEnd {
				sections = append(sections, &rawSection{
					chunks: groupedRaw[currentRaw].chunks,
					mint:   sectionStart,
					maxt:   sectionEnd,
				})
			}

			currentRaw++
		}

		sections = append(sections, &aggrSection{
			chunk: aggr,
		})
		lastAggrMax = aggr.MaxTime
	}

	for currentRaw < len(groupedRaw) {
		sectionStart := max(groupedRaw[currentRaw].mint, lastAggrMax)
		sectionEnd := groupedRaw[currentRaw].maxt

		if sectionStart < sectionEnd {
			sections = append(sections, &rawSection{
				chunks: groupedRaw[currentRaw].chunks,
				mint:   sectionStart,
				maxt:   sectionEnd,
			})
		}

		currentRaw++
	}

	return sections
}

func removeOverlaps(chunks []storepb.AggrChunk) []storepb.AggrChunk {
	var ret []storepb.AggrChunk
	var lastMaxTime int64 = -1
	for _, chk := range chunks {
		if chk.MinTime > lastMaxTime {
			ret = append(ret, chk)
			lastMaxTime = chk.MaxTime
		}
	}

	return ret
}

func groupOverlappingChunks(chunks []storepb.AggrChunk) []rawSection {
	if len(chunks) == 0 {
		return nil
	}

	var groups []rawSection
	var currentGroup []storepb.AggrChunk
	currentGroupMinTime := chunks[0].MinTime
	currentGroupMaxTime := chunks[0].MaxTime

	currentGroup = append(currentGroup, chunks[0])

	for i := 1; i < len(chunks); i++ {
		chunk := chunks[i]
		if chunk.MinTime <= currentGroupMaxTime {
			currentGroup = append(currentGroup, chunk)
			if chunk.MaxTime > currentGroupMaxTime {
				currentGroupMaxTime = chunk.MaxTime
			}
		} else {
			groups = append(groups, rawSection{
				chunks: currentGroup,
				mint:   currentGroupMinTime,
				maxt:   currentGroupMaxTime,
			})
			currentGroup = []storepb.AggrChunk{chunk}
			currentGroupMinTime = chunk.MinTime
			currentGroupMaxTime = chunk.MaxTime
		}
	}

	groups = append(groups, rawSection{
		chunks: currentGroup,
		mint:   currentGroupMinTime,
		maxt:   currentGroupMaxTime,
	})

	return groups
}

type chainedChunkSeries struct {
	lset       labels.Labels
	sections   []section
	mint, maxt int64
	aggrs      []storepb.Aggr
}

func newChainedChunkSeries(lset labels.Labels, sections []section, mint, maxt int64, aggrs []storepb.Aggr) *chainedChunkSeries {
	return &chainedChunkSeries{
		lset:     lset,
		sections: sections,
		mint:     mint,
		maxt:     maxt,
		aggrs:    aggrs,
	}
}

func (s *chainedChunkSeries) Labels() labels.Labels {
	return s.lset
}

func (s *chainedChunkSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	var sit chunkenc.Iterator
	its := make([]chunkenc.Iterator, 0, len(s.sections))

	if len(s.aggrs) == 1 {
		for _, c := range s.sections {
			its = append(its, c.iterator(s.aggrs[0]))
		}

		if s.aggrs[0] == storepb.Aggr_COUNTER {
			// TODO(bwplotka): This breaks resets function. See https://github.com/thanos-io/thanos/issues/3644
			sit = downsample.NewApplyCounterResetsIterator(its...)
		} else {
			sit = newChunkSeriesIterator(its)
		}

		return dedup.NewBoundedSeriesIterator(sit, s.mint, s.maxt)
	}

	if len(s.aggrs) != 2 {
		return errSeriesIterator{err: errors.Errorf("unexpected result aggregate type %v", s.aggrs)}
	}

	switch {
	case s.aggrs[0] == storepb.Aggr_SUM && s.aggrs[1] == storepb.Aggr_COUNT,
		s.aggrs[0] == storepb.Aggr_COUNT && s.aggrs[1] == storepb.Aggr_SUM:

		for _, c := range s.sections {
			if c.iterator(storepb.Aggr_RAW) != nil {
				its = append(its, c.iterator(storepb.Aggr_RAW))
			} else {
				sum, cnt := c.iterator(storepb.Aggr_SUM), c.iterator(storepb.Aggr_COUNT)
				its = append(its, downsample.NewAverageChunkIterator(cnt, sum))
			}
		}
		sit = newChunkSeriesIterator(its)
	default:
		return errSeriesIterator{err: errors.Errorf("unexpected result aggregate type %v", s.aggrs)}
	}
	return dedup.NewBoundedSeriesIterator(sit, s.mint, s.maxt)
}
