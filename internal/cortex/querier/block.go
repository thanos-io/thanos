// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package querier

import (
	"math"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/thanos-io/thanos/internal/cortex/querier/series"
)

func convertMatchersToLabelMatcher(matchers []*labels.Matcher) []storepb.LabelMatcher {
	var converted []storepb.LabelMatcher
	for _, m := range matchers {
		var t storepb.LabelMatcher_Type
		switch m.Type {
		case labels.MatchEqual:
			t = storepb.LabelMatcher_EQ
		case labels.MatchNotEqual:
			t = storepb.LabelMatcher_NEQ
		case labels.MatchRegexp:
			t = storepb.LabelMatcher_RE
		case labels.MatchNotRegexp:
			t = storepb.LabelMatcher_NRE
		}

		converted = append(converted, storepb.LabelMatcher{
			Type:  t,
			Name:  m.Name,
			Value: m.Value,
		})
	}
	return converted
}

// Implementation of storage.SeriesSet, based on individual responses from store client.
type blockQuerierSeriesSet struct {
	series   []*storepb.Series
	warnings storage.Warnings

	// next response to process
	next int

	currSeries storage.Series
}

func (bqss *blockQuerierSeriesSet) Next() bool {
	bqss.currSeries = nil

	if bqss.next >= len(bqss.series) {
		return false
	}

	currLabels := labelpb.ZLabelsToPromLabels(bqss.series[bqss.next].Labels)
	currChunks := bqss.series[bqss.next].Chunks

	bqss.next++

	// Merge chunks for current series. Chunks may come in multiple responses, but as soon
	// as the response has chunks for a new series, we can stop searching. Series are sorted.
	// See documentation for StoreClient.Series call for details.
	for bqss.next < len(bqss.series) && labels.Compare(currLabels, labelpb.ZLabelsToPromLabels(bqss.series[bqss.next].Labels)) == 0 {
		currChunks = append(currChunks, bqss.series[bqss.next].Chunks...)
		bqss.next++
	}

	bqss.currSeries = newBlockQuerierSeries(currLabels, currChunks)
	return true
}

func (bqss *blockQuerierSeriesSet) At() storage.Series {
	return bqss.currSeries
}

func (bqss *blockQuerierSeriesSet) Err() error {
	return nil
}

func (bqss *blockQuerierSeriesSet) Warnings() storage.Warnings {
	return bqss.warnings
}

// newBlockQuerierSeries makes a new blockQuerierSeries. Input labels must be already sorted by name.
func newBlockQuerierSeries(lbls []labels.Label, chunks []storepb.AggrChunk) *blockQuerierSeries {
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].MinTime < chunks[j].MinTime
	})

	return &blockQuerierSeries{labels: lbls, chunks: chunks}
}

type blockQuerierSeries struct {
	labels labels.Labels
	chunks []storepb.AggrChunk
}

func (bqs *blockQuerierSeries) Labels() labels.Labels {
	return bqs.labels
}

func (bqs *blockQuerierSeries) Iterator() chunkenc.Iterator {
	if len(bqs.chunks) == 0 {
		// should not happen in practice, but we have a unit test for it
		return series.NewErrIterator(errors.New("no chunks"))
	}

	its := make([]chunkenc.Iterator, 0, len(bqs.chunks))

	for _, c := range bqs.chunks {
		ch, err := chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
		if err != nil {
			return series.NewErrIterator(errors.Wrapf(err, "failed to initialize chunk from XOR encoded raw data (series: %v min time: %d max time: %d)", bqs.Labels(), c.MinTime, c.MaxTime))
		}

		it := ch.Iterator(nil)
		its = append(its, it)
	}

	return newBlockQuerierSeriesIterator(bqs.Labels(), its)
}

func newBlockQuerierSeriesIterator(labels labels.Labels, its []chunkenc.Iterator) *blockQuerierSeriesIterator {
	return &blockQuerierSeriesIterator{labels: labels, iterators: its, lastT: math.MinInt64}
}

// blockQuerierSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type blockQuerierSeriesIterator struct {
	// only used for error reporting
	labels labels.Labels

	iterators []chunkenc.Iterator
	i         int
	lastT     int64
}

// TODO(rabenhorst): Native histogram support needs to be added, float type is hardcoded.
func (it *blockQuerierSeriesIterator) Seek(t int64) chunkenc.ValueType {
	// We generally expect the chunks already to be cut down
	// to the range we are interested in. There's not much to be gained from
	// hopping across chunks so we just call next until we reach t.
	for {
		ct, _ := it.At()
		if ct >= t {
			return chunkenc.ValFloat
		}
		if it.Next() == chunkenc.ValNone {
			return chunkenc.ValNone
		}
	}
}

func (it *blockQuerierSeriesIterator) At() (int64, float64) {
	if it.i >= len(it.iterators) {
		return 0, 0
	}

	t, v := it.iterators[it.i].At()
	it.lastT = t
	return t, v
}

// TODO(rabenhorst): Needs to be implemented for native histogram support.
func (it *blockQuerierSeriesIterator) AtHistogram() (int64, *histogram.Histogram) {
	panic("not implemented")
}

func (it *blockQuerierSeriesIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	panic("not implemented")
}

func (it *blockQuerierSeriesIterator) AtT() int64 {
	t, _ := it.At()
	return t
}

func (it *blockQuerierSeriesIterator) Next() chunkenc.ValueType {
	if it.i >= len(it.iterators) {
		return chunkenc.ValNone
	}

	if valueType := it.iterators[it.i].Next(); valueType != chunkenc.ValNone {
		return valueType
	}
	if it.iterators[it.i].Err() != nil {
		return chunkenc.ValNone
	}

	for {
		it.i++

		if it.i >= len(it.iterators) {
			return chunkenc.ValNone
		}

		// we must advance iterator first, to see if it has any samples.
		// Seek will call At() as its first operation.
		if it.iterators[it.i].Next() == chunkenc.ValNone {
			if it.iterators[it.i].Err() != nil {
				return chunkenc.ValNone
			}

			// Found empty iterator without error, skip it.
			continue
		}

		// Chunks are guaranteed to be ordered but not generally guaranteed to not overlap.
		// We must ensure to skip any overlapping range between adjacent chunks.
		return it.Seek(it.lastT + 1)
	}
}

func (it *blockQuerierSeriesIterator) Err() error {
	if it.i >= len(it.iterators) {
		return nil
	}

	err := it.iterators[it.i].Err()
	if err != nil {
		return errors.Wrapf(err, "cannot iterate chunk for series: %v", it.labels)
	}
	return nil
}
