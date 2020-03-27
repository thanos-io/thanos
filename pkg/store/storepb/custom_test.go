// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

type sample struct {
	t int64
	v float64
}

type listSeriesSet struct {
	series []Series
	idx    int
}

func newSeries(tb testing.TB, lset labels.Labels, smplChunks [][]sample) Series {
	var s Series

	for _, l := range lset {
		s.Labels = append(s.Labels, Label{Name: l.Name, Value: l.Value})
	}

	for _, smpls := range smplChunks {
		c := chunkenc.NewXORChunk()
		a, err := c.Appender()
		testutil.Ok(tb, err)

		for _, smpl := range smpls {
			a.Append(smpl.t, smpl.v)
		}

		ch := AggrChunk{
			MinTime: smpls[0].t,
			MaxTime: smpls[len(smpls)-1].t,
			Raw:     &Chunk{Type: Chunk_XOR, Data: c.Bytes()},
		}

		s.Chunks = append(s.Chunks, ch)
	}
	return s
}

func newListSeriesSet(tb testing.TB, raw []rawSeries) *listSeriesSet {
	var series []Series
	for _, s := range raw {
		series = append(series, newSeries(tb, s.lset, s.chunks))
	}
	return &listSeriesSet{
		series: series,
		idx:    -1,
	}
}

func (s *listSeriesSet) Next() bool {
	s.idx++
	return s.idx < len(s.series)
}

func (s *listSeriesSet) At() ([]Label, []AggrChunk) {
	if s.idx < 0 || s.idx >= len(s.series) {
		return nil, nil
	}

	return s.series[s.idx].Labels, s.series[s.idx].Chunks
}

func (s *listSeriesSet) Err() error { return nil }

type errSeriesSet struct{ err error }

func (errSeriesSet) Next() bool { return false }

func (errSeriesSet) At() ([]Label, []AggrChunk) { return nil, nil }

func (e errSeriesSet) Err() error { return e.err }

func TestMergeSeriesSet(t *testing.T) {
	for _, tcase := range []struct {
		desc     string
		in       [][]rawSeries
		expected []rawSeries
	}{
		{
			desc:     "nils",
			in:       nil,
			expected: nil,
		},
		{
			desc: "single seriesSet, distinct series",
			in: [][]rawSeries{{{
				lset:   labels.FromStrings("a", "a"),
				chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
			}, {
				lset:   labels.FromStrings("a", "c"),
				chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
			}}},

			expected: []rawSeries{
				{
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
				}, {
					lset:   labels.FromStrings("a", "c"),
					chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
				},
			},
		},
		{
			desc: "two seriesSets, distinct series",
			in: [][]rawSeries{{{
				lset:   labels.FromStrings("a", "a"),
				chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
			}}, {{
				lset:   labels.FromStrings("a", "c"),
				chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
			}}},

			expected: []rawSeries{
				{
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
				}, {
					lset:   labels.FromStrings("a", "c"),
					chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
				},
			},
		},
		{
			desc: "two seriesSets, {a=c} series to merge",
			in: [][]rawSeries{
				{
					{
						lset:   labels.FromStrings("a", "a"),
						chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
					},
					{
						lset:   labels.FromStrings("a", "c"),
						chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
					},
				},
				{
					{
						lset:   labels.FromStrings("a", "c"),
						chunks: [][]sample{{{7, 1}, {8, 2}}, {{9, 3}, {10, 4}, {11, 4444}}}, // Last sample overlaps, merge ignores that.
					},
				},
			},

			expected: []rawSeries{
				{
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
				}, {
					lset:   labels.FromStrings("a", "c"),
					chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}, {{7, 1}, {8, 2}}, {{9, 3}, {10, 4}, {11, 4444}}},
				},
			},
		},
		{
			// SeriesSet can return same series within different iterations. MergeSeries should not try to merge those.
			// We do it on last step possible: Querier promSet.
			desc: "single seriesSets, {a=c} series to merge.",
			in: [][]rawSeries{
				{
					{
						lset:   labels.FromStrings("a", "a"),
						chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
					},
					{
						lset:   labels.FromStrings("a", "c"),
						chunks: [][]sample{{{7, 1}, {8, 2}}, {{9, 3}, {10, 4}, {11, 4444}}},
					},
					{
						lset:   labels.FromStrings("a", "c"),
						chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
					},
				},
			},

			expected: []rawSeries{
				{
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
				}, {
					lset:   labels.FromStrings("a", "c"),
					chunks: [][]sample{{{7, 1}, {8, 2}}, {{9, 3}, {10, 4}, {11, 4444}}},
				}, {
					lset:   labels.FromStrings("a", "c"),
					chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
				},
			},
		},
	} {
		if ok := t.Run(tcase.desc, func(t *testing.T) {
			var input []SeriesSet
			for _, iss := range tcase.in {
				input = append(input, newListSeriesSet(t, iss))
			}
			ss := MergeSeriesSets(input...)
			seriesEquals(t, tcase.expected, ss)
			testutil.Ok(t, ss.Err())
		}); !ok {
			return
		}
	}
}

func TestMergeSeriesSetError(t *testing.T) {
	var input []SeriesSet
	for _, iss := range [][]rawSeries{{{
		lset:   labels.FromStrings("a", "a"),
		chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
	}}, {{
		lset:   labels.FromStrings("a", "c"),
		chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
	}}} {
		input = append(input, newListSeriesSet(t, iss))
	}
	expectedErr := errors.New("test error")
	ss := MergeSeriesSets(append(input, errSeriesSet{err: expectedErr})...)
	testutil.Equals(t, expectedErr, ss.Err())
}

type rawSeries struct {
	lset   labels.Labels
	chunks [][]sample
}

func seriesEquals(t *testing.T, expected []rawSeries, gotSS SeriesSet) {
	var got []Series
	for gotSS.Next() {
		lset, chks := gotSS.At()
		got = append(got, Series{Labels: lset, Chunks: chks})
	}

	testutil.Equals(t, len(expected), len(got), "got: %v", got)

	for i, series := range got {
		testutil.Equals(t, expected[i].lset, LabelsToPromLabels(series.Labels))
		testutil.Equals(t, len(expected[i].chunks), len(series.Chunks), "unexpected number of chunks")

		for k, chk := range series.Chunks {
			c, err := chunkenc.FromData(chunkenc.EncXOR, chk.Raw.Data)
			testutil.Ok(t, err)

			j := 0
			iter := c.Iterator(nil)
			for iter.Next() {
				testutil.Assert(t, j < len(expected[i].chunks[k]), "more samples than expected for %s chunk %d", series.Labels, k)

				tv, v := iter.At()
				testutil.Equals(t, expected[i].chunks[k][j], sample{tv, v})
				j++
			}
			testutil.Ok(t, iter.Err())
			testutil.Equals(t, len(expected[i].chunks[k]), j)
		}
	}

}

// Test the cost of merging series sets for different number of merged sets and their size.
// The subset are all equivalent so this does not capture merging of partial or non-overlapping sets well.
func BenchmarkMergedSeriesSet(b *testing.B) {
	var sel func(sets []SeriesSet) SeriesSet
	sel = func(sets []SeriesSet) SeriesSet {
		if len(sets) == 0 {
			return EmptySeriesSet()
		}
		if len(sets) == 1 {
			return sets[0]
		}
		l := len(sets) / 2
		return newMergedSeriesSet(sel(sets[:l]), sel(sets[l:]))
	}

	chunks := [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}}
	for _, k := range []int{
		100,
		1000,
		10000,
		20000,
	} {
		for _, j := range []int{1, 2, 4, 8, 16, 32} {
			b.Run(fmt.Sprintf("series=%d,blocks=%d", k, j), func(b *testing.B) {
				lbls, err := labels.ReadLabels(filepath.Join("../../testutil/testdata", "20kseries.json"), k)
				testutil.Ok(b, err)

				sort.Sort(labels.Slice(lbls))

				in := make([][]rawSeries, j)

				for _, l := range lbls {
					for j := range in {
						in[j] = append(in[j], rawSeries{lset: l, chunks: chunks})
					}
				}

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					var sets []SeriesSet
					for _, s := range in {
						sets = append(sets, newListSeriesSet(b, s))
					}
					ms := sel(sets)

					i := 0
					for ms.Next() {
						i++
					}
					testutil.Ok(b, ms.Err())
					testutil.Equals(b, len(lbls), i)
				}
			})
		}
	}
}

var testLsetMap = map[string]string{
	"a":                           "1",
	"c":                           "2",
	"d":                           "dsfsdfsdfsdf123414234",
	"124134235423534534ffdasdfsf": "1",
	"":                            "",
	"b":                           "",
}

func TestPromLabelsToLabelsUnsafe(t *testing.T) {
	testutil.Equals(t, PromLabelsToLabels(labels.FromMap(testLsetMap)), PromLabelsToLabelsUnsafe(labels.FromMap(testLsetMap)))
}

func TestLabelsToPromLabelsUnsafe(t *testing.T) {
	testutil.Equals(t, labels.FromMap(testLsetMap), LabelsToPromLabels(PromLabelsToLabels(labels.FromMap(testLsetMap))))
	testutil.Equals(t, labels.FromMap(testLsetMap), LabelsToPromLabelsUnsafe(PromLabelsToLabels(labels.FromMap(testLsetMap))))
}

func TestPrompbLabelsToLabelsUnsafe(t *testing.T) {
	var pb []prompb.Label
	for _, l := range labels.FromMap(testLsetMap) {
		pb = append(pb, prompb.Label{Name: l.Name, Value: l.Value})
	}
	testutil.Equals(t, PromLabelsToLabels(labels.FromMap(testLsetMap)), PrompbLabelsToLabels(pb))
	testutil.Equals(t, PromLabelsToLabels(labels.FromMap(testLsetMap)), PrompbLabelsToLabelsUnsafe(pb))
}

func BenchmarkUnsafeVSSafeLabelsConversion(b *testing.B) {
	const (
		fmtLbl = "%07daaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"
		num    = 10000
	)
	lbls := make([]labels.Label, 0, num)
	for i := 0; i < num; i++ {
		lbls = append(lbls, labels.Label{Name: fmt.Sprintf(fmtLbl, i), Value: fmt.Sprintf(fmtLbl, i)})
	}

	var converted labels.Labels
	b.Run("safe", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			converted = LabelsToPromLabels(PromLabelsToLabels(lbls))
		}
	})
	testutil.Equals(b, num, len(converted))
	b.Run("unsafe", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			converted = LabelsToPromLabelsUnsafe(PromLabelsToLabelsUnsafe(lbls))
		}
	})
	testutil.Equals(b, num, len(converted))

}
