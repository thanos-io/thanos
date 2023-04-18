// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dedup

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

func TestDedupChunkSeriesMerger(t *testing.T) {
	m := NewChunkSeriesMerger()

	for _, tc := range []struct {
		name     string
		input    []storage.ChunkSeries
		expected storage.ChunkSeries
	}{
		{
			name: "single empty series",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), nil),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), nil),
		},
		{
			name: "single series",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}}, []tsdbutil.Sample{sample{3, 3}}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}}, []tsdbutil.Sample{sample{3, 3}}),
		},
		{
			name: "two empty series",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), nil),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), nil),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), nil),
		},
		{
			name: "two non overlapping",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}}, []tsdbutil.Sample{sample{3, 3}, sample{5, 5}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{7, 7}, sample{9, 9}}, []tsdbutil.Sample{sample{10, 10}}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}}, []tsdbutil.Sample{sample{3, 3}, sample{5, 5}}, []tsdbutil.Sample{sample{7, 7}, sample{9, 9}}, []tsdbutil.Sample{sample{10, 10}}),
		},
		{
			name: "two overlapping",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}}, []tsdbutil.Sample{sample{3, 3}, sample{8, 8}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{7, 7}, sample{9, 9}}, []tsdbutil.Sample{sample{10, 10}}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}}, []tsdbutil.Sample{sample{3, 3}, sample{8, 8}}, []tsdbutil.Sample{sample{10, 10}}),
		},
		{
			name: "two overlapping with large time diff",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}}, []tsdbutil.Sample{sample{2, 2}, sample{5008, 5008}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{7, 7}, sample{9, 9}}, []tsdbutil.Sample{sample{10, 10}}),
			},
			// sample{5008, 5008} is added to the result due to its large timestamp.
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{5008, 5008}}),
		},
		{
			name: "two duplicated",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{5, 5}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{2, 2}, sample{3, 3}, sample{5, 5}}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{5, 5}}),
		},
		{
			name: "three overlapping",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{5, 5}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{2, 2}, sample{3, 3}, sample{6, 6}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{0, 0}, sample{4, 4}}),
			},
			// only samples from the last series are retained due to high penalty.
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{0, 0}, sample{4, 4}}),
		},
		{
			name: "three in chained overlap",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{5, 5}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{4, 4}, sample{6, 66}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{6, 6}, sample{10, 10}}),
			},
			// only samples from the last series are retained due to high penalty.
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{1, 1}, sample{2, 2}, sample{3, 3}, sample{5, 5}}),
		},
		{
			name: "three in chained overlap complex",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{0, 0}, sample{5, 5}}, []tsdbutil.Sample{sample{10, 10}, sample{15, 15}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{2, 2}, sample{20, 20}}, []tsdbutil.Sample{sample{25, 25}, sample{30, 30}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []tsdbutil.Sample{sample{18, 18}, sample{26, 26}}, []tsdbutil.Sample{sample{31, 31}, sample{35, 35}}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"),
				[]tsdbutil.Sample{sample{0, 0}, sample{5, 5}},
				[]tsdbutil.Sample{sample{31, 31}, sample{35, 35}},
			),
		},
		{
			name: "110 overlapping samples",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), tsdbutil.GenerateSamples(0, 110)), // [0 - 110)
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), tsdbutil.GenerateSamples(60, 50)), // [60 - 110)
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"),
				tsdbutil.GenerateSamples(0, 110),
			),
		},
		{
			name: "150 overlapping samples, no chunk splitting due to penalty deduplication",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), tsdbutil.GenerateSamples(0, 90)),  // [0 - 90)
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), tsdbutil.GenerateSamples(60, 90)), // [90 - 150)
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"),
				tsdbutil.GenerateSamples(0, 90),
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			merged := m(tc.input...)
			testutil.Equals(t, tc.expected.Labels(), merged.Labels())
			actChks, actErr := storage.ExpandChunks(merged.Iterator(nil))
			expChks, expErr := storage.ExpandChunks(tc.expected.Iterator(nil))

			testutil.Equals(t, expErr, actErr)
			testutil.Equals(t, expChks, actChks)
		})
	}
}

func TestDedupChunkSeriesMergerDownsampledChunks(t *testing.T) {
	m := NewChunkSeriesMerger()

	defaultLabels := labels.FromStrings("bar", "baz")
	emptySamples := downsample.SamplesFromTSDBSamples([]tsdbutil.Sample{})
	// Samples are created with step 1m. So the 5m downsampled chunk has 2 samples.
	samples1 := downsample.SamplesFromTSDBSamples(createSamplesWithStep(0, 10, 60*1000))
	// Non overlapping samples with samples1. 5m downsampled chunk has 2 samples.
	samples2 := downsample.SamplesFromTSDBSamples(createSamplesWithStep(600000, 10, 60*1000))
	// Overlapped with samples1.
	samples3 := downsample.SamplesFromTSDBSamples(createSamplesWithStep(120000, 10, 60*1000))

	for _, tc := range []struct {
		name     string
		input    []storage.ChunkSeries
		expected storage.ChunkSeries
	}{
		{
			name: "single empty series",
			input: []storage.ChunkSeries{
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(emptySamples, downsample.ResLevel1)...)
					},
				},
			},
			expected: &storage.ChunkSeriesEntry{
				Lset: defaultLabels,
				ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
					return storage.NewListChunkSeriesIterator()
				},
			},
		},
		{
			name: "single series",
			input: []storage.ChunkSeries{
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples1, downsample.ResLevel1)...)
					},
				},
			},
			expected: &storage.ChunkSeriesEntry{
				Lset: defaultLabels,
				ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
					return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples1, downsample.ResLevel1)...)
				},
			},
		},
		{
			name: "two empty series",
			input: []storage.ChunkSeries{
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(emptySamples, downsample.ResLevel1)...)
					},
				},
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(emptySamples, downsample.ResLevel1)...)
					},
				},
			},
			expected: &storage.ChunkSeriesEntry{
				Lset: defaultLabels,
				ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
					return storage.NewListChunkSeriesIterator()
				},
			},
		},
		{
			name: "two non overlapping series",
			input: []storage.ChunkSeries{
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples1, downsample.ResLevel1)...)
					},
				},
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples2, downsample.ResLevel1)...)
					},
				},
			},
			expected: &storage.ChunkSeriesEntry{
				Lset: defaultLabels,
				ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
					return storage.NewListChunkSeriesIterator(
						append(downsample.DownsampleRaw(samples1, downsample.ResLevel1),
							downsample.DownsampleRaw(samples2, downsample.ResLevel1)...)...)
				},
			},
		},
		{
			// 1:1 duplicated chunks are deduplicated.
			name: "two same series",
			input: []storage.ChunkSeries{
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples1, downsample.ResLevel1)...)
					},
				},
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples1, downsample.ResLevel1)...)
					},
				},
			},
			expected: &storage.ChunkSeriesEntry{
				Lset: defaultLabels,
				ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
					return storage.NewListChunkSeriesIterator(
						downsample.DownsampleRaw(samples1, downsample.ResLevel1)...)
				},
			},
		},
		{
			name: "two overlapping series",
			input: []storage.ChunkSeries{
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples1, downsample.ResLevel1)...)
					},
				},
				&storage.ChunkSeriesEntry{
					Lset: defaultLabels,
					ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
						return storage.NewListChunkSeriesIterator(downsample.DownsampleRaw(samples3, downsample.ResLevel1)...)
					},
				},
			},
			expected: &storage.ChunkSeriesEntry{
				Lset: defaultLabels,
				ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
					return storage.NewListChunkSeriesIterator(chunks.Meta{
						MinTime: 299999,
						MaxTime: 540000,
						Chunk: downsample.EncodeAggrChunk([5]chunkenc.Chunk{
							tsdbutil.ChunkFromSamples([]tsdbutil.Sample{sample{299999, 3}, sample{540000, 5}}).Chunk,
							tsdbutil.ChunkFromSamples([]tsdbutil.Sample{sample{299999, 540000}, sample{540000, 2100000}}).Chunk,
							tsdbutil.ChunkFromSamples([]tsdbutil.Sample{sample{299999, 120000}, sample{540000, 300000}}).Chunk,
							tsdbutil.ChunkFromSamples([]tsdbutil.Sample{sample{299999, 240000}, sample{540000, 540000}}).Chunk,
							tsdbutil.ChunkFromSamples([]tsdbutil.Sample{sample{299999, 240000}, sample{299999, 240000}}).Chunk,
						}),
					})
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			merged := m(tc.input...)
			testutil.Equals(t, tc.expected.Labels(), merged.Labels())
			actChks, actErr := storage.ExpandChunks(merged.Iterator(nil))
			expChks, expErr := storage.ExpandChunks(tc.expected.Iterator(nil))

			testutil.Equals(t, expErr, actErr)
			testutil.Equals(t, expChks, actChks)
		})
	}
}

func createSamplesWithStep(start, numOfSamples, step int) []tsdbutil.Sample {
	res := make([]tsdbutil.Sample, numOfSamples)
	cur := start
	for i := 0; i < numOfSamples; i++ {
		res[i] = sample{t: int64(cur), f: float64(cur)}
		cur += step
	}

	return res
}
