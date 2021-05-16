// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dedup

import (
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"
)

func TestDedupChunkSeriesMerger(t *testing.T) {
	m := NewDedupChunkSeriesMerger()

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
			name: "150 overlapping samples, split chunk",
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
			require.Equal(t, tc.expected.Labels(), merged.Labels())
			actChks, actErr := storage.ExpandChunks(merged.Iterator())
			expChks, expErr := storage.ExpandChunks(tc.expected.Iterator())

			require.Equal(t, expErr, actErr)
			require.Equal(t, expChks, actChks)
		})
	}
}
