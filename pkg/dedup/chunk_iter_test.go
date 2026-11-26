// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dedup

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

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
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 1, f: 1}, sample{t: 2, f: 2}}, []chunks.Sample{sample{t: 3, f: 3}}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 1, f: 1}, sample{t: 2, f: 2}}, []chunks.Sample{sample{t: 3, f: 3}}),
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
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 1, f: 1}, sample{t: 2, f: 2}}, []chunks.Sample{sample{t: 3, f: 3}, sample{t: 5, f: 5}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 7, f: 7}, sample{t: 9, f: 9}}, []chunks.Sample{sample{t: 10, f: 10}}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 1, f: 1}, sample{t: 2, f: 2}}, []chunks.Sample{sample{t: 3, f: 3}, sample{t: 5, f: 5}}, []chunks.Sample{sample{t: 7, f: 7}, sample{t: 9, f: 9}}, []chunks.Sample{sample{t: 10, f: 10}}),
		},
		{
			name: "two overlapping",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 1, f: 1}, sample{t: 2, f: 2}}, []chunks.Sample{sample{t: 3, f: 3}, sample{t: 8, f: 8}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 7, f: 7}, sample{t: 9, f: 9}}, []chunks.Sample{sample{t: 10, f: 10}}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 1, f: 1}, sample{t: 2, f: 2}}, []chunks.Sample{sample{t: 3, f: 3}, sample{t: 8, f: 8}}, []chunks.Sample{sample{t: 10, f: 10}}),
		},
		{
			name: "two overlapping with large time diff",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 1, f: 1}, sample{t: 2, f: 2}}, []chunks.Sample{sample{t: 2, f: 2}, sample{t: 5008, f: 5008}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 7, f: 7}, sample{t: 9, f: 9}}, []chunks.Sample{sample{t: 10, f: 10}}),
			},
			// sample{t: 5008, f: 5008} is added to the result due to its large timestamp.
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 1, f: 1}, sample{t: 2, f: 2}, sample{t: 5008, f: 5008}}),
		},
		{
			name: "two duplicated",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 1, f: 1}, sample{t: 2, f: 2}, sample{t: 3, f: 3}, sample{t: 5, f: 5}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 2, f: 2}, sample{t: 3, f: 3}, sample{t: 5, f: 5}}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 1, f: 1}, sample{t: 2, f: 2}, sample{t: 3, f: 3}, sample{t: 5, f: 5}}),
		},
		{
			name: "three overlapping",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 1, f: 1}, sample{t: 2, f: 2}, sample{t: 3, f: 3}, sample{t: 5, f: 5}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 2, f: 2}, sample{t: 3, f: 3}, sample{t: 6, f: 6}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 0, f: 0}, sample{t: 4, f: 4}}),
			},
			// only samples from the last series are retained due to high penalty.
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 0, f: 0}, sample{t: 4, f: 4}}),
		},
		{
			name: "three in chained overlap",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 1, f: 1}, sample{t: 2, f: 2}, sample{t: 3, f: 3}, sample{t: 5, f: 5}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 4, f: 4}, sample{t: 6, f: 66}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 6, f: 6}, sample{t: 10, f: 10}}),
			},
			// only samples from the last series are retained due to high penalty.
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 1, f: 1}, sample{t: 2, f: 2}, sample{t: 3, f: 3}, sample{t: 5, f: 5}}),
		},
		{
			name: "three in chained overlap complex",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 0, f: 0}, sample{t: 5, f: 5}}, []chunks.Sample{sample{t: 10, f: 10}, sample{t: 15, f: 15}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 2, f: 2}, sample{t: 20, f: 20}}, []chunks.Sample{sample{t: 25, f: 25}, sample{t: 30, f: 30}}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{sample{t: 18, f: 18}, sample{t: 26, f: 26}}, []chunks.Sample{sample{t: 31, f: 31}, sample{t: 35, f: 35}}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"),
				[]chunks.Sample{sample{t: 0, f: 0}, sample{t: 5, f: 5}},
				[]chunks.Sample{sample{t: 31, f: 31}, sample{t: 35, f: 35}},
			),
		},
		{
			name: "110 overlapping samples",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), chunks.GenerateSamples(0, 110)), // [0 - 110)
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), chunks.GenerateSamples(60, 50)), // [60 - 110)
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"),
				chunks.GenerateSamples(0, 110),
			),
		},
		{
			name: "150 overlapping samples, no chunk splitting due to penalty deduplication",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), chunks.GenerateSamples(0, 90)),  // [0 - 90)
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), chunks.GenerateSamples(60, 90)), // [90 - 150)
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"),
				chunks.GenerateSamples(0, 90),
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			merged := m(tc.input...)
			testutil.Equals(t, tc.expected.Labels(), merged.Labels())
			actChks, actErr := storage.ExpandChunks(merged.Iterator(nil))
			expChks, expErr := storage.ExpandChunks(tc.expected.Iterator(nil))

			testutil.Equals(t, expErr, actErr)
			testutil.Equals(t, expChks, actChks)
		})
	}
}

func TestDedupChunkSeriesMergerPreservesXOR2StartTimestamps(t *testing.T) {
	lset := labels.FromStrings("bar", "baz")
	for _, tc := range []struct {
		name              string
		base              []chunks.Sample
		overlap           []chunks.Sample
		expectedEncodings []chunkenc.Encoding
		expectedTS        []int64
		expectedVals      []float64
		expectedST        []int64
	}{
		{
			name: "XOR2 base overlapping XOR",
			base: []chunks.Sample{
				startTimestampSample{sample: sample{t: 1000, f: 1}, st: 100},
				startTimestampSample{sample: sample{t: 7000, f: 7}, st: 200},
			},
			overlap:           []chunks.Sample{sample{t: 2000, f: 2}, sample{t: 3000, f: 3}},
			expectedEncodings: []chunkenc.Encoding{chunkenc.EncXOR2},
			expectedTS:        []int64{1000, 7000},
			expectedVals:      []float64{1, 7},
			expectedST:        []int64{100, 200},
		},
		{
			name: "XOR base overlapping XOR2 with surviving sample",
			base: []chunks.Sample{sample{t: 1000, f: 1}, sample{t: 7000, f: 7}},
			overlap: []chunks.Sample{
				startTimestampSample{sample: sample{t: 2000, f: 2}, st: 100},
				startTimestampSample{sample: sample{t: 20000, f: 20}, st: 200},
			},
			expectedEncodings: []chunkenc.Encoding{chunkenc.EncXOR, chunkenc.EncXOR2},
			expectedTS:        []int64{1000, 7000, 20000},
			expectedVals:      []float64{1, 7, 20},
			expectedST:        []int64{0, 0, 200},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			merged := NewChunkSeriesMerger()(
				storage.NewListChunkSeriesFromSamples(lset, tc.base),
				storage.NewListChunkSeriesFromSamples(lset, tc.overlap),
			)
			metas, err := storage.ExpandChunks(merged.Iterator(nil))
			testutil.Ok(t, err)
			gotEncodings := make([]chunkenc.Encoding, 0, len(metas))
			var samples []chunks.Sample
			for _, m := range metas {
				gotEncodings = append(gotEncodings, m.Chunk.Encoding())
				chunkSamples, err := storage.ExpandSamples(m.Chunk.Iterator(nil), nil)
				testutil.Ok(t, err)
				samples = append(samples, chunkSamples...)
			}
			testutil.Equals(t, tc.expectedEncodings, gotEncodings)
			testutil.Equals(t, len(tc.expectedST), len(samples))
			for i, s := range samples {
				testutil.Equals(t, tc.expectedTS[i], s.T())
				testutil.Equals(t, tc.expectedVals[i], s.F())
				testutil.Equals(t, tc.expectedST[i], s.ST())
			}
		})
	}
}

type fakeEncodingChunk struct {
	chunkenc.Chunk
	enc chunkenc.Encoding
}

func (f fakeEncodingChunk) Encoding() chunkenc.Encoding { return f.enc }

func TestDedupChunkSeriesMergerWithFloatEncoding(t *testing.T) {
	t.Parallel()

	type wantSample struct {
		st, t int64
		v     float64
	}

	lset := labels.FromStrings("bar", "baz")
	for _, tc := range []struct {
		name          string
		floatEncoding chunkenc.Encoding
		base          []chunks.Sample
		overlap       []chunks.Sample
		wantEncodings []chunkenc.Encoding
		wantBounds    [][2]int64
		wantSamples   []wantSample
	}{
		{
			name:          "overlapping_xor_reencoded_as_xor2",
			floatEncoding: chunkenc.EncXOR2,
			base:          []chunks.Sample{sample{t: 1000, f: 1}, sample{t: 7000, f: 7}},
			overlap:       []chunks.Sample{sample{t: 2000, f: 2}, sample{t: 20000, f: 20}},
			wantEncodings: []chunkenc.Encoding{chunkenc.EncXOR2},
			wantBounds:    [][2]int64{{1000, 20000}},
			wantSamples:   []wantSample{{t: 1000, v: 1}, {t: 7000, v: 7}, {t: 20000, v: 20}},
		},
		{
			name:          "overlapping_xor_reencoded_as_xor",
			floatEncoding: chunkenc.EncXOR,
			base:          []chunks.Sample{sample{t: 1000, f: 1}, sample{t: 7000, f: 7}},
			overlap:       []chunks.Sample{sample{t: 2000, f: 2}, sample{t: 20000, f: 20}},
			wantEncodings: []chunkenc.Encoding{chunkenc.EncXOR},
			wantBounds:    [][2]int64{{1000, 20000}},
			wantSamples:   []wantSample{{t: 1000, v: 1}, {t: 7000, v: 7}, {t: 20000, v: 20}},
		},
		{
			name:          "unsupported_encoding_falls_back_to_xor",
			floatEncoding: chunkenc.EncNone,
			base:          []chunks.Sample{sample{t: 1000, f: 1}, sample{t: 7000, f: 7}},
			overlap:       []chunks.Sample{sample{t: 2000, f: 2}, sample{t: 20000, f: 20}},
			wantEncodings: []chunkenc.Encoding{chunkenc.EncXOR},
			wantBounds:    [][2]int64{{1000, 20000}},
			wantSamples:   []wantSample{{t: 1000, v: 1}, {t: 7000, v: 7}, {t: 20000, v: 20}},
		},
		{
			name:          "non_overlapping_xor_stays_xor_under_xor2",
			floatEncoding: chunkenc.EncXOR2,
			base:          []chunks.Sample{sample{t: 1000, f: 1}, sample{t: 2000, f: 2}},
			overlap:       []chunks.Sample{sample{t: 5000, f: 5}, sample{t: 6000, f: 6}},
			wantEncodings: []chunkenc.Encoding{chunkenc.EncXOR, chunkenc.EncXOR},
			wantBounds:    [][2]int64{{1000, 2000}, {5000, 6000}},
			wantSamples:   []wantSample{{t: 1000, v: 1}, {t: 2000, v: 2}, {t: 5000, v: 5}, {t: 6000, v: 6}},
		},
		{
			name:          "identical_xor_chunks_skipped_without_reencoding",
			floatEncoding: chunkenc.EncXOR2,
			base:          []chunks.Sample{sample{t: 1000, f: 1}, sample{t: 2000, f: 2}},
			overlap:       []chunks.Sample{sample{t: 1000, f: 1}, sample{t: 2000, f: 2}},
			wantEncodings: []chunkenc.Encoding{chunkenc.EncXOR},
			wantBounds:    [][2]int64{{1000, 2000}},
			wantSamples:   []wantSample{{t: 1000, v: 1}, {t: 2000, v: 2}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			merged := NewChunkSeriesMergerWithFloatEncoding(tc.floatEncoding)(
				storage.NewListChunkSeriesFromSamples(lset, tc.base),
				storage.NewListChunkSeriesFromSamples(lset, tc.overlap),
			)
			metas, err := storage.ExpandChunks(merged.Iterator(nil))
			testutil.Ok(t, err)
			testutil.Equals(t, len(tc.wantEncodings), len(metas))

			var gotSamples []wantSample
			for i, m := range metas {
				testutil.Equals(t, tc.wantEncodings[i], m.Chunk.Encoding())
				testutil.Equals(t, tc.wantBounds[i][0], m.MinTime)
				testutil.Equals(t, tc.wantBounds[i][1], m.MaxTime)

				samples, err := storage.ExpandSamples(m.Chunk.Iterator(nil), nil)
				testutil.Ok(t, err)
				for _, s := range samples {
					gotSamples = append(gotSamples, wantSample{st: s.ST(), t: s.T(), v: s.F()})
				}
			}
			testutil.Equals(t, tc.wantSamples, gotSamples)
		})
	}

	t.Run("same_bytes_different_encoding_not_skipped", func(t *testing.T) {
		xorChk := chunkenc.NewXORChunk()
		app, err := xorChk.Appender()
		testutil.Ok(t, err)
		app.Append(0, 1000, 1)
		app.Append(0, 2000, 2)

		s1 := &storage.ChunkSeriesEntry{
			Lset: lset,
			ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
				return storage.NewListChunkSeriesIterator(chunks.Meta{MinTime: 1000, MaxTime: 2000, Chunk: xorChk})
			},
		}
		s2 := &storage.ChunkSeriesEntry{
			Lset: lset,
			ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
				return storage.NewListChunkSeriesIterator(chunks.Meta{
					MinTime: 1000,
					MaxTime: 2000,
					Chunk:   fakeEncodingChunk{Chunk: xorChk, enc: chunkenc.EncXOR2},
				})
			},
		}

		// Equal MinTime/MaxTime/Bytes with differing Encoding must not hit the 1:1 skip path,
		// so overlappingMerger re-encodes the chunk using the configured EncXOR2 target.
		merged := NewChunkSeriesMergerWithFloatEncoding(chunkenc.EncXOR2)(s1, s2)
		metas, err := storage.ExpandChunks(merged.Iterator(nil))
		testutil.Ok(t, err)
		testutil.Equals(t, 1, len(metas))
		testutil.Equals(t, chunkenc.EncXOR2, metas[0].Chunk.Encoding())
		testutil.Equals(t, int64(1000), metas[0].MinTime)
		testutil.Equals(t, int64(2000), metas[0].MaxTime)

		samples, err := storage.ExpandSamples(metas[0].Chunk.Iterator(nil), nil)
		testutil.Ok(t, err)
		testutil.Equals(t, 2, len(samples))
		testutil.Equals(t, int64(1000), samples[0].T())
		testutil.Equals(t, 1.0, samples[0].F())
		testutil.Equals(t, int64(2000), samples[1].T())
		testutil.Equals(t, 2.0, samples[1].F())
	})
}

func TestDedupChunkSeriesMergerDownsampledChunks(t *testing.T) {
	m := NewChunkSeriesMerger()

	defaultLabels := labels.FromStrings("bar", "baz")
	emptySamples := downsample.SamplesFromTSDBSamples([]chunks.Sample{})
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
					samples := [][]chunks.Sample{
						{sample{t: 299999, f: 3}, sample{t: 540000, f: 5}},
						{sample{t: 299999, f: 540000}, sample{t: 540000, f: 2100000}},
						{sample{t: 299999, f: 120000}, sample{t: 540000, f: 300000}},
						{sample{t: 299999, f: 240000}, sample{t: 540000, f: 540000}},
						{sample{t: 299999, f: 240000}, sample{t: 299999, f: 240000}},
					}
					var chks [5]chunkenc.Chunk
					for i, s := range samples {
						chk, err := chunks.ChunkFromSamples(s)
						testutil.Ok(t, err)
						chks[i] = chk.Chunk
					}
					return storage.NewListChunkSeriesIterator(chunks.Meta{
						MinTime: 299999,
						MaxTime: 540000,
						Chunk:   downsample.EncodeAggrChunk(chks),
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

type histoSample struct {
	t  int64
	st int64
	f  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func (h histoSample) T() int64 {
	return h.t
}

func (h histoSample) ST() int64 {
	return h.st
}

func (h histoSample) F() float64 {
	return h.f
}

func (h histoSample) H() *histogram.Histogram {
	return h.h
}

func (h histoSample) FH() *histogram.FloatHistogram {
	return h.fh
}

func (h histoSample) Type() chunkenc.ValueType {
	if h.fh != nil {
		return chunkenc.ValFloatHistogram
	}
	if h.h != nil {
		return chunkenc.ValHistogram
	}
	return chunkenc.ValFloat
}

func (h histoSample) Copy() chunks.Sample {
	c := histoSample{}
	if h.h != nil {
		c.h = h.h.Copy()
	}
	if h.fh != nil {
		c.fh = h.fh.Copy()
	}
	return c
}

var histogramSample = &histogram.Histogram{
	Schema:        0,
	Count:         20,
	Sum:           -3.1415,
	ZeroCount:     12,
	ZeroThreshold: 0.001,
	NegativeSpans: []histogram.Span{
		{Offset: 0, Length: 4},
		{Offset: 1, Length: 1},
	},
	NegativeBuckets:  []int64{1, 2, -2, 1, -1},
	CounterResetHint: histogram.UnknownCounterReset,
}

func TestDedupChunkSeriesMerger_Histogram(t *testing.T) {
	scrapeIntervalMilli := int64(30_000)

	testCases := []struct {
		name     string
		input    []storage.ChunkSeries
		expected storage.ChunkSeries
	}{
		{
			name: "two overlapping - Histogram and Histogram",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{
					histoSample{t: 0 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 2 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 3 * scrapeIntervalMilli, h: histogramSample},
				}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{
					histoSample{t: 1 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 2 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 3 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 4 * scrapeIntervalMilli, h: histogramSample},
				}),
			},
			expected: storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{
				histoSample{t: 0 * scrapeIntervalMilli, h: histogramSample},
				histoSample{t: 1 * scrapeIntervalMilli, h: histogramSample},
				histoSample{t: 2 * scrapeIntervalMilli, h: histogramSample},
				histoSample{t: 3 * scrapeIntervalMilli, h: histogramSample},
				histoSample{t: 4 * scrapeIntervalMilli, h: histogramSample},
			}),
		},
		{
			name: "overlapping mixed - XOR then Histogram - panic repro case",
			input: []storage.ChunkSeries{
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{
					histoSample{t: 1 * scrapeIntervalMilli, f: 1},
					histoSample{t: 2 * scrapeIntervalMilli, f: 1},
				}, []chunks.Sample{
					histoSample{t: 5 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 6 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 7 * scrapeIntervalMilli, h: histogramSample},
				}),
				storage.NewListChunkSeriesFromSamples(labels.FromStrings("bar", "baz"), []chunks.Sample{
					histoSample{t: 1 * scrapeIntervalMilli, f: 1},
					histoSample{t: 2 * scrapeIntervalMilli, f: 1},
				}, []chunks.Sample{
					histoSample{t: 5 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 6 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 7 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 8 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 9 * scrapeIntervalMilli, h: histogramSample},
				},
				),
			},
			expected: storage.NewListChunkSeriesFromSamples(
				labels.FromStrings("bar", "baz"),
				[]chunks.Sample{
					histoSample{t: 1 * scrapeIntervalMilli, f: 1},
					histoSample{t: 2 * scrapeIntervalMilli, f: 1},
				}, []chunks.Sample{
					histoSample{t: 5 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 6 * scrapeIntervalMilli, h: histogramSample},
					histoSample{t: 7 * scrapeIntervalMilli, h: histogramSample},
				},
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := NewChunkSeriesMerger()
			merged := m(tc.input...)
			testutil.Equals(t, labels.FromStrings("bar", "baz"), merged.Labels())
			actChks, actErr := storage.ExpandChunks(merged.Iterator(nil))
			testutil.Ok(t, actErr)

			expChks, expErr := storage.ExpandChunks(tc.expected.Iterator(nil))
			testutil.Ok(t, expErr)
			testutil.Equals(t, expChks, actChks)
		})
	}
}

func TestDedupChunkSeriesMerger_HistogramST(t *testing.T) {
	lset := labels.FromStrings("bar", "baz")
	intHist := histogramSample
	floatHist := histogramSample.ToFloat(nil)

	for _, tc := range []struct {
		name              string
		base              []chunks.Sample
		overlap           []chunks.Sample
		expectedEncodings []chunkenc.Encoding
		expectedTS        []int64
		expectedST        []int64
	}{
		{
			name: "overlapping HistogramST chunks",
			base: []chunks.Sample{
				histoSample{t: 0, st: 100, h: intHist},
				histoSample{t: 30000, st: 100, h: intHist},
			},
			overlap: []chunks.Sample{
				histoSample{t: 30000, st: 100, h: intHist},
				histoSample{t: 100000, st: 200, h: intHist},
			},
			expectedEncodings: []chunkenc.Encoding{chunkenc.EncHistogramST},
			expectedTS:        []int64{0, 30000, 100000},
			expectedST:        []int64{100, 100, 200},
		},
		{
			name: "HistogramST base overlapping non-ST Histogram",
			base: []chunks.Sample{
				histoSample{t: 0, st: 100, h: intHist},
				histoSample{t: 30000, st: 100, h: intHist},
			},
			overlap: []chunks.Sample{
				histoSample{t: 30000, h: intHist},
				histoSample{t: 100000, h: intHist},
			},
			expectedEncodings: []chunkenc.Encoding{chunkenc.EncHistogramST, chunkenc.EncHistogram},
			expectedTS:        []int64{0, 30000, 100000},
			expectedST:        []int64{100, 100, 0},
		},
		{
			name: "overlapping FloatHistogramST chunks",
			base: []chunks.Sample{
				histoSample{t: 0, st: 100, fh: floatHist},
				histoSample{t: 30000, st: 100, fh: floatHist},
			},
			overlap: []chunks.Sample{
				histoSample{t: 30000, st: 100, fh: floatHist},
				histoSample{t: 100000, st: 200, fh: floatHist},
			},
			expectedEncodings: []chunkenc.Encoding{chunkenc.EncFloatHistogramST},
			expectedTS:        []int64{0, 30000, 100000},
			expectedST:        []int64{100, 100, 200},
		},
		{
			name: "FloatHistogramST base overlapping non-ST FloatHistogram",
			base: []chunks.Sample{
				histoSample{t: 0, st: 100, fh: floatHist},
				histoSample{t: 30000, st: 100, fh: floatHist},
			},
			overlap: []chunks.Sample{
				histoSample{t: 30000, fh: floatHist},
				histoSample{t: 100000, fh: floatHist},
			},
			expectedEncodings: []chunkenc.Encoding{chunkenc.EncFloatHistogramST, chunkenc.EncFloatHistogram},
			expectedTS:        []int64{0, 30000, 100000},
			expectedST:        []int64{100, 100, 0},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			merged := NewChunkSeriesMerger()(
				storage.NewListChunkSeriesFromSamples(lset, tc.base),
				storage.NewListChunkSeriesFromSamples(lset, tc.overlap),
			)
			metas, err := storage.ExpandChunks(merged.Iterator(nil))
			testutil.Ok(t, err)
			gotEncodings := make([]chunkenc.Encoding, 0, len(metas))
			var samples []chunks.Sample
			for _, m := range metas {
				gotEncodings = append(gotEncodings, m.Chunk.Encoding())
				chunkSamples, err := storage.ExpandSamples(m.Chunk.Iterator(nil), nil)
				testutil.Ok(t, err)
				samples = append(samples, chunkSamples...)
			}
			testutil.Equals(t, tc.expectedEncodings, gotEncodings)
			testutil.Equals(t, len(tc.expectedTS), len(samples))
			for i, s := range samples {
				testutil.Equals(t, tc.expectedTS[i], s.T())
				testutil.Equals(t, tc.expectedST[i], s.ST())
			}
		})
	}
}

type startTimestampSample struct {
	sample
	st int64
}

func (s startTimestampSample) ST() int64 {
	return s.st
}

func (s startTimestampSample) Copy() chunks.Sample {
	return s
}

func createSamplesWithStep(start, numOfSamples, step int) []chunks.Sample {
	res := make([]chunks.Sample, numOfSamples)
	cur := start
	for i := range numOfSamples {
		res[i] = sample{t: int64(cur), f: float64(cur)}
		cur += step
	}

	return res
}
