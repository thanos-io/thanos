// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package querier

import (
	"math"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/thanos-io/thanos/internal/cortex/util"
)

func TestBlockQuerierSeries(t *testing.T) {
	t.Parallel()

	// Init some test fixtures
	minTimestamp := time.Unix(1, 0)
	maxTimestamp := time.Unix(10, 0)

	tests := map[string]struct {
		series          *storepb.Series
		expectedMetric  labels.Labels
		expectedSamples []model.SamplePair
		expectedErr     string
	}{
		"empty series": {
			series:         &storepb.Series{},
			expectedMetric: labels.Labels(nil),
			expectedErr:    "no chunks",
		},
		"should return series on success": {
			series: &storepb.Series{
				Labels: []labelpb.ZLabel{
					{Name: "foo", Value: "bar"},
				},
				Chunks: []storepb.AggrChunk{
					{MinTime: minTimestamp.Unix() * 1000, MaxTime: maxTimestamp.Unix() * 1000, Raw: &storepb.Chunk{Type: storepb.Chunk_XOR, Data: mockTSDBChunkData()}},
				},
			},
			expectedMetric: labels.Labels{
				{Name: "foo", Value: "bar"},
			},
			expectedSamples: []model.SamplePair{
				{Timestamp: model.TimeFromUnixNano(time.Unix(1, 0).UnixNano()), Value: model.SampleValue(1)},
				{Timestamp: model.TimeFromUnixNano(time.Unix(2, 0).UnixNano()), Value: model.SampleValue(2)},
			},
		},
		"should return error on failure while reading encoded chunk data": {
			series: &storepb.Series{
				Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}},
				Chunks: []storepb.AggrChunk{
					{MinTime: minTimestamp.Unix() * 1000, MaxTime: maxTimestamp.Unix() * 1000, Raw: &storepb.Chunk{Type: storepb.Chunk_XOR, Data: []byte{0, 1}}},
				},
			},
			expectedMetric: labels.Labels{labels.Label{Name: "foo", Value: "bar"}},
			expectedErr:    `cannot iterate chunk for series: {foo="bar"}: EOF`,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			series := newBlockQuerierSeries(labelpb.ZLabelsToPromLabels(testData.series.Labels), testData.series.Chunks)

			assert.Equal(t, testData.expectedMetric, series.Labels())

			sampleIx := 0

			it := series.Iterator()
			for it.Next() != chunkenc.ValNone {
				ts, val := it.At()
				require.True(t, sampleIx < len(testData.expectedSamples))
				assert.Equal(t, int64(testData.expectedSamples[sampleIx].Timestamp), ts)
				assert.Equal(t, float64(testData.expectedSamples[sampleIx].Value), val)
				sampleIx++
			}
			// make sure we've got all expected samples
			require.Equal(t, sampleIx, len(testData.expectedSamples))

			if testData.expectedErr != "" {
				require.EqualError(t, it.Err(), testData.expectedErr)
			} else {
				require.NoError(t, it.Err())
			}
		})
	}
}

func mockTSDBChunkData() []byte {
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		panic(err)
	}

	appender.Append(time.Unix(1, 0).Unix()*1000, 1)
	appender.Append(time.Unix(2, 0).Unix()*1000, 2)

	return chunk.Bytes()
}

func TestBlockQuerierSeriesSet(t *testing.T) {
	now := time.Now()

	// It would be possible to split this test into smaller parts, but I prefer to keep
	// it as is, to also test transitions between series.

	bss := &blockQuerierSeriesSet{
		series: []*storepb.Series{
			// first, with one chunk.
			{
				Labels: mkZLabels("__name__", "first", "a", "a"),
				Chunks: []storepb.AggrChunk{
					createAggrChunkWithSineSamples(now, now.Add(100*time.Second), 3*time.Millisecond), // ceil(100 / 0.003) samples (= 33334)
				},
			},

			// continuation of previous series. Must have exact same labels.
			{
				Labels: mkZLabels("__name__", "first", "a", "a"),
				Chunks: []storepb.AggrChunk{
					createAggrChunkWithSineSamples(now.Add(100*time.Second), now.Add(200*time.Second), 3*time.Millisecond), // ceil(100 / 0.003) samples more, 66668 in total
				},
			},

			// second, with multiple chunks
			{
				Labels: mkZLabels("__name__", "second"),
				Chunks: []storepb.AggrChunk{
					// unordered chunks
					createAggrChunkWithSineSamples(now.Add(400*time.Second), now.Add(600*time.Second), 5*time.Millisecond), // 200 / 0.005 (= 40000 samples, = 120000 in total)
					createAggrChunkWithSineSamples(now.Add(200*time.Second), now.Add(400*time.Second), 5*time.Millisecond), // 200 / 0.005 (= 40000 samples)
					createAggrChunkWithSineSamples(now, now.Add(200*time.Second), 5*time.Millisecond),                      // 200 / 0.005 (= 40000 samples)
				},
			},

			// overlapping
			{
				Labels: mkZLabels("__name__", "overlapping"),
				Chunks: []storepb.AggrChunk{
					createAggrChunkWithSineSamples(now, now.Add(10*time.Second), 5*time.Millisecond), // 10 / 0.005 = 2000 samples
				},
			},
			{
				Labels: mkZLabels("__name__", "overlapping"),
				Chunks: []storepb.AggrChunk{
					// 10 / 0.005 = 2000 samples, but first 1000 are overlapping with previous series, so this chunk only contributes 1000
					createAggrChunkWithSineSamples(now.Add(5*time.Second), now.Add(15*time.Second), 5*time.Millisecond),
				},
			},

			// overlapping 2. Chunks here come in wrong order.
			{
				Labels: mkZLabels("__name__", "overlapping2"),
				Chunks: []storepb.AggrChunk{
					// entire range overlaps with the next chunk, so this chunks contributes 0 samples (it will be sorted as second)
					createAggrChunkWithSineSamples(now.Add(3*time.Second), now.Add(7*time.Second), 5*time.Millisecond),
				},
			},
			{
				Labels: mkZLabels("__name__", "overlapping2"),
				Chunks: []storepb.AggrChunk{
					// this chunk has completely overlaps previous chunk. Since its minTime is lower, it will be sorted as first.
					createAggrChunkWithSineSamples(now, now.Add(10*time.Second), 5*time.Millisecond), // 10 / 0.005 = 2000 samples
				},
			},
			{
				Labels: mkZLabels("__name__", "overlapping2"),
				Chunks: []storepb.AggrChunk{
					// no samples
					createAggrChunkWithSineSamples(now, now, 5*time.Millisecond),
				},
			},

			{
				Labels: mkZLabels("__name__", "overlapping2"),
				Chunks: []storepb.AggrChunk{
					// 2000 samples more (10 / 0.005)
					createAggrChunkWithSineSamples(now.Add(20*time.Second), now.Add(30*time.Second), 5*time.Millisecond),
				},
			},
		},
	}

	verifyNextSeries(t, bss, labels.FromStrings("__name__", "first", "a", "a"), 66668)
	verifyNextSeries(t, bss, labels.FromStrings("__name__", "second"), 120000)
	verifyNextSeries(t, bss, labels.FromStrings("__name__", "overlapping"), 3000)
	verifyNextSeries(t, bss, labels.FromStrings("__name__", "overlapping2"), 4000)
	require.False(t, bss.Next())
}

func verifyNextSeries(t *testing.T, ss storage.SeriesSet, labels labels.Labels, samples int) {
	require.True(t, ss.Next())

	s := ss.At()
	require.Equal(t, labels, s.Labels())

	prevTS := int64(0)
	count := 0
	for it := s.Iterator(); it.Next() != chunkenc.ValNone; {
		count++
		ts, v := it.At()
		require.Equal(t, math.Sin(float64(ts)), v)
		require.Greater(t, ts, prevTS, "timestamps are increasing")
		prevTS = ts
	}

	require.Equal(t, samples, count)
}

func createAggrChunkWithSineSamples(minTime, maxTime time.Time, step time.Duration) storepb.AggrChunk {
	var samples []promql.Point

	minT := minTime.Unix() * 1000
	maxT := maxTime.Unix() * 1000
	stepMillis := step.Milliseconds()

	for t := minT; t < maxT; t += stepMillis {
		samples = append(samples, promql.Point{T: t, V: math.Sin(float64(t))})
	}

	return createAggrChunk(minT, maxT, samples...)
}

func createAggrChunkWithSamples(samples ...promql.Point) storepb.AggrChunk {
	return createAggrChunk(samples[0].T, samples[len(samples)-1].T, samples...)
}

func createAggrChunk(minTime, maxTime int64, samples ...promql.Point) storepb.AggrChunk {
	// Ensure samples are sorted by timestamp.
	sort.Slice(samples, func(i, j int) bool {
		return samples[i].T < samples[j].T
	})

	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		panic(err)
	}

	for _, s := range samples {
		appender.Append(s.T, s.V)
	}

	return storepb.AggrChunk{
		MinTime: minTime,
		MaxTime: maxTime,
		Raw: &storepb.Chunk{
			Type: storepb.Chunk_XOR,
			Data: chunk.Bytes(),
		},
	}
}

func mkZLabels(s ...string) []labelpb.ZLabel {
	var result []labelpb.ZLabel

	for i := 0; i+1 < len(s); i = i + 2 {
		result = append(result, labelpb.ZLabel{
			Name:  s[i],
			Value: s[i+1],
		})
	}

	return result
}

func mkLabels(s ...string) []labels.Label {
	return labelpb.ZLabelsToPromLabels(mkZLabels(s...))
}

func Benchmark_newBlockQuerierSeries(b *testing.B) {
	lbls := mkLabels(
		"__name__", "test",
		"label_1", "value_1",
		"label_2", "value_2",
		"label_3", "value_3",
		"label_4", "value_4",
		"label_5", "value_5",
		"label_6", "value_6",
		"label_7", "value_7",
		"label_8", "value_8",
		"label_9", "value_9")

	chunks := []storepb.AggrChunk{
		createAggrChunkWithSineSamples(time.Now(), time.Now().Add(-time.Hour), time.Minute),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newBlockQuerierSeries(lbls, chunks)
	}
}

func Benchmark_blockQuerierSeriesSet_iteration(b *testing.B) {
	const (
		numSeries          = 8000
		numSamplesPerChunk = 240
		numChunksPerSeries = 24
	)

	// Generate series.
	series := make([]*storepb.Series, 0, numSeries)
	for seriesID := 0; seriesID < numSeries; seriesID++ {
		lbls := mkZLabels("__name__", "test", "series_id", strconv.Itoa(seriesID))
		chunks := make([]storepb.AggrChunk, 0, numChunksPerSeries)

		// Create chunks with 1 sample per second.
		for minT := int64(0); minT < numChunksPerSeries*numSamplesPerChunk; minT += numSamplesPerChunk {
			chunks = append(chunks, createAggrChunkWithSineSamples(util.TimeFromMillis(minT), util.TimeFromMillis(minT+numSamplesPerChunk), time.Millisecond))
		}

		series = append(series, &storepb.Series{
			Labels: lbls,
			Chunks: chunks,
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		set := blockQuerierSeriesSet{series: series}

		for set.Next() {
			for t := set.At().Iterator(); t.Next() != chunkenc.ValNone; {
				t.At()
			}
		}
	}
}
