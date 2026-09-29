// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func createHistogramChunk(t testing.TB, startTs int64, numSamples int, bucketCount int) *storepb.Chunk {
	t.Helper()

	c := chunkenc.NewHistogramChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	for i := 0; i < numSamples; i++ {
		ts := startTs + int64(i)*15000

		posSpans := []histogram.Span{{Offset: 0, Length: uint32(bucketCount)}}
		posBuckets := make([]int64, bucketCount)
		for b := range posBuckets {
			posBuckets[b] = int64(b + 1 + i)
		}

		h := &histogram.Histogram{
			Schema:          1,
			Count:           uint64(10 + i),
			Sum:             float64(100 + i),
			ZeroThreshold:   0.001,
			ZeroCount:       uint64(1 + i),
			PositiveSpans:   posSpans,
			PositiveBuckets: posBuckets,
		}

		_, _, app, err = app.AppendHistogram(nil, ts, h, true)
		require.NoError(t, err)
	}

	return &storepb.Chunk{
		Type: storepb.Chunk_HISTOGRAM,
		Data: c.Bytes(),
	}
}

func TestLazyChunkSeriesIteratorHistogramReuse(t *testing.T) {
	bucketCount := 10
	samplesPerChunk := 5

	chunks := []*storepb.Chunk{
		createHistogramChunk(t, 1000, samplesPerChunk, bucketCount),
		createHistogramChunk(t, 1000+int64(samplesPerChunk)*15000, samplesPerChunk, bucketCount),
		createHistogramChunk(t, 1000+int64(2*samplesPerChunk)*15000, samplesPerChunk, bucketCount),
		createHistogramChunk(t, 1000+int64(3*samplesPerChunk)*15000, samplesPerChunk, bucketCount),
	}

	it := newLazyChunkSeriesIterator(chunks)

	var timestamps []int64
	var histograms []*histogram.Histogram

	for it.Next() != chunkenc.ValNone {
		ts, h := it.AtHistogram(nil)
		timestamps = append(timestamps, ts)
		histograms = append(histograms, h)
	}
	require.NoError(t, it.Err())

	totalExpected := 4 * samplesPerChunk
	require.Equal(t, totalExpected, len(timestamps))

	for i := 1; i < len(timestamps); i++ {
		require.Greater(t, timestamps[i], timestamps[i-1])
	}

	for i, h := range histograms {
		require.NotNil(t, h)
		require.Equal(t, uint64(10+i%samplesPerChunk), h.Count)
		require.InDelta(t, float64(100+i%samplesPerChunk), h.Sum, 0.001)
		require.Equal(t, bucketCount, len(h.PositiveBuckets))
	}

	boundaryIdx := samplesPerChunk - 1
	require.Equal(t, int64(1000+int64(boundaryIdx)*15000), timestamps[boundaryIdx])
	require.Equal(t, int64(1000+int64(samplesPerChunk)*15000), timestamps[samplesPerChunk])
}

func TestLazyChunkSeriesIteratorSeekAcrossChunks(t *testing.T) {
	bucketCount := 5
	samplesPerChunk := 3

	chunks := []*storepb.Chunk{
		createHistogramChunk(t, 0, samplesPerChunk, bucketCount),
		createHistogramChunk(t, int64(samplesPerChunk)*15000, samplesPerChunk, bucketCount),
		createHistogramChunk(t, int64(2*samplesPerChunk)*15000, samplesPerChunk, bucketCount),
	}

	it := newLazyChunkSeriesIterator(chunks)

	targetTs := int64(samplesPerChunk)*15000 + 1
	valType := it.Seek(targetTs)
	require.NotEqual(t, chunkenc.ValNone, valType)

	ts := it.AtT()
	require.GreaterOrEqual(t, ts, targetTs)

	count := 1
	for it.Next() != chunkenc.ValNone {
		count++
	}
	require.NoError(t, it.Err())
	require.Greater(t, count, 0)
}

func TestLazyChunkSeriesIteratorFloatChunks(t *testing.T) {
	c := chunkenc.NewXORChunk()
	app, err := c.Appender()
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		app.Append(int64(i)*15000, float64(i)*1.5)
	}

	c2 := chunkenc.NewXORChunk()
	app2, err := c2.Appender()
	require.NoError(t, err)
	for i := 5; i < 10; i++ {
		app2.Append(int64(i)*15000, float64(i)*1.5)
	}

	chunks := []*storepb.Chunk{
		{Type: storepb.Chunk_XOR, Data: c.Bytes()},
		{Type: storepb.Chunk_XOR, Data: c2.Bytes()},
	}

	it := newLazyChunkSeriesIterator(chunks)

	var values []float64
	for it.Next() != chunkenc.ValNone {
		_, v := it.At()
		values = append(values, v)
	}
	require.NoError(t, it.Err())
	require.Equal(t, 10, len(values))

	for i, v := range values {
		require.InDelta(t, float64(i)*1.5, v, 0.001)
	}
}

func TestChunkSeriesIteratorPreCreated(t *testing.T) {
	bucketCount := 10
	samplesPerChunk := 5

	chunk1 := createHistogramChunk(t, 1000, samplesPerChunk, bucketCount)
	chunk2 := createHistogramChunk(t, 1000+int64(samplesPerChunk)*15000, samplesPerChunk, bucketCount)
	chunk3 := createHistogramChunk(t, 1000+int64(2*samplesPerChunk)*15000, samplesPerChunk, bucketCount)

	// Pre-create iterators (original path — no reuse, nil passed)
	its := []chunkenc.Iterator{
		getFirstIterator(chunk1),
		getFirstIterator(chunk2),
		getFirstIterator(chunk3),
	}

	it := newChunkSeriesIterator(its)

	var timestamps []int64
	var histograms []*histogram.Histogram

	for it.Next() != chunkenc.ValNone {
		ts, h := it.AtHistogram(nil)
		timestamps = append(timestamps, ts)
		histograms = append(histograms, h)
	}
	require.NoError(t, it.Err())

	totalExpected := 3 * samplesPerChunk
	require.Equal(t, totalExpected, len(timestamps))

	for i := 1; i < len(timestamps); i++ {
		require.Greater(t, timestamps[i], timestamps[i-1])
	}

	for i, h := range histograms {
		require.NotNil(t, h)
		require.Equal(t, uint64(10+i%samplesPerChunk), h.Count)
		require.InDelta(t, float64(100+i%samplesPerChunk), h.Sum, 0.001)
		require.Equal(t, bucketCount, len(h.PositiveBuckets))
	}
}
