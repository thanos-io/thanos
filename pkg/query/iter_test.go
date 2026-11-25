// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
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

		_, _, app, err = app.AppendHistogram(nil, 0, ts, h, true)
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
	fh := &histogram.FloatHistogram{}
	var i int
	for it.Next() != chunkenc.ValNone {
		var ts int64
		ts, fh = it.AtFloatHistogram(fh)
		timestamps = append(timestamps, ts)

		require.NotNil(t, fh)
		require.InDelta(t, float64(10+i%samplesPerChunk), fh.Count, 0.001)
		require.InDelta(t, float64(100+i%samplesPerChunk), fh.Sum, 0.001)
		require.Equal(t, bucketCount, len(fh.PositiveBuckets))
		i++
	}
	require.NoError(t, it.Err())

	totalExpected := 4 * samplesPerChunk
	require.Equal(t, totalExpected, len(timestamps))

	for i := 1; i < len(timestamps); i++ {
		require.Greater(t, timestamps[i], timestamps[i-1])
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
		app.Append(0, int64(i)*15000, float64(i)*1.5)
	}

	c2 := chunkenc.NewXORChunk()
	app2, err := c2.Appender()
	require.NoError(t, err)
	for i := 5; i < 10; i++ {
		app2.Append(0, int64(i)*15000, float64(i)*1.5)
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
	fh := &histogram.FloatHistogram{}
	var i int

	for it.Next() != chunkenc.ValNone {
		var ts int64
		ts, fh = it.AtFloatHistogram(fh)
		timestamps = append(timestamps, ts)

		require.NotNil(t, fh)
		require.InDelta(t, float64(10+i%samplesPerChunk), fh.Count, 0.001)
		require.InDelta(t, float64(100+i%samplesPerChunk), fh.Sum, 0.001)
		require.Equal(t, bucketCount, len(fh.PositiveBuckets))
		i++
	}
	require.NoError(t, it.Err())

	totalExpected := 3 * samplesPerChunk
	require.Equal(t, totalExpected, len(timestamps))

	for i := 1; i < len(timestamps); i++ {
		require.Greater(t, timestamps[i], timestamps[i-1])
	}
}

func TestNativeHistogramDedup(t *testing.T) {
	cases := []struct {
		samples []sample
		tcase   string
	}{
		{
			tcase: "unknown counter reset chunk",
			samples: []sample{
				{t: 10000, h: makeHistWithHint(1, histogram.UnknownCounterReset)},
				{t: 20000, h: makeHistWithHint(2, histogram.UnknownCounterReset)},
			},
		},
		{
			tcase: "not counter reset chunk",
			samples: []sample{
				{t: 10000, h: makeHistWithHint(1, histogram.NotCounterReset)},
				{t: 20000, h: makeHistWithHint(2, histogram.NotCounterReset)},
			},
		},
		{
			tcase: "counter reset chunk",
			samples: []sample{
				{t: 10000, h: makeHistWithHint(1, histogram.CounterReset)},
				{t: 20000, h: makeHistWithHint(2, histogram.NotCounterReset)},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.tcase, func(t *testing.T) {
			// When the first sample is read, the counter reset hint for the second sample
			// does not need to be reset.
			t.Run("read_first_sample", func(t *testing.T) {
				it := newHistogramResetDetector(newMockedSeriesIterator(c.samples))
				it.Next()
				_, h := it.AtHistogram(nil)
				testutil.Equals(t, c.samples[0].h.CounterResetHint, h.CounterResetHint)

				it.Next()
				_, h = it.AtHistogram(nil)
				testutil.Equals(t, c.samples[1].h.CounterResetHint, h.CounterResetHint)
			})

			// When the first sample is not read, the counter reset hint for the second
			// sample should be reset.
			t.Run("skip_first_sample", func(t *testing.T) {
				it := newHistogramResetDetector(newMockedSeriesIterator(c.samples))
				it.Next()
				it.Next()
				_, h := it.AtHistogram(nil)
				testutil.Equals(t, h.CounterResetHint, histogram.UnknownCounterReset)
			})
		})
	}
}

func makeHistWithHint(i int64, hint histogram.CounterResetHint) *histogram.Histogram {
	h := tsdbutil.GenerateTestHistogram(i)
	h.CounterResetHint = hint
	return h
}

func TestChunkSeriesIterator_XOR2(t *testing.T) {
	t.Parallel()

	xorChk := chunkenc.NewXORChunk()
	xorApp, err := xorChk.Appender()
	require.NoError(t, err)
	xorApp.Append(0, 1000, 1.0)
	xorApp.Append(0, 2000, 2.0)

	xor2Chk := chunkenc.NewXOR2Chunk()
	xor2App, err := xor2Chk.Appender()
	require.NoError(t, err)
	xor2App.Append(2500, 3000, 3.0)
	xor2App.Append(2500, 4000, 4.0)

	type wantSample struct {
		st, t int64
		v     float64
	}
	wantMixed := []wantSample{
		{st: 0, t: 1000, v: 1.0},
		{st: 0, t: 2000, v: 2.0},
		{st: 2500, t: 3000, v: 3.0},
		{st: 2500, t: 4000, v: 4.0},
	}

	assertSamples := func(t *testing.T, it chunkenc.Iterator, want []wantSample) {
		t.Helper()
		var got []wantSample
		for it.Next() != chunkenc.ValNone {
			ts, v := it.At()
			got = append(got, wantSample{st: it.AtST(), t: ts, v: v})
		}
		require.NoError(t, it.Err())
		require.Equal(t, want, got)
	}

	t.Run("lazy_mixed_xor_and_xor2", func(t *testing.T) {
		it := newLazyChunkSeriesIterator([]*storepb.Chunk{
			{Type: storepb.Chunk_XOR, Data: xorChk.Bytes()},
			{Type: storepb.Chunk_XOR2, Data: xor2Chk.Bytes()},
		})
		assertSamples(t, it, wantMixed)
	})

	t.Run("lazy_xor2_reuse_and_st_reset", func(t *testing.T) {
		xor2NoST := chunkenc.NewXOR2Chunk()
		a, err := xor2NoST.Appender()
		require.NoError(t, err)
		a.Append(0, 5000, 5.0)
		a.Append(0, 6000, 6.0)

		xorTail := chunkenc.NewXORChunk()
		a, err = xorTail.Appender()
		require.NoError(t, err)
		a.Append(0, 7000, 7.0)

		it := newLazyChunkSeriesIterator([]*storepb.Chunk{
			{Type: storepb.Chunk_XOR2, Data: xor2Chk.Bytes()},
			{Type: storepb.Chunk_XOR2, Data: xor2NoST.Bytes()},
			{Type: storepb.Chunk_XOR, Data: xorTail.Bytes()},
		})
		assertSamples(t, it, []wantSample{
			{st: 2500, t: 3000, v: 3.0},
			{st: 2500, t: 4000, v: 4.0},
			{st: 0, t: 5000, v: 5.0},
			{st: 0, t: 6000, v: 6.0},
			{st: 0, t: 7000, v: 7.0},
		})
	})

	t.Run("seek_and_boundaries", func(t *testing.T) {
		mkXOR2 := func(smpls ...wantSample) *storepb.Chunk {
			c := chunkenc.NewXOR2Chunk()
			a, err := c.Appender()
			require.NoError(t, err)
			for _, s := range smpls {
				a.Append(s.st, s.t, s.v)
			}
			return &storepb.Chunk{Type: storepb.Chunk_XOR2, Data: c.Bytes()}
		}

		// Fresh Seek(t <= 0) must not treat an iterator's initial t=0 as a valid sample.
		for _, tc := range []struct {
			name string
			it   func() chunkenc.Iterator
		}{
			{
				name: "lazy",
				it: func() chunkenc.Iterator {
					return newLazyChunkSeriesIterator([]*storepb.Chunk{
						mkXOR2(wantSample{st: -20, t: -10, v: 1.0}, wantSample{st: -20, t: 0, v: 2.0}),
						mkXOR2(wantSample{st: 5, t: 10, v: 3.0}),
					})
				},
			},
			{
				name: "chunk_series",
				it: func() chunkenc.Iterator {
					return newChunkSeriesIterator([]chunkenc.Iterator{
						getFirstIterator(mkXOR2(wantSample{st: -20, t: -10, v: 1.0}, wantSample{st: -20, t: 0, v: 2.0})),
						getFirstIterator(mkXOR2(wantSample{st: 5, t: 10, v: 3.0})),
					})
				},
			},
		} {
			t.Run(tc.name+"_seek_before_next", func(t *testing.T) {
				it := tc.it()
				require.Equal(t, chunkenc.ValFloat, it.Seek(-15))
				ts, v := it.At()
				require.Equal(t, int64(-10), ts)
				require.Equal(t, 1.0, v)
				require.Equal(t, int64(-20), it.AtST())

				require.Equal(t, chunkenc.ValFloat, it.Seek(0))
				ts, v = it.At()
				require.Equal(t, int64(0), ts)
				require.Equal(t, 2.0, v)

				require.Equal(t, chunkenc.ValFloat, it.Seek(5))
				ts, v = it.At()
				require.Equal(t, int64(10), ts)
				require.Equal(t, 3.0, v)
				require.Equal(t, int64(5), it.AtST())

				require.Equal(t, chunkenc.ValNone, it.Next())
				// Seek after exhaustion must stay exhausted rather than returning stale lastVal.
				require.Equal(t, chunkenc.ValNone, it.Seek(0))
				require.NoError(t, it.Err())
			})
		}

		t.Run("empty_leading_and_negative_transition", func(t *testing.T) {
			it := newLazyChunkSeriesIterator([]*storepb.Chunk{
				mkXOR2(),
				mkXOR2(wantSample{st: -30, t: -10, v: 1.0}),
				mkXOR2(wantSample{st: -10, t: -5, v: 2.0}, wantSample{st: 0, t: 5, v: 3.0}),
			})
			assertSamples(t, it, []wantSample{
				{st: -30, t: -10, v: 1.0},
				{st: -10, t: -5, v: 2.0},
				{st: 0, t: 5, v: 3.0},
			})
		})

		t.Run("overlapping_xor_and_xor2", func(t *testing.T) {
			it := newLazyChunkSeriesIterator([]*storepb.Chunk{
				{Type: storepb.Chunk_XOR, Data: xorChk.Bytes()},
				mkXOR2(
					wantSample{st: 1500, t: 2000, v: 99.0},
					wantSample{st: 2500, t: 3000, v: 3.0},
				),
			})
			assertSamples(t, it, []wantSample{
				{st: 0, t: 1000, v: 1.0},
				{st: 0, t: 2000, v: 2.0},
				{st: 2500, t: 3000, v: 3.0},
			})
		})
	})

	t.Run("chunk_series_raw_xor2", func(t *testing.T) {
		aggrChunks := []storepb.AggrChunk{
			{MinTime: 1000, MaxTime: 2000, Raw: &storepb.Chunk{Type: storepb.Chunk_XOR, Data: xorChk.Bytes()}},
			{MinTime: 3000, MaxTime: 4000, Raw: &storepb.Chunk{Type: storepb.Chunk_XOR2, Data: xor2Chk.Bytes()}},
		}
		for _, aggrs := range [][]storepb.Aggr{
			{storepb.Aggr_COUNT},
			{storepb.Aggr_SUM, storepb.Aggr_COUNT},
		} {
			series := newChunkSeries(labels.FromStrings("__name__", "xor2_metric"), aggrChunks, 1000, 4000, aggrs)
			assertSamples(t, series.Iterator(nil), wantMixed)
		}

		// Mix a raw XOR2 chunk with a downsampled Sum/Count chunk to cover the allRaw == false branch.
		sumChk := chunkenc.NewXORChunk()
		sumApp, err := sumChk.Appender()
		require.NoError(t, err)
		sumApp.Append(0, 5000, 20.0)
		cntChk := chunkenc.NewXORChunk()
		cntApp, err := cntChk.Appender()
		require.NoError(t, err)
		cntApp.Append(0, 5000, 4.0)
		mixedAvgChunks := []storepb.AggrChunk{
			{MinTime: 3000, MaxTime: 4000, Raw: &storepb.Chunk{Type: storepb.Chunk_XOR2, Data: xor2Chk.Bytes()}},
			{
				MinTime: 5000,
				MaxTime: 5000,
				Sum:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: sumChk.Bytes()},
				Count:   &storepb.Chunk{Type: storepb.Chunk_XOR, Data: cntChk.Bytes()},
			},
		}
		mixedAvgSeries := newChunkSeries(labels.FromStrings("__name__", "xor2_metric"), mixedAvgChunks, 3000, 5000, []storepb.Aggr{storepb.Aggr_SUM, storepb.Aggr_COUNT})
		assertSamples(t, mixedAvgSeries.Iterator(nil), []wantSample{
			{st: 2500, t: 3000, v: 3.0},
			{st: 2500, t: 4000, v: 4.0},
			{st: 0, t: 5000, v: 5.0},
		})

		// Aggr_COUNTER wraps chunks in ApplyCounterResetsIterator which synthesizes monotonic values (st=0).
		resetXOR2 := chunkenc.NewXOR2Chunk()
		resetApp, err := resetXOR2.Appender()
		require.NoError(t, err)
		resetApp.Append(2500, 3000, 1.0)
		resetApp.Append(2500, 4000, 3.0)
		counterChunks := []storepb.AggrChunk{
			{MinTime: 1000, MaxTime: 2000, Raw: &storepb.Chunk{Type: storepb.Chunk_XOR, Data: xorChk.Bytes()}},
			{MinTime: 3000, MaxTime: 4000, Raw: &storepb.Chunk{Type: storepb.Chunk_XOR2, Data: resetXOR2.Bytes()}},
		}
		counterSeries := newChunkSeries(labels.FromStrings("__name__", "xor2_metric"), counterChunks, 1000, 4000, []storepb.Aggr{storepb.Aggr_COUNTER})
		assertSamples(t, counterSeries.Iterator(nil), []wantSample{
			{st: 0, t: 1000, v: 1.0},
			{st: 0, t: 2000, v: 2.0},
			{st: 0, t: 3000, v: 3.0},
			{st: 0, t: 4000, v: 5.0},
		})
	})

	t.Run("get_first_iterator_xor2", func(t *testing.T) {
		it := getFirstIterator(&storepb.Chunk{Type: storepb.Chunk_XOR2, Data: xor2Chk.Bytes()})
		assertSamples(t, it, wantMixed[2:])
	})

	t.Run("invalid_and_truncated_chunks", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			chunk *storepb.Chunk
		}{
			{name: "unknown_encoding", chunk: &storepb.Chunk{Type: storepb.Chunk_Encoding(99), Data: []byte{0, 1, 2}}},
			{name: "truncated_xor2", chunk: &storepb.Chunk{Type: storepb.Chunk_XOR2, Data: []byte{0, 1}}},
			{name: "truncated_xor", chunk: &storepb.Chunk{Type: storepb.Chunk_XOR, Data: []byte{0}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				lazyIt := newLazyChunkSeriesIterator([]*storepb.Chunk{tc.chunk})
				require.Equal(t, chunkenc.ValNone, lazyIt.Next())
				require.Error(t, lazyIt.Err())

				firstIt := getFirstIterator(tc.chunk)
				require.Equal(t, chunkenc.ValNone, firstIt.Next())
				require.Error(t, firstIt.Err())
			})
		}
	})
}
