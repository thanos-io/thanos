// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func pooledTestSeries(prefix string, n, labels int) []prompb.TimeSeries {
	ts := make([]prompb.TimeSeries, 0, n)
	for i := range n {
		s := prompb.TimeSeries{}
		s.Labels = append(s.Labels, labelpb.ZLabel{Name: "__name__", Value: fmt.Sprintf("%s_metric_%d", prefix, i)})
		for j := range labels - 1 {
			s.Labels = append(s.Labels, labelpb.ZLabel{Name: fmt.Sprintf("label_%02d", j), Value: fmt.Sprintf("%s-%d-%d", prefix, i, j)})
		}
		for j := range i%3 + 1 {
			s.Samples = append(s.Samples, prompb.Sample{Value: float64(i), Timestamp: int64(i*10 + j)})
		}
		if i%4 == 0 {
			s.Exemplars = append(s.Exemplars, prompb.Exemplar{
				Labels:    []labelpb.ZLabel{{Name: "trace_id", Value: fmt.Sprintf("%s-trace-%d", prefix, i)}},
				Value:     1,
				Timestamp: int64(i),
			})
		}
		if i%5 == 0 {
			s.Histograms = append(s.Histograms, prompb.HistogramToHistogramProto(int64(i), &histogram.Histogram{
				Count:           5,
				Sum:             float64(i),
				Schema:          1,
				PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
				PositiveBuckets: []int64{2, 1},
			}))
		}
		ts = append(ts, s)
	}
	return ts
}

func TestWriteRequestUnmarshalPooled(t *testing.T) {
	reqs := []*WriteRequest{
		{Tenant: "a", Replica: 1, Timeseries: pooledTestSeries("a", 50, 10)},
		{Tenant: "b", Replica: 2, Timeseries: pooledTestSeries("b", 7, 3)},
		{Tenant: "c", Timeseries: pooledTestSeries("c", 120, 20)},
		{Tenant: "d", Replica: 3, TimeseriesTenantData: []TimeSeriesTenantTuple{
			{Tenant: "d1", Timeseries: pooledTestSeries("d1", 5, 4)},
			{Tenant: "d2", Timeseries: pooledTestSeries("d2", 30, 12)},
		}},
		{Tenant: "f", TimeseriesTenantData: []TimeSeriesTenantTuple{
			{Tenant: "f1", Timeseries: pooledTestSeries("f1", 2, 2)},
		}},
		{Tenant: "g", Timeseries: pooledTestSeries("g", 4, 3), TimeseriesTenantData: []TimeSeriesTenantTuple{
			{Timeseries: pooledTestSeries("g1", 6, 5)},
		}},
		{Tenant: "e"},
	}

	for round := range 3 {
		for i, want := range reqs {
			raw, err := want.Marshal()
			require.NoError(t, err)

			got := &WriteRequest{}
			require.NoError(t, got.UnmarshalPooled(raw))

			gotRaw, err := got.Marshal()
			require.NoError(t, err)
			require.Equal(t, raw, gotRaw, "round %d request %d", round, i)
			got.ReleasePooled()
			require.Nil(t, got.Timeseries)
		}
	}
}

func TestWriteRequestReleasePooledKeepsExemplarLabels(t *testing.T) {
	first, err := (&WriteRequest{Timeseries: pooledTestSeries("first", 8, 3)}).Marshal()
	require.NoError(t, err)
	second, err := (&WriteRequest{Timeseries: pooledTestSeries("second", 8, 3)}).Marshal()
	require.NoError(t, err)

	req := &WriteRequest{}
	require.NoError(t, req.UnmarshalPooled(first))
	kept := req.Timeseries[4].Exemplars[0].Labels
	labelpb.ReAllocZLabelsStrings(&kept)
	req.ReleasePooled()

	for range 10 {
		req := &WriteRequest{}
		require.NoError(t, req.UnmarshalPooled(second))
		req.ReleasePooled()
	}
	require.Equal(t, []labelpb.ZLabel{{Name: "trace_id", Value: "first-trace-4"}}, kept)
}

func TestWriteRequestUnmarshalPooledError(t *testing.T) {
	raw, err := (&WriteRequest{Tenant: "a", Timeseries: pooledTestSeries("a", 10, 5)}).Marshal()
	require.NoError(t, err)

	for _, data := range [][]byte{raw[:len(raw)/2], raw[:len(raw)-1], {0x0a, 0xff}} {
		req := &WriteRequest{}
		require.Error(t, req.UnmarshalPooled(data))
		req.ReleasePooled()
	}
}

func BenchmarkWriteRequestUnmarshal(b *testing.B) {
	for _, req := range []struct {
		name string
		req  *WriteRequest
	}{
		{"timeseries", &WriteRequest{Tenant: "a", Timeseries: pooledTestSeries("a", 500, 15)}},
		{"tenant-data", &WriteRequest{TimeseriesTenantData: []TimeSeriesTenantTuple{{Tenant: "a", Timeseries: pooledTestSeries("a", 500, 15)}}}},
	} {
		raw, err := req.req.Marshal()
		require.NoError(b, err)
		b.Run(req.name, func(b *testing.B) { benchmarkWriteRequestUnmarshal(b, raw) })
	}
}

func benchmarkWriteRequestUnmarshal(b *testing.B, raw []byte) {
	b.Run("pooled=false", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			req := &WriteRequest{}
			if err := req.Unmarshal(raw); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("pooled=true", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			req := &WriteRequest{}
			if err := req.UnmarshalPooled(raw); err != nil {
				b.Fatal(err)
			}
			req.ReleasePooled()
		}
	})
}

func TestResetTimeseriesDropsOversizedSlices(t *testing.T) {
	require.Nil(t, resetTimeseries(make([]prompb.TimeSeries, 1, maxPooledSeries+1)))

	ts := resetTimeseries([]prompb.TimeSeries{
		{Labels: make([]labelpb.ZLabel, 1, maxPooledLabelsPerSeries+1), Samples: make([]prompb.Sample, 1, maxPooledSamplesPerSeries+1)},
		{Labels: make([]labelpb.ZLabel, 1, 4), Samples: make([]prompb.Sample, 1, 4)},
	})[:2]
	require.Nil(t, ts[0].Labels)
	require.Nil(t, ts[0].Samples)
	require.Equal(t, 4, cap(ts[1].Labels))
	require.Equal(t, 4, cap(ts[1].Samples))
}
