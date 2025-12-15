// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestWriteRequestUnmarshalZeroCopy(t *testing.T) {
	// Create a WriteRequest with multiple time series
	original := &WriteRequest{
		Tenant:  "test-tenant",
		Replica: 1,
		Timeseries: []*prompb.TimeSeries{
			{
				Labels: []*labelpb.Label{
					{Name: "__name__", Value: "test_metric"},
					{Name: "instance", Value: "localhost:9090"},
					{Name: "job", Value: "test"},
				},
				Samples: []*prompb.Sample{
					{Value: 1.0, Timestamp: 1000},
					{Value: 2.0, Timestamp: 2000},
				},
			},
			{
				Labels: []*labelpb.Label{
					{Name: "__name__", Value: "another_metric"},
					{Name: "instance", Value: "localhost:9091"},
				},
				Samples: []*prompb.Sample{
					{Value: 3.0, Timestamp: 3000},
				},
			},
		},
	}

	// Marshal using vtprotobuf
	data, err := original.MarshalVT()
	require.NoError(t, err)

	// Unmarshal using zero-copy
	var zeroCopy WriteRequest
	err = zeroCopy.UnmarshalZeroCopy(data)
	require.NoError(t, err)

	// Verify the data
	require.Equal(t, original.Tenant, zeroCopy.Tenant)
	require.Equal(t, original.Replica, zeroCopy.Replica)
	require.Equal(t, len(original.Timeseries), len(zeroCopy.Timeseries))

	for i, ts := range original.Timeseries {
		require.Equal(t, len(ts.Labels), len(zeroCopy.Timeseries[i].Labels))
		for j, lbl := range ts.Labels {
			require.Equal(t, lbl.Name, zeroCopy.Timeseries[i].Labels[j].Name)
			require.Equal(t, lbl.Value, zeroCopy.Timeseries[i].Labels[j].Value)
		}
		require.Equal(t, len(ts.Samples), len(zeroCopy.Timeseries[i].Samples))
		for j, s := range ts.Samples {
			require.Equal(t, s.Value, zeroCopy.Timeseries[i].Samples[j].Value)
			require.Equal(t, s.Timestamp, zeroCopy.Timeseries[i].Samples[j].Timestamp)
		}
	}
}

func TestSeriesUnmarshalZeroCopy(t *testing.T) {
	// Create a Series with labels and chunks
	original := &Series{
		Labels: []*labelpb.Label{
			{Name: "__name__", Value: "test_metric"},
			{Name: "instance", Value: "localhost:9090"},
			{Name: "job", Value: "test"},
		},
		Chunks: []*AggrChunk{
			{
				MinTime: 1000,
				MaxTime: 2000,
				Raw:     &Chunk{Type: Chunk_XOR, Data: []byte{1, 2, 3, 4, 5}},
			},
		},
	}

	// Marshal using vtprotobuf
	data, err := original.MarshalVT()
	require.NoError(t, err)

	// Unmarshal using zero-copy
	var zeroCopy Series
	err = zeroCopy.UnmarshalZeroCopy(data)
	require.NoError(t, err)

	// Verify the data
	require.Equal(t, len(original.Labels), len(zeroCopy.Labels))
	for i, lbl := range original.Labels {
		require.Equal(t, lbl.Name, zeroCopy.Labels[i].Name)
		require.Equal(t, lbl.Value, zeroCopy.Labels[i].Value)
	}
	require.Equal(t, len(original.Chunks), len(zeroCopy.Chunks))
}

func BenchmarkWriteRequestUnmarshal(b *testing.B) {
	// Create a WriteRequest with 100 time series, each with 10 labels and 10 samples
	timeseries := make([]*prompb.TimeSeries, 100)
	for i := range timeseries {
		labels := make([]*labelpb.Label, 10)
		for j := range labels {
			labels[j] = &labelpb.Label{
				Name:  "label_name_with_some_reasonable_length_" + string(rune('a'+j)),
				Value: "label_value_with_some_reasonable_length_for_realistic_benchmark_" + string(rune('0'+j)),
			}
		}
		samples := make([]*prompb.Sample, 10)
		for j := range samples {
			samples[j] = &prompb.Sample{
				Value:     float64(j),
				Timestamp: int64(i*1000 + j),
			}
		}
		timeseries[i] = &prompb.TimeSeries{
			Labels:  labels,
			Samples: samples,
		}
	}

	wreq := &WriteRequest{
		Tenant:     "test-tenant",
		Replica:    1,
		Timeseries: timeseries,
	}

	data, err := wreq.MarshalVT()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.Run("UnmarshalVT", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var wr WriteRequest
			if err := wr.UnmarshalVT(data); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("UnmarshalZeroCopy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var wr WriteRequest
			if err := wr.UnmarshalZeroCopy(data); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSeriesUnmarshal(b *testing.B) {
	// Create a Series with 20 labels
	labels := make([]*labelpb.Label, 20)
	for i := range labels {
		labels[i] = &labelpb.Label{
			Name:  "label_name_with_some_reasonable_length_" + string(rune('a'+i)),
			Value: "label_value_with_some_reasonable_length_for_realistic_benchmark_" + string(rune('0'+i)),
		}
	}

	series := &Series{
		Labels: labels,
		Chunks: []*AggrChunk{
			{
				MinTime: 1000,
				MaxTime: 2000,
				Raw:     &Chunk{Type: Chunk_XOR, Data: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
			},
		},
	}

	data, err := series.MarshalVT()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.Run("UnmarshalVT", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var s Series
			if err := s.UnmarshalVT(data); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("UnmarshalZeroCopy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var s Series
			if err := s.UnmarshalZeroCopy(data); err != nil {
				b.Fatal(err)
			}
		}
	})
}
