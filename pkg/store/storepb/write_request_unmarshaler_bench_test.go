// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"fmt"
	"testing"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func createBenchmarkWriteRequest(numTimeseries, numLabels, numSamples int) *WriteRequest {
	wr := &WriteRequest{
		Tenant:  "benchmark-tenant",
		Replica: 1,
	}

	for i := 0; i < numTimeseries; i++ {
		ts := prompb.TimeSeries{}

		// Add labels.
		ts.Labels = append(ts.Labels, labelpb.ZLabel{
			Name:  "__name__",
			Value: fmt.Sprintf("benchmark_metric_%d", i),
		})
		for j := 1; j < numLabels; j++ {
			ts.Labels = append(ts.Labels, labelpb.ZLabel{
				Name:  fmt.Sprintf("label_%d", j),
				Value: fmt.Sprintf("value_%d_%d", i, j),
			})
		}

		// Add samples.
		for j := 0; j < numSamples; j++ {
			ts.Samples = append(ts.Samples, prompb.Sample{
				Value:     float64(i*numSamples + j),
				Timestamp: int64(j * 1000),
			})
		}

		wr.Timeseries = append(wr.Timeseries, ts)
	}

	return wr
}

func BenchmarkWriteRequestUnmarshal(b *testing.B) {
	benchmarks := []struct {
		name          string
		numTimeseries int
		numLabels     int
		numSamples    int
	}{
		{"small_10ts_5lbl_1smp", 10, 5, 1},
		{"medium_100ts_10lbl_1smp", 100, 10, 1},
		{"large_1000ts_10lbl_1smp", 1000, 10, 1},
		{"large_1000ts_10lbl_10smp", 1000, 10, 10},
		{"xlarge_10000ts_10lbl_1smp", 10000, 10, 1},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			wr := createBenchmarkWriteRequest(bm.numTimeseries, bm.numLabels, bm.numSamples)
			data, err := wr.Marshal()
			if err != nil {
				b.Fatal(err)
			}

			b.Run("standard", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					result := &WriteRequest{}
					if err := result.Unmarshal(data); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("pooled", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					wru := GetWriteRequestUnmarshaler()
					_, err := wru.UnmarshalProtobuf(data)
					if err != nil {
						b.Fatal(err)
					}
					PutWriteRequestUnmarshaler(wru)
				}
			})
		})
	}
}

func BenchmarkWriteRequestUnmarshal_Parallel(b *testing.B) {
	wr := createBenchmarkWriteRequest(1000, 10, 1)
	data, err := wr.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	b.Run("standard", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				result := &WriteRequest{}
				if err := result.Unmarshal(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	b.Run("pooled", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				wru := GetWriteRequestUnmarshaler()
				_, err := wru.UnmarshalProtobuf(data)
				if err != nil {
					b.Fatal(err)
				}
				PutWriteRequestUnmarshaler(wru)
			}
		})
	})
}

func BenchmarkWriteRequestUnmarshal_WithHistograms(b *testing.B) {
	wr := &WriteRequest{
		Tenant: "benchmark-tenant",
	}

	for i := 0; i < 100; i++ {
		ts := prompb.TimeSeries{
			Labels: []labelpb.ZLabel{
				{Name: "__name__", Value: fmt.Sprintf("histogram_metric_%d", i)},
				{Name: "instance", Value: "localhost:9090"},
				{Name: "job", Value: "benchmark"},
			},
			Histograms: []prompb.Histogram{
				{
					Count:         &prompb.Histogram_CountInt{CountInt: 100},
					Sum:           500.5,
					Schema:        3,
					ZeroThreshold: 0.001,
					ZeroCount:     &prompb.Histogram_ZeroCountInt{ZeroCountInt: 5},
					NegativeSpans: []prompb.BucketSpan{
						{Offset: 0, Length: 5},
						{Offset: 2, Length: 3},
					},
					NegativeDeltas: []int64{1, 2, 3, 4, 5, 6, 7, 8},
					PositiveSpans: []prompb.BucketSpan{
						{Offset: 0, Length: 10},
					},
					PositiveDeltas: []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
					ResetHint:      prompb.Histogram_NO,
					Timestamp:      int64(i * 1000),
				},
			},
		}
		wr.Timeseries = append(wr.Timeseries, ts)
	}

	data, err := wr.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	b.Run("standard", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			result := &WriteRequest{}
			if err := result.Unmarshal(data); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("pooled", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			wru := GetWriteRequestUnmarshaler()
			_, err := wru.UnmarshalProtobuf(data)
			if err != nil {
				b.Fatal(err)
			}
			PutWriteRequestUnmarshaler(wru)
		}
	})
}
