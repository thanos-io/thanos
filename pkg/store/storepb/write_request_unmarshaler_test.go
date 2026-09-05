// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"testing"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestWriteRequestUnmarshaler_BasicUnmarshal(t *testing.T) {
	// Create a WriteRequest with known data.
	original := &WriteRequest{
		Tenant:  "test-tenant",
		Replica: 42,
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "method", Value: "GET"},
				},
				Samples: []prompb.Sample{
					{Value: 100.5, Timestamp: 1000},
					{Value: 200.5, Timestamp: 2000},
				},
			},
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "method", Value: "POST"},
				},
				Samples: []prompb.Sample{
					{Value: 50.5, Timestamp: 1000},
				},
			},
		},
	}

	// Marshal using the standard protobuf marshal.
	data, err := original.Marshal()
	testutil.Ok(t, err)

	// Unmarshal using our new unmarshaler.
	wru := GetWriteRequestUnmarshaler()
	defer PutWriteRequestUnmarshaler(wru)

	result, err := wru.UnmarshalProtobuf(data)
	testutil.Ok(t, err)

	// Verify the result matches the original.
	testutil.Equals(t, original.Tenant, result.Tenant)
	testutil.Equals(t, original.Replica, result.Replica)
	testutil.Equals(t, len(original.Timeseries), len(result.Timeseries))

	for i, ts := range original.Timeseries {
		testutil.Equals(t, len(ts.Labels), len(result.Timeseries[i].Labels))
		for j, lbl := range ts.Labels {
			testutil.Equals(t, lbl.Name, result.Timeseries[i].Labels[j].Name)
			testutil.Equals(t, lbl.Value, result.Timeseries[i].Labels[j].Value)
		}

		testutil.Equals(t, len(ts.Samples), len(result.Timeseries[i].Samples))
		for j, s := range ts.Samples {
			testutil.Equals(t, s.Value, result.Timeseries[i].Samples[j].Value)
			testutil.Equals(t, s.Timestamp, result.Timeseries[i].Samples[j].Timestamp)
		}
	}
}

func TestWriteRequestUnmarshaler_WithExemplars(t *testing.T) {
	original := &WriteRequest{
		Tenant: "test-tenant",
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "http_request_duration_seconds"},
				},
				Samples: []prompb.Sample{
					{Value: 0.5, Timestamp: 1000},
				},
				Exemplars: []prompb.Exemplar{
					{
						Labels:    []labelpb.ZLabel{{Name: "trace_id", Value: "abc123"}},
						Value:     0.5,
						Timestamp: 1000,
					},
				},
			},
		},
	}

	data, err := original.Marshal()
	testutil.Ok(t, err)

	wru := GetWriteRequestUnmarshaler()
	defer PutWriteRequestUnmarshaler(wru)

	result, err := wru.UnmarshalProtobuf(data)
	testutil.Ok(t, err)

	testutil.Equals(t, 1, len(result.Timeseries))
	testutil.Equals(t, 1, len(result.Timeseries[0].Exemplars))

	ex := result.Timeseries[0].Exemplars[0]
	testutil.Equals(t, 0.5, ex.Value)
	testutil.Equals(t, int64(1000), ex.Timestamp)
	testutil.Equals(t, 1, len(ex.Labels))
	testutil.Equals(t, "trace_id", ex.Labels[0].Name)
	testutil.Equals(t, "abc123", ex.Labels[0].Value)
}

func TestWriteRequestUnmarshaler_WithHistograms(t *testing.T) {
	original := &WriteRequest{
		Tenant: "test-tenant",
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "http_request_duration_seconds"},
				},
				Histograms: []prompb.Histogram{
					{
						Count:         &prompb.Histogram_CountInt{CountInt: 100},
						Sum:           50.5,
						Schema:        3,
						ZeroThreshold: 0.001,
						ZeroCount:     &prompb.Histogram_ZeroCountInt{ZeroCountInt: 5},
						NegativeSpans: []prompb.BucketSpan{
							{Offset: 0, Length: 2},
						},
						NegativeDeltas: []int64{1, 2},
						PositiveSpans: []prompb.BucketSpan{
							{Offset: 0, Length: 3},
						},
						PositiveDeltas: []int64{10, 20, 30},
						ResetHint:      prompb.Histogram_NO,
						Timestamp:      1000,
					},
				},
			},
		},
	}

	data, err := original.Marshal()
	testutil.Ok(t, err)

	wru := GetWriteRequestUnmarshaler()
	defer PutWriteRequestUnmarshaler(wru)

	result, err := wru.UnmarshalProtobuf(data)
	testutil.Ok(t, err)

	testutil.Equals(t, 1, len(result.Timeseries))
	testutil.Equals(t, 1, len(result.Timeseries[0].Histograms))

	h := result.Timeseries[0].Histograms[0]
	testutil.Equals(t, uint64(100), h.GetCountInt())
	testutil.Equals(t, 50.5, h.Sum)
	testutil.Equals(t, int32(3), h.Schema)
	testutil.Equals(t, 0.001, h.ZeroThreshold)
	testutil.Equals(t, uint64(5), h.GetZeroCountInt())
	testutil.Equals(t, prompb.Histogram_NO, h.ResetHint)
	testutil.Equals(t, int64(1000), h.Timestamp)

	testutil.Equals(t, 1, len(h.NegativeSpans))
	testutil.Equals(t, int32(0), h.NegativeSpans[0].Offset)
	testutil.Equals(t, uint32(2), h.NegativeSpans[0].Length)

	testutil.Equals(t, []int64{1, 2}, h.NegativeDeltas)

	testutil.Equals(t, 1, len(h.PositiveSpans))
	testutil.Equals(t, int32(0), h.PositiveSpans[0].Offset)
	testutil.Equals(t, uint32(3), h.PositiveSpans[0].Length)

	testutil.Equals(t, []int64{10, 20, 30}, h.PositiveDeltas)
}

func TestWriteRequestUnmarshaler_PoolReuse(t *testing.T) {
	original := &WriteRequest{
		Tenant: "tenant-1",
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "metric1"},
				},
				Samples: []prompb.Sample{
					{Value: 1.0, Timestamp: 1000},
				},
			},
		},
	}

	data, err := original.Marshal()
	testutil.Ok(t, err)

	// First unmarshal.
	wru := GetWriteRequestUnmarshaler()
	result1, err := wru.UnmarshalProtobuf(data)
	testutil.Ok(t, err)
	testutil.Equals(t, "tenant-1", result1.Tenant)
	testutil.Equals(t, 1, len(result1.Timeseries))

	// Return to pool.
	PutWriteRequestUnmarshaler(wru)

	// Get from pool again (should be the same instance).
	wru2 := GetWriteRequestUnmarshaler()

	// Unmarshal different data.
	original2 := &WriteRequest{
		Tenant: "tenant-2",
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "metric2"},
				},
				Samples: []prompb.Sample{
					{Value: 2.0, Timestamp: 2000},
				},
			},
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "metric3"},
				},
				Samples: []prompb.Sample{
					{Value: 3.0, Timestamp: 3000},
				},
			},
		},
	}

	data2, err := original2.Marshal()
	testutil.Ok(t, err)

	result2, err := wru2.UnmarshalProtobuf(data2)
	testutil.Ok(t, err)

	// Verify second unmarshal is correct.
	testutil.Equals(t, "tenant-2", result2.Tenant)
	testutil.Equals(t, 2, len(result2.Timeseries))
	testutil.Equals(t, "metric2", result2.Timeseries[0].Labels[0].Value)
	testutil.Equals(t, "metric3", result2.Timeseries[1].Labels[0].Value)

	PutWriteRequestUnmarshaler(wru2)
}

func TestWriteRequestUnmarshaler_EmptyRequest(t *testing.T) {
	original := &WriteRequest{}

	data, err := original.Marshal()
	testutil.Ok(t, err)

	wru := GetWriteRequestUnmarshaler()
	defer PutWriteRequestUnmarshaler(wru)

	result, err := wru.UnmarshalProtobuf(data)
	testutil.Ok(t, err)

	testutil.Equals(t, "", result.Tenant)
	testutil.Equals(t, int64(0), result.Replica)
	testutil.Equals(t, 0, len(result.Timeseries))
}

func TestWriteRequestUnmarshaler_LargeRequest(t *testing.T) {
	// Create a large request with many timeseries.
	original := &WriteRequest{
		Tenant: "large-tenant",
	}

	numTimeseries := 1000
	numSamplesPerTS := 10
	numLabelsPerTS := 5

	for i := 0; i < numTimeseries; i++ {
		ts := prompb.TimeSeries{}

		for j := 0; j < numLabelsPerTS; j++ {
			ts.Labels = append(ts.Labels, labelpb.ZLabel{
				Name:  "label_" + string(rune('a'+j)),
				Value: "value_" + string(rune('0'+j)),
			})
		}

		for j := 0; j < numSamplesPerTS; j++ {
			ts.Samples = append(ts.Samples, prompb.Sample{
				Value:     float64(i*numSamplesPerTS + j),
				Timestamp: int64(j * 1000),
			})
		}

		original.Timeseries = append(original.Timeseries, ts)
	}

	data, err := original.Marshal()
	testutil.Ok(t, err)

	wru := GetWriteRequestUnmarshaler()
	defer PutWriteRequestUnmarshaler(wru)

	result, err := wru.UnmarshalProtobuf(data)
	testutil.Ok(t, err)

	testutil.Equals(t, "large-tenant", result.Tenant)
	testutil.Equals(t, numTimeseries, len(result.Timeseries))

	for i, ts := range result.Timeseries {
		testutil.Equals(t, numLabelsPerTS, len(ts.Labels))
		testutil.Equals(t, numSamplesPerTS, len(ts.Samples))

		// Verify first sample.
		testutil.Equals(t, float64(i*numSamplesPerTS), ts.Samples[0].Value)
	}
}

// TestWriteRequestUnmarshaler_Compatibility verifies that our unmarshaler
// produces the same result as the standard protobuf unmarshaler.
func TestWriteRequestUnmarshaler_Compatibility(t *testing.T) {
	testCases := []struct {
		name     string
		original *WriteRequest
	}{
		{
			name: "basic",
			original: &WriteRequest{
				Tenant:  "test",
				Replica: 1,
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{Name: "__name__", Value: "test_metric"},
						},
						Samples: []prompb.Sample{
							{Value: 1.0, Timestamp: 1000},
						},
					},
				},
			},
		},
		{
			name: "with_exemplars",
			original: &WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{Name: "__name__", Value: "test_metric"},
						},
						Exemplars: []prompb.Exemplar{
							{
								Labels:    []labelpb.ZLabel{{Name: "trace", Value: "123"}},
								Value:     1.0,
								Timestamp: 1000,
							},
						},
					},
				},
			},
		},
		{
			name: "float_histogram",
			original: &WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []labelpb.ZLabel{
							{Name: "__name__", Value: "histogram_metric"},
						},
						Histograms: []prompb.Histogram{
							{
								Count:          &prompb.Histogram_CountFloat{CountFloat: 100.5},
								Sum:            500.5,
								Schema:         3,
								ZeroThreshold:  0.001,
								ZeroCount:      &prompb.Histogram_ZeroCountFloat{ZeroCountFloat: 5.5},
								NegativeCounts: []float64{1.1, 2.2},
								PositiveCounts: []float64{3.3, 4.4, 5.5},
								Timestamp:      1000,
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.original.Marshal()
			testutil.Ok(t, err)

			// Unmarshal with standard method.
			standard := &WriteRequest{}
			err = standard.Unmarshal(data)
			testutil.Ok(t, err)

			// Unmarshal with our method.
			wru := GetWriteRequestUnmarshaler()
			defer PutWriteRequestUnmarshaler(wru)

			ours, err := wru.UnmarshalProtobuf(data)
			testutil.Ok(t, err)

			// Compare.
			testutil.Equals(t, standard.Tenant, ours.Tenant)
			testutil.Equals(t, standard.Replica, ours.Replica)
			testutil.Equals(t, len(standard.Timeseries), len(ours.Timeseries))

			for i := range standard.Timeseries {
				stdTS := &standard.Timeseries[i]
				ourTS := &ours.Timeseries[i]

				// Compare labels.
				testutil.Equals(t, len(stdTS.Labels), len(ourTS.Labels))
				for j := range stdTS.Labels {
					testutil.Equals(t, stdTS.Labels[j].Name, ourTS.Labels[j].Name)
					testutil.Equals(t, stdTS.Labels[j].Value, ourTS.Labels[j].Value)
				}

				// Compare samples.
				testutil.Equals(t, len(stdTS.Samples), len(ourTS.Samples))
				for j := range stdTS.Samples {
					testutil.Equals(t, stdTS.Samples[j].Value, ourTS.Samples[j].Value)
					testutil.Equals(t, stdTS.Samples[j].Timestamp, ourTS.Samples[j].Timestamp)
				}

				// Compare exemplars.
				testutil.Equals(t, len(stdTS.Exemplars), len(ourTS.Exemplars))
				for j := range stdTS.Exemplars {
					testutil.Equals(t, stdTS.Exemplars[j].Value, ourTS.Exemplars[j].Value)
					testutil.Equals(t, stdTS.Exemplars[j].Timestamp, ourTS.Exemplars[j].Timestamp)
					testutil.Equals(t, len(stdTS.Exemplars[j].Labels), len(ourTS.Exemplars[j].Labels))
				}

				// Compare histograms.
				testutil.Equals(t, len(stdTS.Histograms), len(ourTS.Histograms))
				for j := range stdTS.Histograms {
					stdH := &stdTS.Histograms[j]
					ourH := &ourTS.Histograms[j]

					testutil.Equals(t, stdH.Sum, ourH.Sum)
					testutil.Equals(t, stdH.Schema, ourH.Schema)
					testutil.Equals(t, stdH.ZeroThreshold, ourH.ZeroThreshold)
					testutil.Equals(t, stdH.Timestamp, ourH.Timestamp)
					testutil.Equals(t, stdH.ResetHint, ourH.ResetHint)

					// Compare count (oneof).
					testutil.Equals(t, stdH.GetCountInt(), ourH.GetCountInt())
					testutil.Equals(t, stdH.GetCountFloat(), ourH.GetCountFloat())

					// Compare zero count (oneof).
					testutil.Equals(t, stdH.GetZeroCountInt(), ourH.GetZeroCountInt())
					testutil.Equals(t, stdH.GetZeroCountFloat(), ourH.GetZeroCountFloat())

					// Compare spans and deltas.
					testutil.Equals(t, len(stdH.NegativeSpans), len(ourH.NegativeSpans))
					testutil.Equals(t, len(stdH.PositiveSpans), len(ourH.PositiveSpans))
					testutil.Equals(t, stdH.NegativeDeltas, ourH.NegativeDeltas)
					testutil.Equals(t, stdH.PositiveDeltas, ourH.PositiveDeltas)
					testutil.Equals(t, stdH.NegativeCounts, ourH.NegativeCounts)
					testutil.Equals(t, stdH.PositiveCounts, ourH.PositiveCounts)
				}
			}
		})
	}
}
