// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extgrpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"google.golang.org/grpc/encoding"
)

func TestVTProtoCodecWriteRequestPooling(t *testing.T) {
	// Create a WriteRequest with some time series
	wreq := &storepb.WriteRequest{
		Tenant:  "test-tenant",
		Replica: 1,
		Timeseries: []*prompb.TimeSeries{
			{
				Labels: []*labelpb.Label{
					{Name: "__name__", Value: "test_metric"},
					{Name: "instance", Value: "localhost:9090"},
				},
				Samples: []*prompb.Sample{
					{Value: 1.0, Timestamp: 1000},
					{Value: 2.0, Timestamp: 2000},
				},
			},
		},
	}

	c := &vtprotoCodec{
		CodecV2: encoding.GetCodecV2("proto"),
	}

	// Marshal the request
	marshaled, err := c.Marshal(wreq)
	require.NoError(t, err)

	// Unmarshal into a new WriteRequest
	var wreq2 storepb.WriteRequest
	err = c.Unmarshal(marshaled, &wreq2)
	require.NoError(t, err)

	// Verify the data was correctly unmarshaled
	require.Equal(t, wreq.Tenant, wreq2.Tenant)
	require.Equal(t, wreq.Replica, wreq2.Replica)
	require.Equal(t, len(wreq.Timeseries), len(wreq2.Timeseries))

	// Return to pool (simulating what the handler does)
	wreq2.ReturnToVTPool()
}

func BenchmarkVTProtoCodecWriteRequest(b *testing.B) {
	// Create a WriteRequest with multiple time series
	timeseries := make([]*prompb.TimeSeries, 100)
	for i := range timeseries {
		timeseries[i] = &prompb.TimeSeries{
			Labels: []*labelpb.Label{
				{Name: "__name__", Value: "test_metric"},
				{Name: "instance", Value: "localhost:9090"},
				{Name: "job", Value: "test"},
			},
			Samples: []*prompb.Sample{
				{Value: float64(i), Timestamp: int64(i * 1000)},
			},
		}
	}

	wreq := &storepb.WriteRequest{
		Tenant:     "test-tenant",
		Replica:    1,
		Timeseries: timeseries,
	}

	c := &vtprotoCodec{
		CodecV2: encoding.GetCodecV2("proto"),
	}
	ogCodec := c.CodecV2

	b.Run("vanilla proto marshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, err := ogCodec.Marshal(wreq)
			require.NoError(b, err)
		}
	})

	b.Run("vtproto codec marshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, err := c.Marshal(wreq)
			require.NoError(b, err)
		}
	})

	marshaled, err := c.Marshal(wreq)
	require.NoError(b, err)

	b.Run("vanilla proto unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			var wr storepb.WriteRequest
			err := ogCodec.Unmarshal(marshaled, &wr)
			require.NoError(b, err)
		}
	})

	b.Run("vtproto codec unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			var wr storepb.WriteRequest
			err := c.Unmarshal(marshaled, &wr)
			require.NoError(b, err)
		}
	})

	b.Run("vtproto codec unmarshal with pool return", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			var wr storepb.WriteRequest
			err := c.Unmarshal(marshaled, &wr)
			require.NoError(b, err)
			wr.ReturnToVTPool()
		}
	})
}
