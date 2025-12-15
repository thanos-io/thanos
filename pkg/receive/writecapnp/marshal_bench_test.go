// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"bytes"
	"fmt"
	"testing"

	"capnproto.org/go/capnp/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func BenchmarkMarshalWriteRequest(b *testing.B) {
	const (
		numSeries   = 2
		numClusters = 3
		numPods     = 2
	)
	series := make([]*prompb.TimeSeries, 0, numSeries)
	for range numSeries {
		lbls := make([]*labelpb.Label, 0, numClusters*numPods)
		for j := range numClusters {
			for k := range numPods {
				lbls = append(lbls, &labelpb.Label{
					Name:  fmt.Sprintf("cluster-%d", j),
					Value: fmt.Sprintf("pod-%d", k),
				})
			}
		}
		series = append(series, &prompb.TimeSeries{
			Labels: lbls,
			Samples: []*prompb.Sample{
				{
					Value:     1,
					Timestamp: 2,
				},
			},
		})
	}
	wreq := storepb.WriteRequest{
		Tenant:     "example-tenant",
		Timeseries: series,
	}
	var (
		protoBytes, err                 = proto.Marshal(&wreq)
		capnprotoBytes, paddedErr       = Marshal(wreq.Tenant, wreq.Timeseries)
		capnprotoBytesPacked, packedErr = MarshalPacked(wreq.Tenant, wreq.Timeseries)
	)
	require.NoError(b, err)
	require.NoError(b, paddedErr)
	require.NoError(b, packedErr)
	b.Run("marshal_proto", func(b *testing.B) {
		for b.Loop() {
			var err error
			_, err = proto.Marshal(&wreq)
			require.NoError(b, err)
		}
	})
	b.Run("build", func(b *testing.B) {
		for b.Loop() {
			_, err := Build("example_tenant", wreq.Timeseries)
			require.NoError(b, err)
		}
	})
	b.Run("encode", func(b *testing.B) {
		for b.Loop() {
			var err error
			wr, err := Build("example_tenant", wreq.Timeseries)
			require.NoError(b, err)

			buf := bytes.NewBuffer(nil)
			require.NoError(b, capnp.NewEncoder(buf).Encode(wr.Message()))
		}
	})
	b.Run("encode_packed", func(b *testing.B) {
		for b.Loop() {
			var err error
			wr, err := Build("example_tenant", wreq.Timeseries)
			require.NoError(b, err)

			buf := bytes.NewBuffer(nil)
			require.NoError(b, capnp.NewPackedEncoder(buf).Encode(wr.Message()))
		}
	})

	b.Run("unmarshal_proto", func(b *testing.B) {
		for b.Loop() {
			wr := storepb.WriteRequest{}
			require.NoError(b, proto.Unmarshal(protoBytes, &wr))
		}
	})
	b.Run("unmarshal", func(b *testing.B) {
		for b.Loop() {
			msg, err := capnp.Unmarshal(capnprotoBytes)
			require.NoError(b, err)

			_, err = ReadRootWriteRequest(msg)
			require.NoError(b, err)
		}
	})
	b.Run("unmarshal_packed", func(b *testing.B) {
		for b.Loop() {
			msg, err := capnp.UnmarshalPacked(capnprotoBytesPacked)
			require.NoError(b, err)

			_, err = ReadRootWriteRequest(msg)
			require.NoError(b, err)
		}
	})

	b.Run("decoder", func(b *testing.B) {
		for b.Loop() {
			msg, err := capnp.NewDecoder(bytes.NewReader(capnprotoBytes)).Decode()
			require.NoError(b, err)

			_, err = ReadRootWriteRequest(msg)
			require.NoError(b, err)
		}
	})
	b.Run("decoder_packed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			msg, err := capnp.NewPackedDecoder(bytes.NewReader(capnprotoBytesPacked)).Decode()
			require.NoError(b, err)

			wr, err := ReadRootWriteRequest(msg)
			require.NoError(b, err)

			var ts Series
			iter, err := NewRequest(wr)
			require.NoError(b, err)
			for iter.Next() {
				require.NoError(b, iter.At(&ts))
			}
			iter.Close()
		}
	})
}
