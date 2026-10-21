// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extgrpc

import (
	"testing"
	"testing/synctest"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

func BenchmarkVanillaProtoMarshalUnmarshal(b *testing.B) {
	ls := []labels.Labels{}

	for i := range 1000 {
		ls = append(ls, labels.FromStrings("label_"+string(rune(i)), "value_"+string(rune(i))))
	}
	i := &infopb.InfoResponse{
		LabelSets: labelpb.ZLabelSetsFromPromLabels(
			ls...,
		),
		ComponentType: "asdsadsadsa",
		Store: &infopb.StoreInfo{
			MinTime:          123,
			MaxTime:          456,
			SupportsSharding: true,
			TsdbInfos: []infopb.TSDBInfo{
				{
					Labels:  labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("replica", "1"))},
					MinTime: 100,
					MaxTime: 200,
				},
			},
		},
		Rules:          &infopb.RulesInfo{},
		MetricMetadata: &infopb.MetricMetadataInfo{},
		Exemplars: &infopb.ExemplarsInfo{
			MinTime: 100,
			MaxTime: 200,
		},
	}

	c := &nonPoolingCodec{}

	ogCodec := encoding.GetCodecV2("proto").(*nonPoolingCodec).CodecV2

	b.Run("vanilla proto marshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			_, err := ogCodec.Marshal(i)
			require.NoError(b, err)
		}
	})

	b.Run("non pooling codec marshal", func(b *testing.B) {
		for b.Loop() {
			_, err := c.Marshal(i)
			require.NoError(b, err)
		}
	})

	marshaled, err := ogCodec.Marshal(i)
	require.NoError(b, err)

	b.Run("vanilla proto unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			var ir infopb.InfoResponse
			err := ogCodec.Unmarshal(marshaled, &ir)
			require.NoError(b, err)
		}
	})

	b.Run("non pooling codec unmarshal", func(b *testing.B) {
		for b.Loop() {
			var ir infopb.InfoResponse
			err := c.Unmarshal(marshaled, &ir)
			require.NoError(b, err)
		}
	})
}

func TestNonPoolingCodecMarshal(t *testing.T) {
	i := &infopb.InfoResponse{
		LabelSets: labelpb.ZLabelSetsFromPromLabels(
			labels.FromStrings("foo", "bar"),
			labels.FromStrings("baz", "qux"),
			labels.FromStrings("aaa", "bbb"),
		),
		ComponentType: "asdsadsadsa",
		Store: &infopb.StoreInfo{
			MinTime:          123,
			MaxTime:          456,
			SupportsSharding: true,
			TsdbInfos: []infopb.TSDBInfo{
				{
					Labels:  labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("replica", "1"))},
					MinTime: 100,
					MaxTime: 200,
				},
			},
		},
		Rules:          &infopb.RulesInfo{},
		MetricMetadata: &infopb.MetricMetadataInfo{},
		Exemplars: &infopb.ExemplarsInfo{
			MinTime: 100,
			MaxTime: 200,
		},
	}

	c := &nonPoolingCodec{}

	_, err := c.Marshal(i)
	require.NoError(t, err)
}

func TestEndpointGroupGRPCOptsDefaultRetryPolicy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cc, err := grpc.NewClient("passthrough:///127.0.0.1:1", append(EndpointGroupGRPCOpts(""), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
		require.NoError(t, err)
		t.Cleanup(func() { _ = cc.Close() })
		cc.Connect()

		// NOTE(GiedriusS): The default service config is applied asynchronously once the channel leaves idle.
		const method = "/thanos.Store/Series"

		synctest.Wait()

		require.NotNil(t, cc.GetMethodConfig(method).RetryPolicy)

		rp := cc.GetMethodConfig(method).RetryPolicy
		require.Equal(t, 5, rp.MaxAttempts)
		require.True(t, rp.RetryableStatusCodes[codes.Unavailable])

	})

}
