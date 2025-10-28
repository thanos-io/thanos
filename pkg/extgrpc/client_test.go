// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extgrpc

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"google.golang.org/grpc/encoding"
)

func BenchmarkVanillaProtoMarshal(b *testing.B) {
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
