// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/labelpbv2"
)

func FuzzMarshalUnmarshalLabelsV1V2(f *testing.F) {
	f.Add(uint64(100), "a")

	f.Fuzz(func(t *testing.T, a uint64, b string) {
		lbls := labels.NewBuilder(labels.EmptyLabels())

		if a == 0 || a > 3000 {
			return
		}

		for i := range a {
			lbls.Set(fmt.Sprintf("i%d", i), fmt.Sprintf("%d_%s", i, b))
		}

		zlbls := labelpb.ZLabelsFromPromLabels(lbls.Labels())

		zlblset := labelpb.ZLabelSet{
			Labels: zlbls,
		}
		marshaled, err := zlblset.Marshal()
		require.NoError(t, err)

		var v2 = labelpbv2.LabelSetV2(lbls.Labels())

		sz := v2.Size()
		buf := make([]byte, sz)

		_, err = v2.MarshalToSizedBuffer(buf)
		require.NoError(t, err)

		require.Equal(t, marshaled, buf)

		var unmarshaledV2 labelpbv2.LabelSetV2

		err = unmarshaledV2.Unmarshal(marshaled)
		require.NoError(t, err)
		require.Equal(t, v2, unmarshaledV2)
	})
}

func BenchmarkUnmarshalLabelsV1V2(b *testing.B) {
	lbls := labels.NewBuilder(labels.EmptyLabels())

	for i := range 1000 {
		lbls.Set(fmt.Sprintf("i%d", i), fmt.Sprintf("%d", i))
	}

	zlbls := labelpb.ZLabelsFromPromLabels(lbls.Labels())

	zlblset := labelpb.ZLabelSet{
		Labels: zlbls,
	}
	marshaled, err := zlblset.Marshal()
	require.NoError(b, err)

	b.Run("labelpbv2 - marshal", func(b *testing.B) {
		var unmarshaled labelpbv2.LabelSetV2
		err := unmarshaled.Unmarshal(marshaled)
		require.NoError(b, err)

		sz := unmarshaled.Size()
		require.Greater(b, sz, 0)

		buf := make([]byte, sz)

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			_, err := unmarshaled.MarshalToSizedBuffer(buf)
			require.NoError(b, err)
			clear(buf)
		}
	})

	b.Run("labelpbv2 - unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			var unmarshaled labelpbv2.LabelSetV2
			err := unmarshaled.Unmarshal(marshaled)
			require.NoError(b, err)
		}
	})

	b.Run("labelpbv1  - unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			var unmarshaled labelpb.ZLabelSet
			err := unmarshaled.Unmarshal(marshaled)
			require.NoError(b, err)
		}
	})

	b.Run("labelpbv1 - marshal", func(b *testing.B) {
		var unmarshaled labelpb.ZLabelSet
		err := unmarshaled.Unmarshal(marshaled)
		require.NoError(b, err)

		sz := unmarshaled.Size()
		require.Greater(b, sz, 0)

		buf := make([]byte, sz)

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			_, err := unmarshaled.MarshalToSizedBuffer(buf)
			require.NoError(b, err)
			// []uint8 len: 9, cap: 9, [10,7,10,2,105,48,18,1,48]
			clear(buf)
		}
	})

}
