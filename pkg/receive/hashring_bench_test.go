// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkKetamaHashringIncrease(b *testing.B) {
	series := makeSeries()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ring := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
		_, err := assignReplicatedSeries(series, ring, 3)
		require.NoError(b, err)
	}
}
