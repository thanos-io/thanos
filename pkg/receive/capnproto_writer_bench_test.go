// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func BenchmarkValidateLabels(b *testing.B) {
	const numLabels = 20
	builder := labels.NewScratchBuilder(numLabels)
	for i := 0; i < numLabels; i++ {
		builder.Add("name-"+strconv.Itoa(i), "value-"+strconv.Itoa(i))
	}
	builder.Sort()
	lbls := builder.Labels()
	for i := 0; i < b.N; i++ {
		require.NoError(b, validateLabels(lbls))
	}
}
