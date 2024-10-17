package receive

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
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
