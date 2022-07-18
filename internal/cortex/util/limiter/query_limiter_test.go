// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package limiter

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/internal/cortex/cortexpb"
)

func TestQueryLimiter_AddSeries_ShouldReturnNoErrorOnLimitNotExceeded(t *testing.T) {
	const (
		metricName = "test_metric"
	)

	var (
		series1 = labels.FromMap(map[string]string{
			labels.MetricName: metricName + "_1",
			"series1":         "1",
		})
		series2 = labels.FromMap(map[string]string{
			labels.MetricName: metricName + "_2",
			"series2":         "1",
		})
		limiter = NewQueryLimiter(100, 0, 0)
	)
	err := limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(series1))
	assert.NoError(t, err)
	err = limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(series2))
	assert.NoError(t, err)
	assert.Equal(t, 2, limiter.uniqueSeriesCount())

	// Re-add previous series to make sure it's not double counted
	err = limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(series1))
	assert.NoError(t, err)
	assert.Equal(t, 2, limiter.uniqueSeriesCount())
}

func TestQueryLimiter_AddSeriers_ShouldReturnErrorOnLimitExceeded(t *testing.T) {
	const (
		metricName = "test_metric"
	)

	var (
		series1 = []cortexpb.LabelAdapter{
			{
				Name:  labels.MetricName,
				Value: metricName + "_1",
			},
			{
				Name:  "series1",
				Value: "1",
			},
		}

		series1OtherOrderLabels = []cortexpb.LabelAdapter{
			{
				Name:  "series1",
				Value: "1",
			},
			{
				Name:  labels.MetricName,
				Value: metricName + "_1",
			},
		}

		series1FromMap = labels.FromMap(map[string]string{
			"series1":         "1",
			labels.MetricName: metricName + "_1",
		})
		series2 = labels.FromMap(map[string]string{
			labels.MetricName: metricName + "_2",
			"series2":         "1",
		})
		limiter = NewQueryLimiter(1, 0, 0)
	)
	err := limiter.AddSeries(series1)
	require.NoError(t, err)
	err = limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(series1FromMap))
	require.NoError(t, err)
	err = limiter.AddSeries(series1OtherOrderLabels)
	require.NoError(t, err)
	err = limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(series2))
	require.Error(t, err)
}

func TestQueryLimiter_AddChunkBytes(t *testing.T) {
	var limiter = NewQueryLimiter(0, 100, 0)

	err := limiter.AddChunkBytes(100)
	require.NoError(t, err)
	err = limiter.AddChunkBytes(1)
	require.Error(t, err)
}

func BenchmarkQueryLimiter_AddSeries(b *testing.B) {
	const (
		metricName = "test_metric"
	)
	var series []labels.Labels
	for i := 0; i < b.N; i++ {
		series = append(series,
			labels.FromMap(map[string]string{
				labels.MetricName: metricName + "_1",
				"series1":         fmt.Sprint(i),
			}))
	}
	b.ResetTimer()

	limiter := NewQueryLimiter(b.N+1, 0, 0)
	for _, s := range series {
		err := limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(s))
		assert.NoError(b, err)
	}

}
