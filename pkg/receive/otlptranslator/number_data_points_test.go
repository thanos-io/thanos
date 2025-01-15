// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Provenance-includes-location: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/247a9f996e09a83cdc25addf70c05e42b8b30186/pkg/translator/prometheusremotewrite/number_data_points_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright The OpenTelemetry Authors.

package otlptranslator

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestPrometheusConverter_addGaugeNumberDataPoints(t *testing.T) {
	ts := uint64(time.Now().UnixNano())
	tests := []struct {
		name   string
		metric func() pmetric.Metric
		want   func() map[uint64]*prompb.TimeSeries
	}{
		{
			name: "gauge",
			metric: func() pmetric.Metric {
				return getIntGaugeMetric(
					"test",
					pcommon.NewMap(),
					1, ts,
				)
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []labelpb.ZLabel{
					{Name: model.MetricNameLabel, Value: "test"},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{
								Value:     1,
								Timestamp: convertTimeStamp(pcommon.Timestamp(ts)),
							}},
					},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metric := tt.metric()
			converter := NewPrometheusConverter()

			_ = converter.addGaugeNumberDataPoints(
				context.Background(),
				metric.Gauge().DataPoints(),
				pcommon.NewResource(),
				Settings{
					ExportCreatedMetric: true,
				},
				metric.Name(),
			)

			assert.Equal(t, tt.want(), converter.unique)
			assert.Empty(t, converter.conflicts)
		})
	}
}

func TestPrometheusConverter_addSumNumberDataPoints(t *testing.T) {
	ts := pcommon.Timestamp(time.Now().UnixNano())
	tests := []struct {
		name   string
		metric func() pmetric.Metric
		want   func() map[uint64]*prompb.TimeSeries
	}{
		{
			name: "sum",
			metric: func() pmetric.Metric {
				return getIntSumMetric(
					"test",
					pcommon.NewMap(),
					1,
					uint64(ts.AsTime().UnixNano()),
				)
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []labelpb.ZLabel{
					{Name: model.MetricNameLabel, Value: "test"},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{
								Value:     1,
								Timestamp: convertTimeStamp(ts),
							}},
					},
				}
			},
		},
		{
			name: "sum with exemplars",
			metric: func() pmetric.Metric {
				m := getIntSumMetric(
					"test",
					pcommon.NewMap(),
					1,
					uint64(ts.AsTime().UnixNano()),
				)
				m.Sum().DataPoints().At(0).Exemplars().AppendEmpty().SetDoubleValue(2)
				return m
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []labelpb.ZLabel{
					{Name: model.MetricNameLabel, Value: "test"},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{{
							Value:     1,
							Timestamp: convertTimeStamp(ts),
						}},
						Exemplars: []prompb.Exemplar{
							{Value: 2},
						},
					},
				}
			},
		},
		{
			name: "monotonic cumulative sum with start timestamp",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_sum")
				metric.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				metric.SetEmptySum().SetIsMonotonic(true)

				dp := metric.Sum().DataPoints().AppendEmpty()
				dp.SetDoubleValue(1)
				dp.SetTimestamp(ts)
				dp.SetStartTimestamp(ts)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []labelpb.ZLabel{
					{Name: model.MetricNameLabel, Value: "test_sum"},
				}
				createdLabels := []labelpb.ZLabel{
					{Name: model.MetricNameLabel, Value: "test_sum" + createdSuffix},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{Value: 1, Timestamp: convertTimeStamp(ts)},
						},
					},
					timeSeriesSignature(createdLabels): {
						Labels: createdLabels,
						Samples: []prompb.Sample{
							{Value: float64(convertTimeStamp(ts)), Timestamp: convertTimeStamp(ts)},
						},
					},
				}
			},
		},
		{
			name: "monotonic cumulative sum with no start time",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_sum")
				metric.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				metric.SetEmptySum().SetIsMonotonic(true)

				dp := metric.Sum().DataPoints().AppendEmpty()
				dp.SetTimestamp(ts)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []labelpb.ZLabel{
					{Name: model.MetricNameLabel, Value: "test_sum"},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(ts)},
						},
					},
				}
			},
		},
		{
			name: "non-monotonic cumulative sum with start time",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_sum")
				metric.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				metric.SetEmptySum().SetIsMonotonic(false)

				dp := metric.Sum().DataPoints().AppendEmpty()
				dp.SetTimestamp(ts)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []labelpb.ZLabel{
					{Name: model.MetricNameLabel, Value: "test_sum"},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(ts)},
						},
					},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metric := tt.metric()
			converter := NewPrometheusConverter()

			_ = converter.addSumNumberDataPoints(
				context.Background(),
				metric.Sum().DataPoints(),
				pcommon.NewResource(),
				metric,
				Settings{
					ExportCreatedMetric: true,
				},
				metric.Name(),
			)

			assert.Equal(t, tt.want(), converter.unique)
			assert.Empty(t, converter.conflicts)
		})
	}
}
