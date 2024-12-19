// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Provenance-includes-location: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/95e8f8fdc2a9dc87230406c9a3cf02be4fd68bea/pkg/translator/prometheusremotewrite/number_data_points.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright The OpenTelemetry Authors.

package otlptranslator

import (
	"context"
	"math"

	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/prometheus/prometheus/model/value"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func (c *PrometheusConverter) addGaugeNumberDataPoints(ctx context.Context, dataPoints pmetric.NumberDataPointSlice,
	resource pcommon.Resource, settings Settings, name string) error {
	for x := 0; x < dataPoints.Len(); x++ {
		if err := c.everyN.checkContext(ctx); err != nil {
			return err
		}

		pt := dataPoints.At(x)
		labels := createAttributes(
			resource,
			pt.Attributes(),
			settings,
			nil,
			true,
			model.MetricNameLabel,
			name,
		)
		sample := &prompb.Sample{
			// convert ns to ms
			Timestamp: convertTimeStamp(pt.Timestamp()),
		}
		switch pt.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			sample.Value = float64(pt.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			sample.Value = pt.DoubleValue()
		}
		if pt.Flags().NoRecordedValue() {
			sample.Value = math.Float64frombits(value.StaleNaN)
		}
		c.addSample(sample, labels)
	}

	return nil
}

func (c *PrometheusConverter) addSumNumberDataPoints(ctx context.Context, dataPoints pmetric.NumberDataPointSlice,
	resource pcommon.Resource, metric pmetric.Metric, settings Settings, name string) error {
	for x := 0; x < dataPoints.Len(); x++ {
		if err := c.everyN.checkContext(ctx); err != nil {
			return err
		}

		pt := dataPoints.At(x)
		lbls := createAttributes(
			resource,
			pt.Attributes(),
			settings,
			nil,
			true,
			model.MetricNameLabel,
			name,
		)
		sample := &prompb.Sample{
			// convert ns to ms
			Timestamp: convertTimeStamp(pt.Timestamp()),
		}
		switch pt.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			sample.Value = float64(pt.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			sample.Value = pt.DoubleValue()
		}
		if pt.Flags().NoRecordedValue() {
			sample.Value = math.Float64frombits(value.StaleNaN)
		}
		ts := c.addSample(sample, lbls)
		if ts != nil {
			exemplars, err := getPromExemplars[pmetric.NumberDataPoint](ctx, &c.everyN, pt)
			if err != nil {
				return err
			}
			ts.Exemplars = append(ts.Exemplars, exemplars...)
		}

		// add created time series if needed
		if settings.ExportCreatedMetric && metric.Sum().IsMonotonic() {
			startTimestamp := pt.StartTimestamp()
			if startTimestamp == 0 {
				return nil
			}

			createdLabels := make([]labelpb.ZLabel, len(lbls))
			copy(createdLabels, lbls)
			for i, l := range createdLabels {
				if l.Name == model.MetricNameLabel {
					createdLabels[i].Value = name + createdSuffix
					break
				}
			}
			c.addTimeSeriesIfNeeded(createdLabels, startTimestamp, pt.Timestamp())
		}
	}

	return nil
}
