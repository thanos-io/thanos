// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Provenance-includes-location: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/95e8f8fdc2a9dc87230406c9a3cf02be4fd68bea/pkg/translator/prometheusremotewrite/helper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright The OpenTelemetry Authors.

package otlptranslator

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

type Settings struct {
	Namespace                         string
	ExternalLabels                    map[string]string
	DisableTargetInfo                 bool
	ExportCreatedMetric               bool
	AddMetricSuffixes                 bool
	AllowUTF8                         bool
	PromoteResourceAttributes         []string
	KeepIdentifyingResourceAttributes bool
}

// PrometheusConverter converts from OTel write format to Prometheus remote write format.
type PrometheusConverter struct {
	unique    map[uint64]*prompb.TimeSeries
	conflicts map[uint64][]*prompb.TimeSeries
	everyN    everyNTimes
	metadata  []prompb.MetricMetadata
}

func NewPrometheusConverter() *PrometheusConverter {
	return &PrometheusConverter{
		unique:    map[uint64]*prompb.TimeSeries{},
		conflicts: map[uint64][]*prompb.TimeSeries{},
	}
}

// FromMetrics converts pmetric.Metrics to Prometheus remote write format.
func (c *PrometheusConverter) FromMetrics(ctx context.Context, md pmetric.Metrics, settings Settings) (annots annotations.Annotations, errs errutil.MultiError) {
	c.everyN = everyNTimes{n: 128}
	resourceMetricsSlice := md.ResourceMetrics()

	numMetrics := 0
	for i := 0; i < resourceMetricsSlice.Len(); i++ {
		scopeMetricsSlice := resourceMetricsSlice.At(i).ScopeMetrics()
		for j := 0; j < scopeMetricsSlice.Len(); j++ {
			numMetrics += scopeMetricsSlice.At(j).Metrics().Len()
		}
	}
	c.metadata = make([]prompb.MetricMetadata, 0, numMetrics)

	for i := 0; i < resourceMetricsSlice.Len(); i++ {
		resourceMetrics := resourceMetricsSlice.At(i)
		resource := resourceMetrics.Resource()
		scopeMetricsSlice := resourceMetrics.ScopeMetrics()
		// keep track of the most recent timestamp in the ResourceMetrics for
		// use with the "target" info metric
		var mostRecentTimestamp pcommon.Timestamp
		for j := 0; j < scopeMetricsSlice.Len(); j++ {
			metricSlice := scopeMetricsSlice.At(j).Metrics()

			// TODO: decide if instrumentation library information should be exported as labels
			for k := 0; k < metricSlice.Len(); k++ {
				if err := c.everyN.checkContext(ctx); err != nil {
					errs.Add(err)
					return
				}

				metric := metricSlice.At(k)
				mostRecentTimestamp = max(mostRecentTimestamp, mostRecentTimestampInMetric(metric))

				if !isValidAggregationTemporality(metric) {
					errs.Add(fmt.Errorf("invalid temporality for metric %q", metric.Name()))
					continue
				}

				promName := BuildCompliantName(metric, settings.Namespace, settings.AddMetricSuffixes, settings.AllowUTF8)
				c.metadata = append(c.metadata, prompb.MetricMetadata{
					Type:             otelMetricTypeToPromMetricType(metric),
					MetricFamilyName: promName,
					Help:             metric.Description(),
					Unit:             metric.Unit(),
				})

				// handle individual metrics based on type
				//exhaustive:enforce
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					dataPoints := metric.Gauge().DataPoints()
					if dataPoints.Len() == 0 {
						errs.Add(fmt.Errorf("empty data points. %s is dropped", metric.Name()))
						break
					}
					if err := c.addGaugeNumberDataPoints(ctx, dataPoints, resource, settings, promName); err != nil {
						errs.Add(err)
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return
						}
					}
				case pmetric.MetricTypeSum:
					dataPoints := metric.Sum().DataPoints()
					if dataPoints.Len() == 0 {
						errs.Add(fmt.Errorf("empty data points. %s is dropped", metric.Name()))
						break
					}
					if err := c.addSumNumberDataPoints(ctx, dataPoints, resource, metric, settings, promName); err != nil {
						errs.Add(err)
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return
						}
					}
				case pmetric.MetricTypeHistogram:
					dataPoints := metric.Histogram().DataPoints()
					if dataPoints.Len() == 0 {
						errs.Add(fmt.Errorf("empty data points. %s is dropped", metric.Name()))
						break
					}
					if err := c.addHistogramDataPoints(ctx, dataPoints, resource, settings, promName); err != nil {
						errs.Add(err)
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return
						}
					}
				case pmetric.MetricTypeExponentialHistogram:
					dataPoints := metric.ExponentialHistogram().DataPoints()
					if dataPoints.Len() == 0 {
						errs.Add(fmt.Errorf("empty data points. %s is dropped", metric.Name()))
						break
					}
					ws, err := c.addExponentialHistogramDataPoints(
						ctx,
						dataPoints,
						resource,
						settings,
						promName,
					)
					annots.Merge(ws)
					if err != nil {
						errs.Add(err)
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return
						}
					}
				case pmetric.MetricTypeSummary:
					dataPoints := metric.Summary().DataPoints()
					if dataPoints.Len() == 0 {
						errs.Add(fmt.Errorf("empty data points. %s is dropped", metric.Name()))
						break
					}
					if err := c.addSummaryDataPoints(ctx, dataPoints, resource, settings, promName); err != nil {
						errs.Add(err)
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return
						}
					}
				default:
					errs.Add(errors.New("unsupported metric type"))
				}
			}
		}
		addResourceTargetInfo(resource, settings, mostRecentTimestamp, c)
	}

	return annots, errs
}

func isSameMetric(ts *prompb.TimeSeries, lbls []labelpb.ZLabel) bool {
	if len(ts.Labels) != len(lbls) {
		return false
	}
	for i, l := range ts.Labels {
		if l.Name != ts.Labels[i].Name || l.Value != ts.Labels[i].Value {
			return false
		}
	}
	return true
}

// addExemplars adds exemplars for the dataPoint. For each exemplar, if it can find a bucket bound corresponding to its value,
// the exemplar is added to the bucket bound's time series, provided that the time series' has samples.
func (c *PrometheusConverter) addExemplars(ctx context.Context, dataPoint pmetric.HistogramDataPoint, bucketBounds []bucketBoundsData) error {
	if len(bucketBounds) == 0 {
		return nil
	}

	exemplars, err := getPromExemplars(ctx, &c.everyN, dataPoint)
	if err != nil {
		return err
	}
	if len(exemplars) == 0 {
		return nil
	}

	sort.Sort(byBucketBoundsData(bucketBounds))
	for _, exemplar := range exemplars {
		for _, bound := range bucketBounds {
			if err := c.everyN.checkContext(ctx); err != nil {
				return err
			}
			if len(bound.ts.Samples) > 0 && exemplar.Value <= bound.bound {
				bound.ts.Exemplars = append(bound.ts.Exemplars, exemplar)
				break
			}
		}
	}

	return nil
}

// addSample finds a TimeSeries that corresponds to lbls, and adds sample to it.
// If there is no corresponding TimeSeries already, it's created.
// The corresponding TimeSeries is returned.
// If either lbls is nil/empty or sample is nil, nothing is done.
func (c *PrometheusConverter) addSample(sample *prompb.Sample, lbls []labelpb.ZLabel) *prompb.TimeSeries {
	if sample == nil || len(lbls) == 0 {
		// This shouldn't happen
		return nil
	}

	ts, _ := c.getOrCreateTimeSeries(lbls)
	ts.Samples = append(ts.Samples, *sample)
	return ts
}
