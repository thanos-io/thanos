// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Provenance-includes-location: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/95e8f8fdc2a9dc87230406c9a3cf02be4fd68bea/pkg/translator/prometheusremotewrite/otlp_to_openmetrics_metadata.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright The OpenTelemetry Authors.

package otlptranslator

import (
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func otelMetricTypeToPromMetricType(otelMetric pmetric.Metric) prompb.MetricMetadata_MetricType {
	switch otelMetric.Type() {
	case pmetric.MetricTypeGauge:
		return prompb.MetricMetadata_GAUGE
	case pmetric.MetricTypeSum:
		metricType := prompb.MetricMetadata_GAUGE
		if otelMetric.Sum().IsMonotonic() {
			metricType = prompb.MetricMetadata_COUNTER
		}
		return metricType
	case pmetric.MetricTypeHistogram:
		return prompb.MetricMetadata_HISTOGRAM
	case pmetric.MetricTypeSummary:
		return prompb.MetricMetadata_SUMMARY
	case pmetric.MetricTypeExponentialHistogram:
		return prompb.MetricMetadata_HISTOGRAM
	}
	return prompb.MetricMetadata_UNKNOWN
}
