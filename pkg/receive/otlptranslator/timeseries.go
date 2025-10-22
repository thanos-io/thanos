// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Provenance-includes-location: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/95e8f8fdc2a9dc87230406c9a3cf02be4fd68bea/pkg/translator/prometheusremotewrite/metrics_to_prw.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright The OpenTelemetry Authors.

package otlptranslator

import "github.com/thanos-io/thanos/pkg/store/storepb/prompb"

// TimeSeries returns a slice of the prompb.TimeSeries that were converted from OTel format.
func (c *PrometheusConverter) TimeSeries() []prompb.TimeSeries {
	conflicts := 0
	for _, ts := range c.conflicts {
		conflicts += len(ts)
	}
	allTS := make([]prompb.TimeSeries, 0, len(c.unique)+conflicts)
	for _, ts := range c.unique {
		allTS = append(allTS, *ts)
	}
	for _, cTS := range c.conflicts {
		for _, ts := range cTS {
			allTS = append(allTS, *ts)
		}
	}

	return allTS
}

// Metadata returns a slice of the prompb.Metadata that were converted from OTel format.
func (c *PrometheusConverter) Metadata() []prompb.MetricMetadata {
	return c.metadata
}
