// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Provenance-includes-location: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/247a9f996e09a83cdc25addf70c05e42b8b30186/pkg/translator/prometheusremotewrite/testutil_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright The OpenTelemetry Authors.

package otlptranslator

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func getIntGaugeMetric(name string, attributes pcommon.Map, value int64, ts uint64) pmetric.Metric {
	metric := pmetric.NewMetric()
	metric.SetName(name)
	dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	if strings.HasPrefix(name, "staleNaN") {
		dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
	}
	dp.SetIntValue(value)
	attributes.CopyTo(dp.Attributes())

	dp.SetStartTimestamp(pcommon.Timestamp(0))
	dp.SetTimestamp(pcommon.Timestamp(ts))
	return metric
}

func getIntSumMetric(name string, attributes pcommon.Map, value int64, ts uint64) pmetric.Metric {
	metric := pmetric.NewMetric()
	metric.SetName(name)
	metric.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := metric.Sum().DataPoints().AppendEmpty()
	if strings.HasPrefix(name, "staleNaN") {
		dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
	}
	dp.SetIntValue(value)
	attributes.CopyTo(dp.Attributes())

	dp.SetStartTimestamp(pcommon.Timestamp(0))
	dp.SetTimestamp(pcommon.Timestamp(ts))
	return metric
}
