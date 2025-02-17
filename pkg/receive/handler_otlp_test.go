// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func TestOTLPWriteHandler(t *testing.T) {
	exportRequest := generateOTLPWriteRequest()

	buf, err := exportRequest.MarshalProto()
	require.NoError(t, err)

	appendables := []*fakeAppendable{
		{
			appender: newFakeAppender(nil, nil, nil),
		},
	}

	handlers, _, closeFunc, err := newTestHandlerHashring(appendables, 1, AlgorithmHashmod, false)
	require.NoError(t, err)
	defer func() {
		testutil.Ok(t, closeFunc())
		// Wait a few milliseconds for peer workers to process the queue.
		time.AfterFunc(50*time.Millisecond, func() {
			for _, h := range handlers {
				h.Close()
			}
		})
	}()

	for _, handler := range handlers {
		req := httptest.NewRequest("POST", "/api/v1/otlp", bytes.NewReader(buf))

		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set(handler.options.ReplicaHeader, "0")
		req.Header.Set(handler.options.TenantHeader, "test")

		recorder := httptest.NewRecorder()
		handler.receiveOTLPHTTP(recorder, req)

		result := recorder.Result()
		defer result.Body.Close()

		require.Equal(t, http.StatusOK, result.StatusCode)
		for _, app := range appendables {
			require.Greater(t, len(app.appender.(*fakeAppender).samples), 0)
			require.Greater(t, len(app.appender.(*fakeAppender).exemplars), 0)
		}
	}

}

func generateOTLPWriteRequest() pmetricotlp.ExportRequest {
	d := pmetric.NewMetrics()

	// Generate One Counter, One Gauge, One Histogram
	// with resource attributes: service.name="test-service", service.instance.id="test-instance", host.name="test-host"
	// with metric attribute: foo.bar="baz"

	timestamp := time.Now()

	resourceMetric := d.ResourceMetrics().AppendEmpty()
	resourceMetric.Resource().Attributes().PutStr("service.name", "test-service")
	resourceMetric.Resource().Attributes().PutStr("service.instance.id", "test-instance")
	resourceMetric.Resource().Attributes().PutStr("host.name", "test-host")

	scopeMetric := resourceMetric.ScopeMetrics().AppendEmpty()

	// Generate One Counter
	counterMetric := scopeMetric.Metrics().AppendEmpty()
	counterMetric.SetName("test-counter")
	counterMetric.SetDescription("test-counter-description")
	counterMetric.SetEmptySum()
	counterMetric.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	counterMetric.Sum().SetIsMonotonic(true)

	counterDataPoint := counterMetric.Sum().DataPoints().AppendEmpty()
	counterDataPoint.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	counterDataPoint.SetDoubleValue(10.0)
	counterDataPoint.Attributes().PutStr("foo.bar", "baz")

	counterExemplar := counterDataPoint.Exemplars().AppendEmpty()

	counterExemplar.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	counterExemplar.SetDoubleValue(10.0)
	counterExemplar.SetSpanID(pcommon.SpanID{0, 1, 2, 3, 4, 5, 6, 7})
	counterExemplar.SetTraceID(pcommon.TraceID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})

	// Generate One Gauge
	gaugeMetric := scopeMetric.Metrics().AppendEmpty()
	gaugeMetric.SetName("test-gauge")
	gaugeMetric.SetDescription("test-gauge-description")
	gaugeMetric.SetEmptyGauge()

	gaugeDataPoint := gaugeMetric.Gauge().DataPoints().AppendEmpty()
	gaugeDataPoint.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	gaugeDataPoint.SetDoubleValue(10.0)
	gaugeDataPoint.Attributes().PutStr("foo.bar", "baz")

	// Generate One Histogram
	histogramMetric := scopeMetric.Metrics().AppendEmpty()
	histogramMetric.SetName("test-histogram")
	histogramMetric.SetDescription("test-histogram-description")
	histogramMetric.SetEmptyHistogram()
	histogramMetric.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	histogramDataPoint := histogramMetric.Histogram().DataPoints().AppendEmpty()
	histogramDataPoint.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	histogramDataPoint.ExplicitBounds().FromRaw([]float64{0.0, 1.0, 2.0, 3.0, 4.0, 5.0})
	histogramDataPoint.BucketCounts().FromRaw([]uint64{2, 2, 2, 2, 2, 2})
	histogramDataPoint.SetCount(10)
	histogramDataPoint.SetSum(30.0)
	histogramDataPoint.Attributes().PutStr("foo.bar", "baz")

	return pmetricotlp.NewExportRequestFromMetrics(d)
}
