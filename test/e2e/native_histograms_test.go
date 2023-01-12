// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestQueryNativeHistograms(t *testing.T) {
	e, err := e2e.NewDockerEnvironment("nat-hist-query")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "ha1", e2ethanos.DefaultPromConfig("prom-ha", 0, "", "", e2ethanos.LocalPrometheusTarget), "", "quay.io/prometheus/prometheus:v2.40.5", "", "native-histograms", "remote-write-receiver")
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "ha2", e2ethanos.DefaultPromConfig("prom-ha", 1, "", "", e2ethanos.LocalPrometheusTarget), "", "quay.io/prometheus/prometheus:v2.40.5", "", "native-histograms", "remote-write-receiver")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	querier := e2ethanos.NewQuerierBuilder(e, "querier", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	rawRemoteWriteURL1 := "http://" + prom1.Endpoint("http") + "/api/v1/write"
	rawRemoteWriteURL2 := "http://" + prom2.Endpoint("http") + "/api/v1/write"

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	ts := time.Now()

	getTs := func() time.Time {
		return ts
	}

	testutil.Ok(t, storeWriteRequest(ctx, rawRemoteWriteURL1, nativeHistogramWriteRequest(ts)))
	testutil.Ok(t, storeWriteRequest(ctx, rawRemoteWriteURL2, nativeHistogramWriteRequest(ts)))

	// Make sure we can query native histogram directly from Prometheus.
	queryAndAssertSeries(t, ctx, prom1.Endpoint("http"), func() string { return "test_histogram" }, getTs, promclient.QueryOptions{}, []model.Metric{
		{
			"__name__": "test_histogram",
			"foo":      "bar",
		},
	})
	queryAndAssertSeries(t, ctx, prom2.Endpoint("http"), func() string { return "test_histogram" }, getTs, promclient.QueryOptions{}, []model.Metric{
		{
			"__name__": "test_histogram",
			"foo":      "bar",
		},
	})

	externalLabels := map[string]string{
		"prometheus": "prom-ha",
	}
	queryAndAssertRawResult(t, ctx, querier.Endpoint("http"), func() string { return "test_histogram" }, getTs, promclient.QueryOptions{Deduplicate: true}, expectedNativeHistogramResult(ts, externalLabels))
}

func TestWriteNativeHistograms(t *testing.T) {
	e, err := e2e.NewDockerEnvironment("nat-hist-write")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	ingestor0 := e2ethanos.NewReceiveBuilder(e, "ingestor0").WithIngestionEnabled().WithNativeHistograms().Init()
	ingestor1 := e2ethanos.NewReceiveBuilder(e, "ingestor1").WithIngestionEnabled().WithNativeHistograms().Init()

	h := receive.HashringConfig{
		Endpoints: []string{
			ingestor0.InternalEndpoint("grpc"),
			ingestor1.InternalEndpoint("grpc"),
		},
	}

	router0 := e2ethanos.NewReceiveBuilder(e, "router0").WithRouting(2, h).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(ingestor0, ingestor1, router0))

	querier := e2ethanos.NewQuerierBuilder(e, "querier", ingestor0.InternalEndpoint("grpc"), ingestor1.InternalEndpoint("grpc")).WithReplicaLabels("receive").Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	rawRemoteWriteURL := "http://" + router0.Endpoint("remote-write") + "/api/v1/receive"

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	ts := time.Now()

	getTs := func() time.Time {
		return ts
	}

	testutil.Ok(t, storeWriteRequest(ctx, rawRemoteWriteURL, nativeHistogramWriteRequest(ts)))

	externalLabels := map[string]string{
		"tenant_id": "default-tenant",
	}
	queryAndAssertRawResult(t, ctx, querier.Endpoint("http"), func() string { return "test_histogram" }, getTs, promclient.QueryOptions{Deduplicate: true}, expectedNativeHistogramResult(ts, externalLabels))

	queryAndAssertSeries(t, ctx, querier.Endpoint("http"), func() string { return "test_sample" }, getTs, promclient.QueryOptions{Deduplicate: true}, []model.Metric{
		{
			"__name__":  "test_sample",
			"bar":       "foo",
			"tenant_id": "default-tenant",
		},
	})
}

func nativeHistogramWriteRequest(ts time.Time) *prompb.WriteRequest {
	return &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "test_histogram"},
					{Name: "foo", Value: "bar"},
				},
				Histograms: []prompb.Histogram{
					remote.HistogramToHistogramProto(ts.UnixMilli(), &histogram.Histogram{
						Count:         5,
						ZeroCount:     2,
						Sum:           18.4,
						ZeroThreshold: 0.1,
						Schema:        1,
						PositiveSpans: []histogram.Span{
							{Offset: 0, Length: 2},
							{Offset: 1, Length: 2},
						},
						PositiveBuckets: []int64{1, 1, -1, 0}, // counts: 1, 2, 1, 1 (total 5)
					}),
				},
			},
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "test_sample"},
					{Name: "bar", Value: "foo"},
				},
				Samples: []prompb.Sample{
					{Timestamp: ts.UnixMilli(), Value: 1.2},
				},
			},
		},
	}
}

// Code below this line is a workaround until https://github.com/prometheus/common/pull/417 is available.
func expectedNativeHistogramResult(ts time.Time, externalLabels map[string]string) string {
	ls := make([]string, 0, len(externalLabels))
	for k, v := range externalLabels {
		ls = append(ls, fmt.Sprintf("\"%v\":\"%v\"", k, v))
	}

	return fmt.Sprintf(`
[
	{
		"metric": {
			"__name__": "test_histogram",
			"foo": "bar",
			%v
		},
		"histogram": [
			%v,
			{
				"count": "5",
				"sum": "18.4",
				"buckets": [
					[
						3,
						"-0.1",
						"0.1",
						"2"
					],
					[
						0,
						"0.7071067811865475",
						"1",
						"1"
					],
					[
						0,
						"1",
						"1.414213562373095",
						"2"
					],
					[
						0,
						"2",
						"2.82842712474619",
						"1"
					],
					[
						0,
						"2.82842712474619",
						"4",
						"1"
					]
				]
			}
		]
	}
]
`, strings.Join(ls, ","), toPromTs(ts))
}

func toPromTs(ts time.Time) string {
	promTs := fmt.Sprintf("%.3f", float64(ts.UnixMilli())/1000)
	return strings.TrimLeft(promTs, "0")
}

func queryAndAssertRawResult(t testing.TB, ctx context.Context, addr string, q func() string, ts func() time.Time, opts promclient.QueryOptions, expectedResult string) {
	t.Helper()

	var result []byte

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	_ = logger.Log(
		"caller", "queryAndAssertRawResult",
		"msg", fmt.Sprintf("Waiting for raw results for query %s", q()),
	)

	testutil.Ok(t, runutil.RetryWithLog(logger, 5*time.Second, ctx.Done(), func() error {
		res, err := simpleInstantQueryRawResult(t, ctx, addr, q, ts, opts)
		if err != nil {
			return err
		}
		result = res
		return nil
	}))

	require.JSONEq(t, expectedResult, string(result))
}

func simpleInstantQueryRawResult(t testing.TB, ctx context.Context, addr string, q func() string, ts func() time.Time, opts promclient.QueryOptions) ([]byte, error) {
	_, rawRes, warnings, err := promclient.NewDefaultClient().QueryInstant(ctx, urlParse(t, "http://"+addr), q(), ts(), opts)
	if err != nil {
		return nil, err
	}

	if len(warnings) > 0 {
		return nil, errors.Errorf("unexpected warnings %s", warnings)
	}

	if len(rawRes) <= 0 {
		return rawRes, errors.Errorf("unexpected result size, expected %d; result %d: %s", 0, len(rawRes), rawRes)
	}

	return rawRes, nil
}
