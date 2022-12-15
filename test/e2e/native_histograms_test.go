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

	"github.com/efficientgo/e2e"
	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestQueryNativeHistograms(t *testing.T) {
	e, err := e2e.NewDockerEnvironment("nat-hist-query")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "prom", e2ethanos.DefaultPromConfig("prom-alone", 0, "", "", e2ethanos.LocalPrometheusTarget), "", "quay.io/prometheus/prometheus:v2.40.5", "", "native-histograms", "remote-write-receiver")
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	querier := e2ethanos.NewQuerierBuilder(e, "querier", sidecar.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	rawRemoteWriteURL := "http://" + prom.Endpoint("http") + "/api/v1/write"

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	ts := time.Now()

	getTs := func() time.Time {
		return ts
	}

	testutil.Ok(t, storeWriteRequest(ctx, rawRemoteWriteURL, nativeHistogramWriteRequest(ts)))

	// Make sure we can query native histogram directly from Prometheus.
	queryAndAssertSeries(t, ctx, prom.Endpoint("http"), func() string { return "test_histogram" }, getTs, promclient.QueryOptions{}, []model.Metric{
		{
			"__name__": "test_histogram",
			"foo":      "bar",
		},
	})

	queryAndAssertError(t, ctx, querier.Endpoint("http"), func() string { return "test_histogram" }, getTs, promclient.QueryOptions{}, "invalid chunk encoding")
}

func TestWriteNativeHistograms(t *testing.T) {
	e, err := e2e.NewDockerEnvironment("nat-hist-write")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	receiver := e2ethanos.NewReceiveBuilder(e, "receiver").WithIngestionEnabled().Init()
	testutil.Ok(t, e2e.StartAndWaitReady(receiver))

	querier := e2ethanos.NewQuerierBuilder(e, "querier", receiver.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	rawRemoteWriteURL := "http://" + receiver.Endpoint("remote-write") + "/api/v1/receive"

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	ts := time.Now()

	getTs := func() time.Time {
		return ts
	}

	testutil.Ok(t, storeWriteRequest(ctx, rawRemoteWriteURL, nativeHistogramWriteRequest(ts)))

	queryAndAssertSeries(t, ctx, querier.Endpoint("http"), func() string { return "test_histogram" }, getTs, promclient.QueryOptions{}, []model.Metric{})
	queryAndAssertSeries(t, ctx, querier.Endpoint("http"), func() string { return "test_sample" }, getTs, promclient.QueryOptions{}, []model.Metric{
		{
			"__name__":  "test_sample",
			"bar":       "foo",
			"receive":   "receive-receiver",
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
						ZeroThreshold: 1e-100,
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

func queryAndAssertError(t testing.TB, ctx context.Context, addr string, q func() string, ts func() time.Time, opts promclient.QueryOptions, str string) {
	t.Helper()

	client := promclient.NewDefaultClient()

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	_ = logger.Log(
		"caller", "instantQuery",
		"msg", fmt.Sprintf("Waiting for result with error containing %q.", str),
	)
	testutil.Ok(t, runutil.RetryWithLog(logger, 5*time.Second, ctx.Done(), func() error {
		_, _, err := client.QueryInstant(ctx, urlParse(t, "http://"+addr), q(), ts(), opts)
		return errorContainsString(t, err, str)
	}))
}

func errorContainsString(tb testing.TB, err error, str string) error {
	tb.Helper()

	if err == nil {
		return fmt.Errorf("expected error containing string %q, but error is nil", str)
	}

	if !strings.Contains(err.Error(), str) {
		return fmt.Errorf("expected error containing string %q, but got %q", str, err.Error())
	}

	return nil
}
