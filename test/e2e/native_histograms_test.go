package e2e_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/thanos-io/thanos/pkg/promclient"
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

	writeRequest(t, ctx, rawRemoteWriteURL, nativeHistogramWriteRequest(ts))

	// Make sure we can query native histogram directly from Prometheus.
	queryAndAssertSeries(t, ctx, prom.Endpoint("http"), func() string { return "test_histogram" }, time.Now, promclient.QueryOptions{}, []model.Metric{
		{
			"__name__": "test_histogram",
			"foo":      "bar",
		},
	})

	// Querying from querier should fail.
	_, _, err = promclient.NewDefaultClient().QueryInstant(ctx, urlParse(t, "http://"+querier.Endpoint("http")), "test_histogram", ts, promclient.QueryOptions{})
	testutil.ErrorContainsString(t, err, "invalid chunk encoding")
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

	writeRequest(t, ctx, rawRemoteWriteURL, nativeHistogramWriteRequest(time.Now()))

	queryAndAssertSeries(t, ctx, querier.Endpoint("http"), func() string { return "test_histogram" }, time.Now, promclient.QueryOptions{}, []model.Metric{})
	queryAndAssertSeries(t, ctx, querier.Endpoint("http"), func() string { return "test_sample" }, time.Now, promclient.QueryOptions{}, []model.Metric{
		{
			"__name__":  "test_sample",
			"bar":       "foo",
			"receive":   "receive-receiver",
			"tenant_id": "default-tenant",
		},
	})
}

func writeRequest(t *testing.T, ctx context.Context, rawRemoteWriteURL string, req *prompb.WriteRequest) {
	t.Helper()

	remoteWriteURL, err := url.Parse(rawRemoteWriteURL)
	testutil.Ok(t, err)

	client, err := remote.NewWriteClient("remote-write-client", &remote.ClientConfig{
		URL:     &config_util.URL{URL: remoteWriteURL},
		Timeout: model.Duration(30 * time.Second),
	})
	testutil.Ok(t, err)

	var buf []byte
	pBuf := proto.NewBuffer(nil)
	err = pBuf.Marshal(req)
	testutil.Ok(t, err)

	compressed := snappy.Encode(buf, pBuf.Bytes())

	err = client.Store(ctx, compressed)
	testutil.Ok(t, err)
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
