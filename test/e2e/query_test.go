// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"testing"
	"time"
	"unsafe"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

// NOTE: by using aggregation all results are now unsorted.
const queryUpWithoutInstance = "sum(up) without (instance)"

// defaultPromConfig returns Prometheus config that sets Prometheus to:
// * expose 2 external labels, source and replica.
// * scrape fake target. This will produce up == 0 metric which we can assert on.
// * optionally remote write endpoint to write into.
func defaultPromConfig(name string, replica int, remoteWriteEndpoint string) string {
	config := fmt.Sprintf(`
global:
  external_labels:
    prometheus: %v
    replica: %v
scrape_configs:
- job_name: 'myself'
  # Quick scrapes for test purposes.
  scrape_interval: 1s
  scrape_timeout: 1s
  static_configs:
  - targets: ['localhost:9090']
`, name, replica)

	if remoteWriteEndpoint != "" {
		config = fmt.Sprintf(`
%s
remote_write:
- url: "%s"
  # Don't spam receiver on mistake.
  queue_config:
    min_backoff: 2s
    max_backoff: 10s
`, config, remoteWriteEndpoint)
	}
	return config
}

func sortResults(res model.Vector) {
	sort.Slice(res, func(i, j int) bool {
		return res[i].String() < res[j].String()
	})
}

func TestQuery(t *testing.T) {
	t.Parallel()

	s, err := e2e.NewScenario("e2e_test_query")
	testutil.Ok(t, err)
	defer s.Close()

	receiver, err := e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "1", 1)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(receiver))

	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(s.SharedDir(), "e2e_test_query", "alone", defaultPromConfig("prom-alone", 0, ""), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(s.SharedDir(), "e2e_test_query", "remote-and-sidecar", defaultPromConfig("prom-both-remote-write-and-sidecar", 1234, e2ethanos.RemoteWriteEndpoint(receiver.NetworkEndpoint(81))), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	prom3, sidecar3, err := e2ethanos.NewPrometheusWithSidecar(s.SharedDir(), "e2e_test_query", "ha1", defaultPromConfig("prom-ha", 0, ""), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	prom4, sidecar4, err := e2ethanos.NewPrometheusWithSidecar(s.SharedDir(), "e2e_test_query", "ha2", defaultPromConfig("prom-ha", 1, ""), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2, prom3, sidecar3, prom4, sidecar4))

	// Querier. Both fileSD and directly by flags.
	q, err := e2ethanos.NewQuerier(
		s.SharedDir(), "1",
		[]string{sidecar1.GRPCNetworkEndpoint(), sidecar2.GRPCNetworkEndpoint(), receiver.GRPCNetworkEndpoint()},
		[]string{sidecar3.GRPCNetworkEndpoint(), sidecar4.GRPCNetworkEndpoint()},
	)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	testutil.Ok(t, q.WaitSumMetrics(e2e.Equals(5), "thanos_store_nodes_grpc_connections"))

	queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "prom-alone",
			"replica":    "0",
		},
		{
			"job":        "myself",
			"prometheus": "prom-both-remote-write-and-sidecar",
			"receive":    "1",
			"replica":    "1234",
		},
		{
			"job":        "myself",
			"prometheus": "prom-both-remote-write-and-sidecar",
			"replica":    "1234",
		},
		{
			"job":        "myself",
			"prometheus": "prom-ha",
			"replica":    "0",
		},
		{
			"job":        "myself",
			"prometheus": "prom-ha",
			"replica":    "1",
		},
	})

	// With deduplication.
	queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
		Deduplicate: true,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "prom-alone",
		},
		{
			"job":        "myself",
			"prometheus": "prom-both-remote-write-and-sidecar",
			"receive":    "1",
		},
		{
			"job":        "myself",
			"prometheus": "prom-both-remote-write-and-sidecar",
		},
		{
			"job":        "myself",
			"prometheus": "prom-ha",
		},
	})

	callRemoteReadAndAssertSeries(t, ctx, q.HTTPEndpoint(), &prompb.Query{
		StartTimestampMs: 0,
		EndTimestampMs:   math.MaxInt64,
		Matchers: []*prompb.LabelMatcher{
			{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "__name__",
				Value: "up",
			},
		},
	}, []*prompb.ChunkedSeries{
		{
			Labels: []prompb.Label{{Name: "job", Value: "myself"}},
		},
	})
}

func urlParse(t *testing.T, addr string) *url.URL {
	u, err := url.Parse(addr)
	testutil.Ok(t, err)

	return u
}

func instantQuery(t *testing.T, ctx context.Context, addr string, q string, opts promclient.QueryOptions, expectedSeriesLen int) model.Vector {
	t.Helper()

	fmt.Println("queryAndAssert: Waiting for", expectedSeriesLen, "results for query", q)
	var result model.Vector
	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	testutil.Ok(t, runutil.RetryWithLog(logger, time.Second, ctx.Done(), func() error {
		res, warnings, err := promclient.QueryInstant(ctx, nil, urlParse(t, "http://"+addr), q, time.Now(), opts)
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		if len(res) != expectedSeriesLen {
			return errors.Errorf("unexpected result size, expected %d; result %d: %v", expectedSeriesLen, len(res), res)
		}
		result = res
		return nil
	}))
	sortResults(result)
	return result
}

func queryAndAssertSeries(t *testing.T, ctx context.Context, addr string, q string, opts promclient.QueryOptions, expected []model.Metric) {
	t.Helper()

	result := instantQuery(t, ctx, addr, q, opts, len(expected))
	for i, exp := range expected {
		testutil.Equals(t, exp, result[i].Metric)
	}
}

func queryAndAssert(t *testing.T, ctx context.Context, addr string, q string, opts promclient.QueryOptions, expected model.Vector) {
	t.Helper()

	sortResults(expected)
	result := instantQuery(t, ctx, addr, q, opts, len(expected))
	for _, r := range result {
		r.Timestamp = 0 // Does not matter for us.
	}
	testutil.Equals(t, expected, result)
}

func callRemoteReadAndAssertSeries(t *testing.T, ctx context.Context, addr string, q *prompb.Query, expected []*prompb.ChunkedSeries) {
	t.Helper()

	fmt.Println("callRemoteReadAndAssertSeries: Waiting for", len(expected), "results for remote read", q)
	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	var series []*prompb.ChunkedSeries
	testutil.Ok(t, runutil.RetryWithLog(logger, time.Second, ctx.Done(), func() error {
		resp, err := store.StartPromSeries(ctx, logger, q, http.DefaultClient, urlParse(t, "http://"+addr), []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS, prompb.ReadRequest_SAMPLES})
		if err != nil {
			return err
		}

		series = series[:0]
		// TODO(bwplotka): Put read limit as a flag.
		stream := remote.NewChunkedReader(resp.Body, remote.DefaultChunkedReadLimit, nil)
		for {
			res := &prompb.ChunkedReadResponse{}
			err := stream.NextProto(res)
			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.Wrap(err, "next proto")
			}

			if res.QueryIndex != 0 {
				return errors.Errorf("unexpected query index %d", res.QueryIndex)
			}
			series = append(series, *(*[]*prompb.ChunkedSeries)(unsafe.Pointer(&res.ChunkedSeries))...)
		}

		if len(expected) != len(series) {
			return errors.Errorf("unexpected result size, expected %d; result %d: %v", len(expected), len(series), series)
		}
		return nil
	}))

	for i, exp := range expected {
		testutil.Equals(t, exp.Labels, series[i].Labels)
	}
}
