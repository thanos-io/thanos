// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
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

	queryAndAssert(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
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
	queryAndAssert(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
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
}

func urlParse(t *testing.T, addr string) *url.URL {
	u, err := url.Parse(addr)
	testutil.Ok(t, err)

	return u
}

func queryAndAssert(t *testing.T, ctx context.Context, addr string, query string, opts promclient.QueryOptions, expected []model.Metric) {
	t.Helper()

	fmt.Println("queryAndAssert: Waiting for", len(expected), "results for query", query)
	var result model.Vector
	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		res, warnings, err := promclient.NewDefaultClient().QueryInstant(ctx, urlParse(t, "http://"+addr), query, time.Now(), opts)
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			// we don't expect warnings.
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		if len(result) != len(res) {
			fmt.Println("queryAndAssert: New result:", res)
		}

		if len(res) != len(expected) {
			return errors.Errorf("unexpected result size, expected %d; result: %v", len(expected), res)
		}
		result = res
		return nil
	}))

	sortResults(result)
	for i, exp := range expected {
		testutil.Equals(t, exp, result[i].Metric)
	}
}
