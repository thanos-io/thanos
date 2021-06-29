// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestMetadataAPI_Fanout(t *testing.T) {
	t.Parallel()

	netName := "e2e_test_metadata_fanout"

	s, err := e2e.NewScenario(netName)
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	// 2x Prometheus.
	// Each Prometheus scrapes its own metrics and Sidecar's metrics.
	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom1",
		defaultPromConfig("ha", 0, "", "", "localhost:9090", "sidecar-prom1:8080"),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)

	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom2",
		defaultPromConfig("ha", 1, "", "", "localhost:9090", "sidecar-prom2:8080"),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	stores := []string{sidecar1.GRPCNetworkEndpoint(), sidecar2.GRPCNetworkEndpoint()}
	q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "query", stores).
		WithMetadataAddresses(stores).
		Build()
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))
	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"thanos_query_metadata_apis_dns_provider_results"}, e2e.WaitMissingMetrics))

	var promMeta map[string][]metadatapb.Meta
	// Wait metadata response to be ready as Prometheus gets metadata after scrape.
	testutil.Ok(t, runutil.Retry(3*time.Second, ctx.Done(), func() error {
		promMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, mustURLParse(t, "http://"+prom1.HTTPEndpoint()), "", -1)
		testutil.Ok(t, err)
		if len(promMeta) > 0 {
			return nil
		}
		return fmt.Errorf("empty metadata response from Prometheus")
	}))

	thanosMeta, err := promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", -1)
	testutil.Ok(t, err)
	testutil.Assert(t, len(thanosMeta) > 0, "got empty metadata response from Thanos")

	// Metadata response from Prometheus and Thanos Querier should be the same after deduplication.
	metadataEqual(t, thanosMeta, promMeta)

	// We only expect to see one metadata returned.
	thanosMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", 1)
	testutil.Ok(t, err)
	testutil.Equals(t, len(thanosMeta), 1)

	// We only expect to see ten metadata returned.
	thanosMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", 10)
	testutil.Ok(t, err)
	testutil.Equals(t, len(thanosMeta), 10)

	// No metadata returned.
	thanosMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", 0)
	testutil.Ok(t, err)
	testutil.Equals(t, len(thanosMeta), 0)

	// Only prometheus_build_info metric will be returned.
	thanosMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "prometheus_build_info", -1)
	testutil.Ok(t, err)
	testutil.Assert(t, len(thanosMeta) == 1 && len(thanosMeta["prometheus_build_info"]) > 0, "expected one prometheus_build_info metadata from Thanos, got %v", thanosMeta)
}

func metadataEqual(t *testing.T, meta1, meta2 map[string][]metadatapb.Meta) {
	// The two responses should have equal # of entries.
	testutil.Equals(t, len(meta1), len(meta2))

	for metric := range meta1 {
		// Get metadata for the metric.
		meta1MetricMeta := meta1[metric]
		meta2MetricMeta, ok := meta2[metric]
		testutil.Assert(t, ok)

		sort.Slice(meta1MetricMeta, func(i, j int) bool {
			return meta1MetricMeta[i].Help < meta1MetricMeta[j].Help
		})
		sort.Slice(meta2MetricMeta, func(i, j int) bool {
			return meta2MetricMeta[i].Help < meta2MetricMeta[j].Help
		})
		testutil.Equals(t, meta1MetricMeta, meta2MetricMeta)
	}
}
