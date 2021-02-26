// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/thanos-io/thanos/pkg/promclient"
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

	q, err := e2ethanos.NewQuerier(
		s.SharedDir(),
		"query",
		[]string{sidecar1.GRPCNetworkEndpoint(), sidecar2.GRPCNetworkEndpoint()},
		nil,
		nil,
		nil,
		[]string{sidecar1.GRPCNetworkEndpoint(), sidecar2.GRPCNetworkEndpoint()},
		"",
		"",
	)

	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))
	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"thanos_query_metadata_apis_dns_provider_results"}, e2e.WaitMissingMetrics))

	promMeta, err := promclient.NewDefaultClient().MetadataInGRPC(ctx, mustURLParse(t, "http://"+prom1.HTTPEndpoint()), "", -1)
	testutil.Ok(t, err)

	thanosMeta, err := promclient.NewDefaultClient().MetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", -1)
	testutil.Ok(t, err)

	// Metadata response from Prometheus and Thanos Querier should be the same after deduplication.
	testutil.Equals(t, thanosMeta, promMeta)

	// We only expect to see one metadata returned.
	thanosMeta, err = promclient.NewDefaultClient().MetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", 1)
	testutil.Ok(t, err)
	testutil.Assert(t, true, len(thanosMeta) == 1)

	// We only expect to see ten metadata returned.
	thanosMeta, err = promclient.NewDefaultClient().MetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", 10)
	testutil.Ok(t, err)
	testutil.Assert(t, true, len(thanosMeta) == 10)

	// No metadata returned.
	thanosMeta, err = promclient.NewDefaultClient().MetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", 0)
	testutil.Ok(t, err)
	testutil.Assert(t, true, len(thanosMeta) == 0)

	// Only prometheus_build_info metric will be returned.
	thanosMeta, err = promclient.NewDefaultClient().MetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "prometheus_build_info", -1)
	testutil.Ok(t, err)
	testutil.Assert(t, true, len(thanosMeta) == 1 && len(thanosMeta["prometheus_build_info"]) > 0)
}
