// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2emon "github.com/efficientgo/e2e/monitoring"

	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestMetadataAPI_Fanout(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("metadata-fanout")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	// 2x Prometheus.
	// Each Prometheus scrapes its own metrics and Sidecar's metrics.
	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(
		e,
		"prom1",
		e2ethanos.DefaultPromConfig("ha", 0, "", "", e2ethanos.LocalPrometheusTarget, "sidecar-prom1:8080"),
		"",
		e2ethanos.DefaultPrometheusImage(), "",
	)
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(
		e,
		"prom2",
		e2ethanos.DefaultPromConfig("ha", 1, "", "", e2ethanos.LocalPrometheusTarget, "sidecar-prom2:8080"),
		"",
		e2ethanos.DefaultPrometheusImage(), "",
	)
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	stores := []string{sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc")}
	q := e2ethanos.NewQuerierBuilder(
		e, "query", stores...).
		WithMetadataAddresses(stores...).
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(2), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))
	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(2), []string{"thanos_query_metadata_apis_dns_provider_results"}, e2emon.WaitMissingMetrics()))

	var promMeta map[string][]metadatapb.Meta
	// Wait metadata response to be ready as Prometheus gets metadata after scrape.
	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() error {
		promMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, urlParse(t, "http://"+prom1.Endpoint("http")), "", -1)
		if err != nil {
			return err
		}
		if len(promMeta) > 0 {
			return nil
		}
		return fmt.Errorf("empty metadata response from Prometheus")
	}))

	var thanosMeta map[string][]metadatapb.Meta
	// Retry until length of metadata response is the same as Prometheus.
	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() error {
		thanosMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, urlParse(t, "http://"+q.Endpoint("http")), "", -1)
		if err != nil {
			return err
		}
		if len(thanosMeta) == len(promMeta) {
			return nil
		}

		return fmt.Errorf("different metadata response from Prometheus")
	}))

	testutil.Assert(t, len(thanosMeta) > 0, "got empty metadata response from Thanos")

	// Metadata response from Prometheus and Thanos Querier should be the same after deduplication.
	metadataEqual(t, thanosMeta, promMeta)

	// We only expect to see one metadata returned.
	thanosMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, urlParse(t, "http://"+q.Endpoint("http")), "", 1)
	testutil.Ok(t, err)
	testutil.Equals(t, len(thanosMeta), 1)

	// We only expect to see ten metadata returned.
	thanosMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, urlParse(t, "http://"+q.Endpoint("http")), "", 10)
	testutil.Ok(t, err)
	testutil.Equals(t, len(thanosMeta), 10)

	// No metadata returned.
	thanosMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, urlParse(t, "http://"+q.Endpoint("http")), "", 0)
	testutil.Ok(t, err)
	testutil.Equals(t, len(thanosMeta), 0)

	// Only prometheus_build_info metric will be returned.
	thanosMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, urlParse(t, "http://"+q.Endpoint("http")), "prometheus_build_info", -1)
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
