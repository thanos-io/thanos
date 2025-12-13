// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/prometheus/common/model"

	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

// TestCapnpStoreAPI tests the Cap'n Proto Store API end-to-end.
// It sets up a Receive and a Querier that connects to it using Cap'n Proto instead of gRPC.
func TestCapnpStoreAPI(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("capnp-store-api")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	i := e2ethanos.NewReceiveBuilder(e, "ingestor").
		WithIngestionEnabled().
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(i))

	prom := e2ethanos.NewPrometheus(
		e, "1",
		e2ethanos.DefaultPromConfig("prom1", 0, e2ethanos.RemoteWriteEndpoint(i.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget),
		"",
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, e2e.StartAndWaitReady(prom))

	// Setup Querier connecting via Cap'n Proto.
	q := e2ethanos.NewQuerierBuilder(e, "1").
		WithCapnpStoreAddresses(i.InternalEndpoint("capnp")).
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	time.Sleep(5 * time.Second)

	queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "prom1",
			"receive":    "receive-ingestor",
			"replica":    "0",
			"tenant_id":  "default-tenant",
		},
	})
}

// TestCapnpStoreAPI_LabelNames tests that LabelNames works over Cap'n Proto.
func TestCapnpStoreAPI_LabelNames(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("capnp-lblnames")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	i := e2ethanos.NewReceiveBuilder(e, "ingestor").
		WithIngestionEnabled().
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(i))

	prom := e2ethanos.NewPrometheus(
		e, "1",
		e2ethanos.DefaultPromConfig("prom1", 0, e2ethanos.RemoteWriteEndpoint(i.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget),
		"",
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, e2e.StartAndWaitReady(prom))

	q := e2ethanos.NewQuerierBuilder(e, "1").
		WithCapnpStoreAddresses(i.InternalEndpoint("capnp")).
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	time.Sleep(5 * time.Second)

	labelNames, err := promclient.NewDefaultClient().LabelNamesInGRPC(ctx, mustURLParse(t, "http://"+q.Endpoint("http")), nil, 0, time.Now().UnixMilli(), 0)
	testutil.Ok(t, err)
	testutil.Assert(t, len(labelNames) > 0, "expected at least one label name, got %v", labelNames)

	hasJob := false
	for _, name := range labelNames {
		if name == "job" {
			hasJob = true
			break
		}
	}
	testutil.Assert(t, hasJob, "expected 'job' label in label names, got %v", labelNames)
}

// TestCapnpStoreAPI_LabelValues tests that LabelValues works over Cap'n Proto.
func TestCapnpStoreAPI_LabelValues(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("capnp-lblvals")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	i := e2ethanos.NewReceiveBuilder(e, "ingestor").
		WithIngestionEnabled().
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(i))

	prom := e2ethanos.NewPrometheus(
		e, "1",
		e2ethanos.DefaultPromConfig("prom1", 0, e2ethanos.RemoteWriteEndpoint(i.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget),
		"",
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, e2e.StartAndWaitReady(prom))

	q := e2ethanos.NewQuerierBuilder(e, "1").
		WithCapnpStoreAddresses(i.InternalEndpoint("capnp")).
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	time.Sleep(5 * time.Second)

	labelValues, err := promclient.NewDefaultClient().LabelValuesInGRPC(ctx, mustURLParse(t, "http://"+q.Endpoint("http")), "job", nil, 0, time.Now().UnixMilli(), 0)
	testutil.Ok(t, err)
	testutil.Assert(t, len(labelValues) > 0, "expected at least one label value for 'job', got %v", labelValues)

	hasMyself := false
	for _, val := range labelValues {
		if val == "myself" {
			hasMyself = true
			break
		}
	}
	testutil.Assert(t, hasMyself, "expected 'myself' value for 'job' label, got %v", labelValues)
}

// TestCapnpStoreAPI_MixedProtocols tests that gRPC and Cap'n Proto stores can be used together.
func TestCapnpStoreAPI_MixedProtocols(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("capnp-mixed")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	i1 := e2ethanos.NewReceiveBuilder(e, "capnp-ingestor").
		WithIngestionEnabled().
		WithLabel("store", "capnp").
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(i1))

	i2 := e2ethanos.NewReceiveBuilder(e, "grpc-ingestor").
		WithIngestionEnabled().
		WithLabel("store", "grpc").
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(i2))

	prom1 := e2ethanos.NewPrometheus(
		e, "capnp",
		e2ethanos.DefaultPromConfig("prom-capnp", 0, e2ethanos.RemoteWriteEndpoint(i1.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget),
		"",
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, e2e.StartAndWaitReady(prom1))

	prom2 := e2ethanos.NewPrometheus(
		e, "grpc",
		e2ethanos.DefaultPromConfig("prom-grpc", 0, e2ethanos.RemoteWriteEndpoint(i2.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget),
		"",
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, e2e.StartAndWaitReady(prom2))

	// Querier connects to i1 via Cap'n Proto and i2 via gRPC.
	q := e2ethanos.NewQuerierBuilder(e, "1").
		WithCapnpStoreAddresses(i1.InternalEndpoint("capnp")).
		WithStoreAddresses(i2.InternalEndpoint("grpc")).
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

	time.Sleep(5 * time.Second)

	queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "prom-capnp",
			"receive":    "receive-capnp-ingestor",
			"replica":    "0",
			"store":      "capnp",
			"tenant_id":  "default-tenant",
		},
		{
			"job":        "myself",
			"prometheus": "prom-grpc",
			"receive":    "receive-grpc-ingestor",
			"replica":    "0",
			"store":      "grpc",
			"tenant_id":  "default-tenant",
		},
	})
}

func mustURLParse(t testing.TB, addr string) *url.URL {
	u, err := url.Parse(addr)
	testutil.Ok(t, err)
	return u
}
