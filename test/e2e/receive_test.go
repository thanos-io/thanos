// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"testing"
	"time"

	"github.com/efficientgo/core/backoff"
	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/relabel"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

type DebugTransport struct{}

func (DebugTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	_, err := httputil.DumpRequestOut(r, false)
	if err != nil {
		return nil, err
	}
	return http.DefaultTransport.RoundTrip(r)
}

func ErrorHandler(_ http.ResponseWriter, _ *http.Request, err error) {
	log.Print("Response from receiver")
	log.Print(err)
}

func TestReceive(t *testing.T) {
	t.Parallel()

	t.Run("single_ingestor", func(t *testing.T) {
		/*
			The single_ingestor suite represents the simplest possible configuration of Thanos Receive.
			 ┌──────────┐
			 │  Prom    │
			 └────┬─────┘
			      │
			 ┌────▼─────┐
			 │ Ingestor │
			 └────┬─────┘
			      │
			 ┌────▼─────┐
			 │  Query   │
			 └──────────┘
			NB: Made with asciiflow.com - you can copy & paste the above there to modify.
		*/

		t.Parallel()
		e, err := e2e.NewDockerEnvironment("single-ingestor")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, e))

		// Setup Router Ingestor.
		i := e2ethanos.NewReceiveBuilder(e, "ingestor").WithIngestionEnabled().Init()
		testutil.Ok(t, e2e.StartAndWaitReady(i))

		// Setup Prometheus
		prom := e2ethanos.NewPrometheus(e, "1", e2ethanos.DefaultPromConfig("prom1", 0, e2ethanos.RemoteWriteEndpoint(i.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, e2e.StartAndWaitReady(prom))

		q := e2ethanos.NewQuerierBuilder(e, "1", i.InternalEndpoint("grpc")).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

		// We expect the data from each Prometheus instance to be replicated twice across our ingesting instances
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
	})

	t.Run("router_replication", func(t *testing.T) {
		/*
			The router_replication suite configures separate routing and ingesting components.
			It verifies that data ingested from Prometheus instances through the router is successfully replicated twice
			across the ingestors.

			  ┌───────┐       ┌───────┐      ┌───────┐
			  │       │       │       │      │       │
			  │ Prom1 │       │ Prom2 │      │ Prom3 │
			  │       │       │       │      │       │
			  └───┬───┘       └───┬───┘      └──┬────┘
			      │           ┌───▼────┐        │
			      └───────────►        ◄────────┘
			                  │ Router │
			      ┌───────────┤        ├──────────┐
			      │           └───┬────┘          │
			┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
			│           │   │           │   │           │
			│ Ingestor1 │   │ Ingestor2 │   │ Ingestor3 │
			│           │   │           │   │           │
			└─────┬─────┘   └─────┬─────┘   └─────┬─────┘
			      │           ┌───▼───┐           │
			      │           │       │           │
			      └───────────► Query ◄───────────┘
			                  │       │
			                  └───────┘

			NB: Made with asciiflow.com - you can copy & paste the above there to modify.
		*/

		t.Parallel()
		e, err := e2e.NewDockerEnvironment("routerReplica")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, e))

		// Setup 3 ingestors.
		i1 := e2ethanos.NewReceiveBuilder(e, "i1").WithIngestionEnabled().Init()
		i2 := e2ethanos.NewReceiveBuilder(e, "i2").WithIngestionEnabled().Init()
		i3 := e2ethanos.NewReceiveBuilder(e, "i3").WithIngestionEnabled().Init()

		h := receive.HashringConfig{
			Endpoints: []string{
				i1.InternalEndpoint("grpc"),
				i2.InternalEndpoint("grpc"),
				i3.InternalEndpoint("grpc"),
			},
		}

		// Setup 1 distributor with double replication
		r1 := e2ethanos.NewReceiveBuilder(e, "r1").WithRouting(2, h).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(i1, i2, i3, r1))

		prom1 := e2ethanos.NewPrometheus(e, "1", e2ethanos.DefaultPromConfig("prom1", 0, e2ethanos.RemoteWriteEndpoint(r1.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage())
		prom2 := e2ethanos.NewPrometheus(e, "2", e2ethanos.DefaultPromConfig("prom2", 0, e2ethanos.RemoteWriteEndpoint(r1.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage())
		prom3 := e2ethanos.NewPrometheus(e, "3", e2ethanos.DefaultPromConfig("prom3", 0, e2ethanos.RemoteWriteEndpoint(r1.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, e2e.StartAndWaitReady(prom1, prom2, prom3))

		q := e2ethanos.NewQuerierBuilder(e, "1", i1.InternalEndpoint("grpc"), i2.InternalEndpoint("grpc"), i3.InternalEndpoint("grpc")).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

		expectedReplicationFactor := 2.0

		queryAndAssert(t, ctx, q.Endpoint("http"), func() string { return "count(up) by (prometheus)" }, time.Now, promclient.QueryOptions{
			Deduplicate: false,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{
					"prometheus": "prom1",
				},
				Value: model.SampleValue(expectedReplicationFactor),
			},
			&model.Sample{
				Metric: model.Metric{
					"prometheus": "prom2",
				},
				Value: model.SampleValue(expectedReplicationFactor),
			},
			&model.Sample{
				Metric: model.Metric{
					"prometheus": "prom3",
				},
				Value: model.SampleValue(expectedReplicationFactor),
			},
		})
	})

	t.Run("routing_tree", func(t *testing.T) {
		/*
			The routing_tree suite configures a valid and plausible, but non-trivial topology of receiver components.
			Crucially, the first router routes to both a routing component, and a receiving component. This demonstrates
			Receiver's ability to handle arbitrary depth receiving trees.

			Router1 is configured to duplicate data twice, once to Ingestor1, and once to Router2,
			Router2 is also configured to duplicate data twice, once to Ingestor2, and once to Ingestor3.

			           ┌───────┐         ┌───────┐
			           │       │         │       │
			           │ Prom1 ├──┐   ┌──┤ Prom2 │
			           │       │  │   │  │       │
			           └───────┘  │   │  └───────┘
			                   ┌──▼───▼──┐
			                   │         │
			                   │ Router1 │
			              ┌────┤         ├───────┐
			              │    └─────────┘       │
			          ┌───▼─────┐          ┌─────▼─────┐
			          │         │          │           │
			          │ Router2 │          │ Ingestor1 │
			      ┌───┤         ├───┐      │           │
			      │   └─────────┘   │      └─────┬─────┘
			┌─────▼─────┐      ┌────▼──────┐     │
			│           │      │           │     │
			│ Ingestor2 │      │ Ingestor3 │     │
			│           │      │           │     │
			└─────┬─────┘      └─────┬─────┘     │
			      │             ┌────▼────┐      │
			      │             │         │      │
			      └─────────────►  Query  ◄──────┘
			                    │         │
			                    └─────────┘

			NB: Made with asciiflow.com - you can copy & paste the above there to modify.
		*/

		t.Parallel()
		e, err := e2e.NewDockerEnvironment("routing-tree")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, e))

		// Setup ingestors.
		i1 := e2ethanos.NewReceiveBuilder(e, "i1").WithIngestionEnabled().Init()
		i2 := e2ethanos.NewReceiveBuilder(e, "i2").WithIngestionEnabled().Init()
		i3 := e2ethanos.NewReceiveBuilder(e, "i3").WithIngestionEnabled().Init()

		// Setup distributors
		r2 := e2ethanos.NewReceiveBuilder(e, "r2").WithRouting(2, receive.HashringConfig{
			Endpoints: []string{
				i2.InternalEndpoint("grpc"),
				i3.InternalEndpoint("grpc"),
			},
		}).Init()
		r1 := e2ethanos.NewReceiveBuilder(e, "r1").WithRouting(2, receive.HashringConfig{
			Endpoints: []string{
				i1.InternalEndpoint("grpc"),
				r2.InternalEndpoint("grpc"),
			},
		}).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(i1, i2, i3, r1, r2))

		// Setup Prometheus.
		prom1 := e2ethanos.NewPrometheus(e, "1", e2ethanos.DefaultPromConfig("prom1", 0, e2ethanos.RemoteWriteEndpoint(r1.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage())
		prom2 := e2ethanos.NewPrometheus(e, "2", e2ethanos.DefaultPromConfig("prom2", 0, e2ethanos.RemoteWriteEndpoint(r1.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, e2e.StartAndWaitReady(prom1, prom2))

		//Setup Querier
		q := e2ethanos.NewQuerierBuilder(e, "1", i1.InternalEndpoint("grpc"), i2.InternalEndpoint("grpc"), i3.InternalEndpoint("grpc")).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

		expectedReplicationFactor := 3.0

		queryAndAssert(t, ctx, q.Endpoint("http"), func() string { return "count(up) by (prometheus)" }, time.Now, promclient.QueryOptions{
			Deduplicate: false,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{
					"prometheus": "prom1",
				},
				Value: model.SampleValue(expectedReplicationFactor),
			},
			&model.Sample{
				Metric: model.Metric{
					"prometheus": "prom2",
				},
				Value: model.SampleValue(expectedReplicationFactor),
			},
		})
	})

	t.Run("hashring", func(t *testing.T) {
		/*
			The hashring suite creates three receivers, each with a Prometheus
			remote-writing data to it. However, due to the hashing of the labels,
			the time series from the Prometheus is forwarded to a different
			receiver in the hashring than the one handling the request.
			The querier queries all the receivers and the test verifies
			the time series are forwarded to the correct receive node.

			                      ┌───────┐
			                      │       │
			                      │ Prom2 │
			                      │       │
			                      └───┬───┘
			                          │
			                          │
			    ┌────────┐      ┌─────▼─────┐     ┌───────┐
			    │        │      │           │     │       │
			    │ Prom1  │      │ Router    │     │ Prom3 │
			    │        │      │ Ingestor2 │     │       │
			    └───┬────┘      │           │     └───┬───┘
			        │           └──▲──┬──▲──┘         │
			        │              │  │  │            │
			   ┌────▼──────┐       │  │  │       ┌────▼──────┐
			   │           ◄───────┘  │  └───────►           │
			   │ Router    │          │          │ Router    │
			   │ Ingestor1 ◄──────────┼──────────► Ingestor3 │
			   │           │          │          │           │
			   └─────┬─────┘          │          └────┬──────┘
			         │                │               │
			         │            ┌───▼───┐           │
			         │            │       │           │
			         └────────────► Query ◄───────────┘
			                      │       │
			                      └───────┘
		*/
		t.Parallel()

		e, err := e2e.NewDockerEnvironment("hashring")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, e))

		r1 := e2ethanos.NewReceiveBuilder(e, "1").WithIngestionEnabled()
		r2 := e2ethanos.NewReceiveBuilder(e, "2").WithIngestionEnabled()
		r3 := e2ethanos.NewReceiveBuilder(e, "3").WithIngestionEnabled()

		h := receive.HashringConfig{
			Endpoints: []string{
				r1.InternalEndpoint("grpc"),
				r2.InternalEndpoint("grpc"),
				r3.InternalEndpoint("grpc"),
			},
		}

		// Create with hashring config watcher.
		r1Runnable := r1.WithRouting(1, h).Init()
		r2Runnable := r2.WithRouting(1, h).Init()
		r3Runnable := r3.WithRouting(1, h).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(r1Runnable, r2Runnable, r3Runnable))

		prom1 := e2ethanos.NewPrometheus(e, "1", e2ethanos.DefaultPromConfig("prom1", 0, e2ethanos.RemoteWriteEndpoint(r1.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage())
		prom2 := e2ethanos.NewPrometheus(e, "2", e2ethanos.DefaultPromConfig("prom2", 0, e2ethanos.RemoteWriteEndpoint(r2.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage())
		prom3 := e2ethanos.NewPrometheus(e, "3", e2ethanos.DefaultPromConfig("prom3", 0, e2ethanos.RemoteWriteEndpoint(r3.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, e2e.StartAndWaitReady(prom1, prom2, prom3))

		q := e2ethanos.NewQuerierBuilder(e, "1", r1.InternalEndpoint("grpc"), r2.InternalEndpoint("grpc"), r3.InternalEndpoint("grpc")).Init()
		testutil.Ok(t, err)
		testutil.Ok(t, e2e.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

		queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "receive-2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom2",
				"receive":    "receive-1",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom3",
				"receive":    "receive-2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
		})
	})

	t.Run("replication", func(t *testing.T) {
		t.Parallel()

		e, err := e2e.NewDockerEnvironment("replication")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, e))

		// The replication suite creates three receivers but only one
		// receives Prometheus remote-written data. The querier queries all
		// receivers and the test verifies that the time series are
		// replicated to all of the nodes.

		r1 := e2ethanos.NewReceiveBuilder(e, "1").WithIngestionEnabled()
		r2 := e2ethanos.NewReceiveBuilder(e, "2").WithIngestionEnabled()
		r3 := e2ethanos.NewReceiveBuilder(e, "3").WithIngestionEnabled()

		h := receive.HashringConfig{
			Endpoints: []string{
				r1.InternalEndpoint("grpc"),
				r2.InternalEndpoint("grpc"),
				r3.InternalEndpoint("grpc"),
			},
		}

		// Create with hashring config.
		r1Runnable := r1.WithRouting(3, h).Init()
		r2Runnable := r2.WithRouting(3, h).Init()
		r3Runnable := r3.WithRouting(3, h).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(r1Runnable, r2Runnable, r3Runnable))

		prom1 := e2ethanos.NewPrometheus(e, "1", e2ethanos.DefaultPromConfig("prom1", 0, e2ethanos.RemoteWriteEndpoint(r1.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, e2e.StartAndWaitReady(prom1))

		q := e2ethanos.NewQuerierBuilder(e, "1", r1.InternalEndpoint("grpc"), r2.InternalEndpoint("grpc"), r3.InternalEndpoint("grpc")).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

		queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "receive-1",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "receive-2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "receive-3",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
		})
	})

	t.Run("replication_with_outage", func(t *testing.T) {
		t.Parallel()

		e, err := e2e.NewDockerEnvironment("outage")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, e))

		// The replication suite creates a three-node hashring but one of the
		// receivers is dead. In this case, replication should still
		// succeed and the time series should be replicated to the other nodes.

		r1 := e2ethanos.NewReceiveBuilder(e, "1").WithIngestionEnabled()
		r2 := e2ethanos.NewReceiveBuilder(e, "2").WithIngestionEnabled()
		r3 := e2ethanos.NewReceiveBuilder(e, "3").WithIngestionEnabled()

		h := receive.HashringConfig{
			Endpoints: []string{
				r1.InternalEndpoint("grpc"),
				r2.InternalEndpoint("grpc"),
				r3.InternalEndpoint("grpc"),
			},
		}

		// Create with hashring config.
		r1Runnable := r1.WithRouting(3, h).Init()
		r2Runnable := r2.WithRouting(3, h).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(r1Runnable, r2Runnable))

		prom1 := e2ethanos.NewPrometheus(e, "1", e2ethanos.DefaultPromConfig("prom1", 0, e2ethanos.RemoteWriteEndpoint(r1.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, e2e.StartAndWaitReady(prom1))

		q := e2ethanos.NewQuerierBuilder(e, "1", r1.InternalEndpoint("grpc"), r2.InternalEndpoint("grpc")).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(2), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

		queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "receive-1",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "receive-2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
		})
	})

	t.Run("multitenancy", func(t *testing.T) {
		t.Parallel()

		e, err := e2e.NewDockerEnvironment("multitenancy")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, e))

		r1 := e2ethanos.NewReceiveBuilder(e, "1").WithIngestionEnabled()

		h := receive.HashringConfig{
			Endpoints: []string{
				r1.InternalEndpoint("grpc"),
			},
		}

		// Create with hashring config.
		r1Runnable := r1.WithRouting(1, h).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(r1Runnable))

		rp1 := e2ethanos.NewReverseProxy(e, "1", "tenant-1", "http://"+r1.InternalEndpoint("remote-write"))
		rp2 := e2ethanos.NewReverseProxy(e, "2", "tenant-2", "http://"+r1.InternalEndpoint("remote-write"))
		testutil.Ok(t, e2e.StartAndWaitReady(rp1, rp2))

		prom1 := e2ethanos.NewPrometheus(e, "1", e2ethanos.DefaultPromConfig("prom1", 0, "http://"+rp1.InternalEndpoint("http")+"/api/v1/receive", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage())
		prom2 := e2ethanos.NewPrometheus(e, "2", e2ethanos.DefaultPromConfig("prom2", 0, "http://"+rp2.InternalEndpoint("http")+"/api/v1/receive", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, e2e.StartAndWaitReady(prom1, prom2))

		q := e2ethanos.NewQuerierBuilder(e, "1", r1.InternalEndpoint("grpc")).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(q))
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "receive-1",
				"replica":    "0",
				"tenant_id":  "tenant-1",
			},
			{
				"job":        "myself",
				"prometheus": "prom2",
				"receive":    "receive-1",
				"replica":    "0",
				"tenant_id":  "tenant-2",
			},
		})
	})

	t.Run("relabel", func(t *testing.T) {
		t.Parallel()
		e, err := e2e.NewDockerEnvironment("receive-relabel")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, e))

		// Setup Router Ingestor.
		i := e2ethanos.NewReceiveBuilder(e, "ingestor").
			WithIngestionEnabled().
			WithRelabelConfigs([]*relabel.Config{
				{
					Action: relabel.LabelDrop,
					Regex:  relabel.MustNewRegexp("prometheus"),
				},
			}).Init()

		testutil.Ok(t, e2e.StartAndWaitReady(i))

		// Setup Prometheus
		prom := e2ethanos.NewPrometheus(e, "1", e2ethanos.DefaultPromConfig("prom1", 0, e2ethanos.RemoteWriteEndpoint(i.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, e2e.StartAndWaitReady(prom))

		q := e2ethanos.NewQuerierBuilder(e, "1", i.InternalEndpoint("grpc")).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))
		// Label `prometheus` should be dropped.
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":       "myself",
				"receive":   "receive-ingestor",
				"replica":   "0",
				"tenant_id": "default-tenant",
			},
		})
	})

	t.Run("multitenant_active_series_limiting", func(t *testing.T) {

		/*
			The multitenant_active_series_limiting suite configures a hashring with
			two avalanche writers and dedicated meta-monitoring.

			┌──────────┐                           ┌──────────┐
			│          │                           │          │
			│Avalanche │                           │Avalanche │
			│          │                           │          │
			│          │                           │          │
			└──────────┴──────────┐     ┌──────────┴──────────┘
			                      │     │
			                    ┌─▼─────▼──┐
			                    │          │
			                    │Router    ├────────────────► Meta-monitoring
			                    │Ingestor  │
			                    │          │
			                    └──▲─┬──▲──┘
			                       │ │  │
			    ┌──────────┐       │ │  │        ┌──────────┐
			    │          │       │ │  │        │          │
			    │Router    ◄───────┘ │  └────────►Router    │
			    │Ingestor  │         │           │Ingestor  │
			    │          ◄─────────┼───────────►          │
			    └────┬─────┘         │           └────┬─────┘
			         │               │                │
			         │          ┌────▼─────┐          │
			         │          │          │          │
			         └──────────► Query    ◄──────────┘
			                    │          │
			                    │          │
			                    └──────────┘

			NB: Made with asciiflow.com - you can copy & paste the above there to modify.
		*/

		t.Parallel()
		e, err := e2e.NewDockerEnvironment("active-series")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, e))

		// This can be treated as the meta-monitoring service.
		meta, err := e2emon.Start(e)
		testutil.Ok(t, err)

		// Setup 3 RouterIngestors with a limit of 10 active series.
		ingestor1 := e2ethanos.NewReceiveBuilder(e, "i1").WithIngestionEnabled()
		ingestor2 := e2ethanos.NewReceiveBuilder(e, "i2").WithIngestionEnabled()
		ingestor3 := e2ethanos.NewReceiveBuilder(e, "i3").WithIngestionEnabled()

		h := receive.HashringConfig{
			Endpoints: []string{
				ingestor1.InternalEndpoint("grpc"),
				ingestor2.InternalEndpoint("grpc"),
				ingestor3.InternalEndpoint("grpc"),
			},
		}

		i1Runnable := ingestor1.WithRouting(1, h).WithValidationEnabled(10, "http://"+meta.GetMonitoringRunnable().InternalEndpoint(e2edb.AccessPortName)).Init()
		i2Runnable := ingestor2.WithRouting(1, h).WithValidationEnabled(10, "http://"+meta.GetMonitoringRunnable().InternalEndpoint(e2edb.AccessPortName)).Init()
		i3Runnable := ingestor3.WithRouting(1, h).WithValidationEnabled(10, "http://"+meta.GetMonitoringRunnable().InternalEndpoint(e2edb.AccessPortName)).Init()

		testutil.Ok(t, e2e.StartAndWaitReady(i1Runnable, i2Runnable, i3Runnable))

		querier := e2ethanos.NewQuerierBuilder(e, "1", ingestor1.InternalEndpoint("grpc"), ingestor2.InternalEndpoint("grpc"), ingestor3.InternalEndpoint("grpc")).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(querier))

		testutil.Ok(t, querier.WaitSumMetricsWithOptions(e2emon.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

		// We run two avalanches, one tenant which exceeds the limit, and one tenant which remains under it.

		// Avalanche in this configuration, would send 5 requests each with 10 new timeseries.
		// One request always fails due to TSDB not being ready for new tenant.
		// So without limiting we end up with 40 timeseries and 40 samples.
		avalanche1 := e2ethanos.NewAvalanche(e, "avalanche-1",
			e2ethanos.AvalancheOptions{
				MetricCount:    "10",
				SeriesCount:    "1",
				MetricInterval: "30",
				SeriesInterval: "3600",
				ValueInterval:  "3600",

				RemoteURL:           e2ethanos.RemoteWriteEndpoint(ingestor1.InternalEndpoint("remote-write")),
				RemoteWriteInterval: "30s",
				RemoteBatchSize:     "10",
				RemoteRequestCount:  "5",

				TenantID: "exceed-tenant",
			})

		// Avalanche in this configuration, would send 5 requests each with 5 of the same timeseries.
		// One request always fails due to TSDB not being ready for new tenant.
		// So we end up with 5 timeseries, 20 samples.
		avalanche2 := e2ethanos.NewAvalanche(e, "avalanche-2",
			e2ethanos.AvalancheOptions{
				MetricCount:    "5",
				SeriesCount:    "1",
				MetricInterval: "3600",
				SeriesInterval: "3600",
				ValueInterval:  "3600",

				RemoteURL:           e2ethanos.RemoteWriteEndpoint(ingestor1.InternalEndpoint("remote-write")),
				RemoteWriteInterval: "30s",
				RemoteBatchSize:     "5",
				RemoteRequestCount:  "5",

				TenantID: "under-tenant",
			})

		testutil.Ok(t, e2e.StartAndWaitReady(avalanche1, avalanche2))

		// Here, 3/5 requests are failed due to limiting, as one request fails due to TSDB readiness and we ingest one initial request.
		// 3 limited requests belong to the exceed-tenant.
		testutil.Ok(t, i1Runnable.WaitSumMetricsWithOptions(e2emon.Equals(3), []string{"thanos_receive_head_series_limited_requests_total"}, e2emon.WithWaitBackoff(&backoff.Config{Min: 1 * time.Second, Max: 10 * time.Minute, MaxRetries: 200}), e2emon.WaitMissingMetrics()))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		ingestor1Name := e.Name() + "-" + ingestor1.Name()
		// Here for exceed-tenant we go above limit by 10, which results in 0 value.
		queryWaitAndAssert(t, ctx, meta.GetMonitoringRunnable().Endpoint(e2edb.AccessPortName), func() string {
			return fmt.Sprintf("sum(prometheus_tsdb_head_series{tenant=\"exceed-tenant\"}) - on() thanos_receive_head_series_limit{instance=\"%s:8080\", job=\"receive-i1\"}", ingestor1Name)
		}, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{},
				Value:  model.SampleValue(0),
			},
		})

		// For under-tenant we stay at -5, as we have only pushed 5 series.
		queryWaitAndAssert(t, ctx, meta.GetMonitoringRunnable().Endpoint(e2edb.AccessPortName), func() string {
			return fmt.Sprintf("sum(prometheus_tsdb_head_series{tenant=\"under-tenant\"}) - on() thanos_receive_head_series_limit{instance=\"%s:8080\", job=\"receive-i1\"}", ingestor1Name)
		}, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{},
				Value:  model.SampleValue(-5),
			},
		})

		// Query meta-monitoring solution to assert that only 10 timeseries have been ingested for exceed-tenant.
		queryWaitAndAssert(t, ctx, meta.GetMonitoringRunnable().Endpoint(e2edb.AccessPortName), func() string { return "sum(prometheus_tsdb_head_series{tenant=\"exceed-tenant\"})" }, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{},
				Value:  model.SampleValue(10),
			},
		})

		// Query meta-monitoring solution to assert that only 5 timeseries have been ingested for under-tenant.
		queryWaitAndAssert(t, ctx, meta.GetMonitoringRunnable().Endpoint(e2edb.AccessPortName), func() string { return "sum(prometheus_tsdb_head_series{tenant=\"under-tenant\"})" }, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{},
				Value:  model.SampleValue(5),
			},
		})

		// Query meta-monitoring solution to assert that 3 requests were limited for exceed-tenant and none for under-tenant.
		queryWaitAndAssert(t, ctx, meta.GetMonitoringRunnable().Endpoint(e2edb.AccessPortName), func() string { return "thanos_receive_head_series_limited_requests_total" }, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{
					"__name__": "thanos_receive_head_series_limited_requests_total",
					"instance": model.LabelValue(fmt.Sprintf("%s:8080", ingestor1Name)),
					"job":      "receive-i1",
					"tenant":   "exceed-tenant",
				},
				Value: model.SampleValue(3),
			},
		})
	})
}
