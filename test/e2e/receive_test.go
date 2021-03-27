// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

type ReverseProxyConfig struct {
	tenantId string
	port     string
	target   string
}

type DebugTransport struct{}

func (DebugTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	_, err := httputil.DumpRequestOut(r, false)
	if err != nil {
		return nil, err
	}
	return http.DefaultTransport.RoundTrip(r)
}

func generateProxy(conf ReverseProxyConfig) {
	targetURL, _ := url.Parse(conf.target)
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	d := proxy.Director
	proxy.Director = func(r *http.Request) {
		d(r) // call default director
		r.Header.Add("THANOS-TENANT", conf.tenantId)
	}
	proxy.ErrorHandler = ErrorHandler
	proxy.Transport = DebugTransport{}
	log.Fatal(http.ListenAndServe(conf.port, proxy))
}

func ErrorHandler(_ http.ResponseWriter, _ *http.Request, err error) {
	log.Print("Response from receiver")
	log.Print(err)
}

func TestReceive(t *testing.T) {
	t.Parallel()

	t.Run("hashring", func(t *testing.T) {
		t.Parallel()

		s, err := e2e.NewScenario("e2e_test_receive_hashring")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, s))

		// The hashring suite creates three receivers, each with a Prometheus
		// remote-writing data to it. However, due to the hashing of the labels,
		// the time series from the Prometheus is forwarded to a different
		// receiver in the hashring than the one handling the request.
		// The querier queries all the receivers and the test verifies
		// the time series are forwarded to the correct receive node.
		r1, err := e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "1", 1)
		testutil.Ok(t, err)
		r2, err := e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "2", 1)
		testutil.Ok(t, err)
		r3, err := e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "3", 1)
		testutil.Ok(t, err)

		h := receive.HashringConfig{
			Endpoints: []string{
				r1.GRPCNetworkEndpointFor(s.NetworkName()),
				r2.GRPCNetworkEndpointFor(s.NetworkName()),
				r3.GRPCNetworkEndpointFor(s.NetworkName()),
			},
		}

		// Recreate again, but with hashring config.
		r1, err = e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "1", 1, h)
		testutil.Ok(t, err)
		r2, err = e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "2", 1, h)
		testutil.Ok(t, err)
		r3, err = e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "3", 1, h)
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(r1, r2, r3))

		prom1, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "1", defaultPromConfig("prom1", 0, e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081)), ""), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		prom2, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "2", defaultPromConfig("prom2", 0, e2ethanos.RemoteWriteEndpoint(r2.NetworkEndpoint(8081)), ""), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		prom3, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "3", defaultPromConfig("prom3", 0, e2ethanos.RemoteWriteEndpoint(r3.NetworkEndpoint(8081)), ""), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(prom1, prom2, prom3))

		q, err := e2ethanos.NewQuerier(s.SharedDir(), "1", []string{r1.GRPCNetworkEndpoint(), r2.GRPCNetworkEndpoint(), r3.GRPCNetworkEndpoint()}, nil, nil, nil, nil, "", "")
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

		queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom2",
				"receive":    "1",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom3",
				"receive":    "2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
		})
	})

	t.Run("hashring with config watcher", func(t *testing.T) {
		t.Parallel()

		s, err := e2e.NewScenario("e2e_test_receive_hashring")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, s))

		r1, err := e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "1", 1)
		testutil.Ok(t, err)
		r2, err := e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "2", 1)
		testutil.Ok(t, err)
		r3, err := e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "3", 1)
		testutil.Ok(t, err)

		h := receive.HashringConfig{
			Endpoints: []string{
				r1.GRPCNetworkEndpointFor(s.NetworkName()),
				r2.GRPCNetworkEndpointFor(s.NetworkName()),
				r3.GRPCNetworkEndpointFor(s.NetworkName()),
			},
		}

		// Recreate again, but with hashring config.
		// TODO(kakkoyun): Update config file and wait config watcher to reconcile hashring.
		r1, err = e2ethanos.NewReceiverWithConfigWatcher(s.SharedDir(), s.NetworkName(), "1", 1, h)
		testutil.Ok(t, err)
		r2, err = e2ethanos.NewReceiverWithConfigWatcher(s.SharedDir(), s.NetworkName(), "2", 1, h)
		testutil.Ok(t, err)
		r3, err = e2ethanos.NewReceiverWithConfigWatcher(s.SharedDir(), s.NetworkName(), "3", 1, h)
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(r1, r2, r3))

		prom1, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "1", defaultPromConfig("prom1", 0, e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081)), ""), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		prom2, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "2", defaultPromConfig("prom2", 0, e2ethanos.RemoteWriteEndpoint(r2.NetworkEndpoint(8081)), ""), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		prom3, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "3", defaultPromConfig("prom3", 0, e2ethanos.RemoteWriteEndpoint(r3.NetworkEndpoint(8081)), ""), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(prom1, prom2, prom3))

		q, err := e2ethanos.NewQuerier(s.SharedDir(), "1", []string{r1.GRPCNetworkEndpoint(), r2.GRPCNetworkEndpoint(), r3.GRPCNetworkEndpoint()}, nil, nil, nil, nil, "", "")
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

		queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom2",
				"receive":    "1",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom3",
				"receive":    "2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
		})
	})

	t.Run("replication", func(t *testing.T) {
		t.Parallel()

		s, err := e2e.NewScenario("e2e_test_receive_replication")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, s))

		// The replication suite creates three receivers but only one
		// receives Prometheus remote-written data. The querier queries all
		// receivers and the test verifies that the time series are
		// replicated to all of the nodes.
		r1, err := e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "1", 3)
		testutil.Ok(t, err)
		r2, err := e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "2", 3)
		testutil.Ok(t, err)
		r3, err := e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "3", 3)
		testutil.Ok(t, err)

		h := receive.HashringConfig{
			Endpoints: []string{
				r1.GRPCNetworkEndpointFor(s.NetworkName()),
				r2.GRPCNetworkEndpointFor(s.NetworkName()),
				r3.GRPCNetworkEndpointFor(s.NetworkName()),
			},
		}

		// Recreate again, but with hashring config.
		r1, err = e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "1", 3, h)
		testutil.Ok(t, err)
		r2, err = e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "2", 3, h)
		testutil.Ok(t, err)
		r3, err = e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "3", 3, h)
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(r1, r2, r3))

		prom1, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "1", defaultPromConfig("prom1", 0, e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081)), ""), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(prom1))

		q, err := e2ethanos.NewQuerier(s.SharedDir(), "1", []string{r1.GRPCNetworkEndpoint(), r2.GRPCNetworkEndpoint(), r3.GRPCNetworkEndpoint()}, nil, nil, nil, nil, "", "")
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

		queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "1",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "3",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
		})
	})

	t.Run("replication_with_outage", func(t *testing.T) {
		t.Parallel()

		s, err := e2e.NewScenario("e2e_test_receive_replication_with_outage")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, s))

		// The replication suite creates a three-node hashring but one of the
		// receivers is dead. In this case, replication should still
		// succeed and the time series should be replicated to the other nodes.
		r1, err := e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "1", 3)
		testutil.Ok(t, err)
		r2, err := e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "2", 3)
		testutil.Ok(t, err)
		notRunningR3, err := e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "3", 3)
		testutil.Ok(t, err)

		h := receive.HashringConfig{
			Endpoints: []string{
				r1.GRPCNetworkEndpointFor(s.NetworkName()),
				r2.GRPCNetworkEndpointFor(s.NetworkName()),
				notRunningR3.GRPCNetworkEndpointFor(s.NetworkName()),
			},
		}

		// Recreate again, but with hashring config.
		r1, err = e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "1", 3, h)
		testutil.Ok(t, err)
		r2, err = e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "2", 3, h)
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(r1, r2))

		prom1, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "1", defaultPromConfig("prom1", 0, e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081)), ""), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(prom1))

		q, err := e2ethanos.NewQuerier(s.SharedDir(), "1", []string{r1.GRPCNetworkEndpoint(), r2.GRPCNetworkEndpoint()}, nil, nil, nil, nil, "", "")
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

		queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "1",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
		})
	})

	t.Run("multitenancy", func(t *testing.T) {
		t.Parallel()

		s, err := e2e.NewScenario("e2e_test_for_multitenancy")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, s))

		// The replication suite creates a three-node hashring but one of the
		// receivers is dead. In this case, replication should still
		// succeed and the time series should be replicated to the other nodes.
		r1, err := e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "1", 1)
		testutil.Ok(t, err)

		h := receive.HashringConfig{
			Endpoints: []string{
				r1.GRPCNetworkEndpointFor(s.NetworkName()),
			},
		}

		// Recreate again, but with hashring config.
		r1, err = e2ethanos.NewReceiver(s.SharedDir(), s.NetworkName(), "1", 1, h)
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(r1))
		testutil.Ok(t, err)

		conf1 := ReverseProxyConfig{
			tenantId: "tenant-1",
			port:     ":9097",
			target:   "http://" + r1.Endpoint(8081),
		}
		conf2 := ReverseProxyConfig{
			tenantId: "tenant-2",
			port:     ":9098",
			target:   "http://" + r1.Endpoint(8081),
		}

		go generateProxy(conf1)
		go generateProxy(conf2)

		prom1, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "1", defaultPromConfig("prom1", 0, "http://172.17.0.1:9097/api/v1/receive", ""), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		prom2, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "2", defaultPromConfig("prom1", 0, "http://172.17.0.1:9098/api/v1/receive", ""), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(prom1))
		testutil.Ok(t, s.StartAndWaitReady(prom2))

		q, err := e2ethanos.NewQuerier(s.SharedDir(), "1", []string{r1.GRPCNetworkEndpoint()}, nil, nil, nil, nil, "", "")
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(q))
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))
		queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "1",
				"replica":    "0",
				"tenant_id":  "tenant-1",
			},
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "1",
				"replica":    "0",
				"tenant_id":  "tenant-2",
			},
		})
	})
}
