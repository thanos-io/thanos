// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestTargetsAPI_Fanout(t *testing.T) {
	t.Skip("TODO: Flaky test. See: https://github.com/thanos-io/thanos/issues/4069")

	t.Parallel()

	netName := "e2e_test_targets_fanout"

	s, err := e2e.NewScenario(netName)
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	// 2x Prometheus.
	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom1",
		defaultPromConfig("ha", 0, "", "", "localhost:9090", "localhost:80"),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom2",
		defaultPromConfig("ha", 1, "", "", "localhost:9090", "localhost:80"),
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
		[]string{sidecar1.GRPCNetworkEndpoint(), sidecar2.GRPCNetworkEndpoint()},
		nil,
		nil,
		"",
		"",
	)

	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

	targetAndAssert(t, ctx, q.HTTPEndpoint(), "", &targetspb.TargetDiscovery{
		ActiveTargets: []*targetspb.ActiveTarget{
			{
				DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "__address__", Value: "localhost:9090"},
					{Name: "__metrics_path__", Value: "/metrics"},
					{Name: "__scheme__", Value: "http"},
					{Name: "job", Value: "myself"},
					{Name: "prometheus", Value: "ha"},
				}},
				Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "instance", Value: "localhost:9090"},
					{Name: "job", Value: "myself"},
					{Name: "prometheus", Value: "ha"},
				}},
				ScrapePool: "myself",
				ScrapeUrl:  "http://localhost:9090/metrics",
				Health:     targetspb.TargetHealth_UP,
			},
		},
		DroppedTargets: []*targetspb.DroppedTarget{
			{
				DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "__address__", Value: "localhost:80"},
					{Name: "__metrics_path__", Value: "/metrics"},
					{Name: "__scheme__", Value: "http"},
					{Name: "job", Value: "myself"},
					{Name: "prometheus", Value: "ha"},
				}},
			},
		},
	})
}
