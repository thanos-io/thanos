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

	stores := []string{sidecar1.GRPCNetworkEndpoint(), sidecar2.GRPCNetworkEndpoint()}
	q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "query", stores).
		WithTargetAddresses(stores).
		Build()
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

//nolint:unused
func targetAndAssert(t *testing.T, ctx context.Context, addr, state string, want *targetspb.TargetDiscovery) {
	t.Helper()

	fmt.Println("targetAndAssert: Waiting for results for targets state", state)

	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		res, err := promclient.NewDefaultClient().TargetsInGRPC(ctx, mustURLParse(t, "http://"+addr), state)
		if err != nil {
			return err
		}

		if len(res.ActiveTargets) != len(want.ActiveTargets) {
			return errors.Errorf("unexpected result.ActiveTargets size, want %d; got: %d result: %v", len(want.ActiveTargets), len(res.ActiveTargets), res)
		}

		if len(res.DroppedTargets) != len(want.DroppedTargets) {
			return errors.Errorf("unexpected result.DroppedTargets size, want %d; got: %d result: %v", len(want.DroppedTargets), len(res.DroppedTargets), res)
		}

		for it := range res.ActiveTargets {
			res.ActiveTargets[it].LastScrape = time.Time{}
			res.ActiveTargets[it].LastScrapeDuration = 0
			res.ActiveTargets[it].GlobalUrl = ""
		}

		sort.Slice(res.ActiveTargets, func(i, j int) bool { return res.ActiveTargets[i].Compare(res.ActiveTargets[j]) < 0 })
		sort.Slice(res.DroppedTargets, func(i, j int) bool { return res.DroppedTargets[i].Compare(res.DroppedTargets[j]) < 0 })

		if !reflect.DeepEqual(want, res) {
			return errors.Errorf("unexpected result\nwant %v\ngot: %v", want, res)
		}

		return nil
	}))
}
