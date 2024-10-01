// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/go-kit/log"
	"github.com/pkg/errors"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestTargetsAPI_Fanout(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("targets-fanout")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	// 2x Prometheus.
	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(
		e,
		"prom1",
		e2ethanos.DefaultPromConfig("ha", 0, "", "", e2ethanos.LocalPrometheusTarget, "localhost:80"),
		"",
		e2ethanos.DefaultPrometheusImage(), "",
	)
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(
		e,
		"prom2",
		e2ethanos.DefaultPromConfig("ha", 1, "", "", e2ethanos.LocalPrometheusTarget, "localhost:80"),
		"",
		e2ethanos.DefaultPrometheusImage(), "",
	)
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	stores := []string{sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc")}
	q := e2ethanos.NewQuerierBuilder(e, "query", stores...).
		WithTargetAddresses(stores...).
		Init()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(2), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

	targetAndAssert(t, ctx, q.Endpoint("http"), "", &targetspb.TargetDiscovery{
		ActiveTargets: []*targetspb.ActiveTarget{
			{
				DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "__address__", Value: "localhost:9090"},
					{Name: "__metrics_path__", Value: "/metrics"},
					{Name: "__scheme__", Value: "http"},
					{Name: "__scrape_interval__", Value: "1s"},
					{Name: "__scrape_timeout__", Value: "1s"},
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
					{Name: "__scrape_interval__", Value: "1s"},
					{Name: "__scrape_timeout__", Value: "1s"},
					{Name: "job", Value: "myself"},
					{Name: "prometheus", Value: "ha"},
				}},
			},
		},
	})
}

func targetAndAssert(t *testing.T, ctx context.Context, addr, state string, want *targetspb.TargetDiscovery) {
	t.Helper()

	fmt.Println("targetAndAssert: Waiting for results for targets state", state)

	logger := log.NewLogfmtLogger(os.Stdout)
	testutil.Ok(t, runutil.RetryWithLog(logger, time.Second, ctx.Done(), func() error {
		res, err := promclient.NewDefaultClient().TargetsInGRPC(ctx, urlParse(t, "http://"+addr), state)
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
