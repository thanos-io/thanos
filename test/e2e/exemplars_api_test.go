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
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestExemplarsAPI_Fanout(t *testing.T) {
	t.Parallel()

	netName := "e2e_test_exemplars_fanout"

	s, err := e2e.NewScenario(netName)
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	// exemplarsSubDir := filepath.Join("exemplars")

	// 2x Prometheus.
	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom1",
		defaultPromConfig("ha", 0, "", ""),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom2",
		defaultPromConfig("ha", 1, "", ""),
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
		"",
		"",
	)

	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

	exemplarAndAssert(t, ctx, q.HTTPEndpoint(), "", []*exemplarspb.ExemplarData{
		{
			SeriesLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
				{Name: "__name__", Value: "test_exemplar_metric_total"},
				{Name: "instance", Value: "localhost:9090"},
				{Name: "job", Value: "job-x"},
				{Name: "service", Value: "service-x"},
			}},
			Exemplars: []*exemplarspb.Exemplar{
				{
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "traceID", Value: "trace-x"},
					}},
					Value: 6,
				},
			},
		},
	})
}

func exemplarAndAssert(t *testing.T, ctx context.Context, addr, state string, want []*exemplarspb.ExemplarData) {
	t.Helper()

	fmt.Println("exemplarAndAssert: Waiting for results for exemplars state", state)

	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		res, err := promclient.NewDefaultClient().ExemplarsInGRPC(ctx, mustURLParse(t, "http://"+addr), state)
		if err != nil {
			return err
		}

		if len(res) != len(want) {
			return errors.Errorf("unexpected result size, want %d; got: %d result: %v", len(want), len(res), res)
		}

		for i, ed := range res {
			res[i].PartialResponseStrategy = 0

			sort.Slice(ed.Exemplars, func(i, j int) bool { return ed.Exemplars[i].Compare(ed.Exemplars[j]) < 0 })

			for ie := range ed.Exemplars {
				res[i].Exemplars[ie].Ts = time.Time{}
			}
		}

		if !reflect.DeepEqual(want, res) {
			return errors.Errorf("unexpected result\nwant %v\ngot: %v", want, res)
		}

		return nil
	}))
}
