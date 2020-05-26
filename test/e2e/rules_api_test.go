// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestRulesAPI_Fanout(t *testing.T) {
	t.Skip("Fix in next PR")
	t.Parallel()

	netName := "e2e_test_rules_fanout"

	s, err := e2e.NewScenario(netName)
	testutil.Ok(t, err)
	defer s.Close()

	rulesSubDir := filepath.Join("rules")
	testutil.Ok(t, os.MkdirAll(filepath.Join(s.SharedDir(), rulesSubDir), os.ModePerm))
	createRuleFiles(t, filepath.Join(s.SharedDir(), rulesSubDir))

	// 2x Prometheus.
	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom1",
		defaultPromConfig("prom1", 0, "", filepath.Join(e2e.ContainerSharedDir, rulesSubDir, "*.yaml")),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom2",
		defaultPromConfig("prom2", 1, "", filepath.Join(e2e.ContainerSharedDir, rulesSubDir, "*.yaml")),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	// 2x Rulers.
	r1, err := e2ethanos.NewRuler(s.SharedDir(), "rule1", rulesSubDir, nil, nil)
	testutil.Ok(t, err)
	r2, err := e2ethanos.NewRuler(s.SharedDir(), "rule2", rulesSubDir, nil, nil)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(r1, r2))

	q, err := e2ethanos.NewQuerier(s.SharedDir(), "query",
		[]string{sidecar1.GRPCNetworkEndpoint(), sidecar2.GRPCNetworkEndpoint(), r1.GRPCNetworkEndpoint(), r2.GRPCNetworkEndpoint()},
		nil,
		[]string{sidecar1.GRPCNetworkEndpoint(), sidecar2.GRPCNetworkEndpoint(), r1.GRPCNetworkEndpoint(), r2.GRPCNetworkEndpoint()},
	)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	testutil.Ok(t, q.WaitSumMetrics(e2e.Equals(4), "thanos_store_nodes_grpc_connections"))

	// TODO(bwplotka): Let's not be lazy and expect EXACT rules and alerts for all request types.
	// TODO(bwplotka): Test dedup true and false.

	// For now expects two, as we should deduplicate both rulers and prometheus.
	ruleAndAssert(t, ctx, q.HTTPEndpoint(), "", 2)
}

func ruleAndAssert(t *testing.T, ctx context.Context, addr string, typ string, expectedLen int) {
	t.Helper()

	fmt.Println("ruleAndAssert: Waiting for", expectedLen, "results for rules type", typ)
	var result []*rulespb.RuleGroup
	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		res, err := promclient.NewDefaultClient().RulesInGRPC(ctx, urlParse(t, "http://"+addr), typ)
		if err != nil {
			return err
		}

		if len(result) != len(res) {
			fmt.Println("ruleAndAssert: New result:", res)
		}

		if len(res) != expectedLen {
			return errors.Errorf("unexpected result size, expected %d; got: %d result: %v", expectedLen, len(res), res)
		}
		result = res
		return nil
	}))
}
