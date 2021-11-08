// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/httpconfig"

	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestRulesAPI_Fanout(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_rules_fanout")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	promRulesSubDir := filepath.Join("rules")
	testutil.Ok(t, os.MkdirAll(filepath.Join(e.SharedDir(), promRulesSubDir), os.ModePerm))
	// Create the abort_on_partial_response alert for Prometheus.
	// We don't create the warn_on_partial_response alert as Prometheus has strict yaml unmarshalling.
	createRuleFile(t, filepath.Join(e.SharedDir(), promRulesSubDir, "rules.yaml"), testAlertRuleAbortOnPartialResponse)

	thanosRulesSubDir := filepath.Join("thanos-rules")
	testutil.Ok(t, os.MkdirAll(filepath.Join(e.SharedDir(), thanosRulesSubDir), os.ModePerm))
	createRuleFiles(t, filepath.Join(e.SharedDir(), thanosRulesSubDir))

	// 2x Prometheus.
	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(
		e,
		"prom1",
		defaultPromConfig("ha", 0, "", filepath.Join(e2ethanos.ContainerSharedDir, promRulesSubDir, "*.yaml")),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(
		e,
		"prom2",
		defaultPromConfig("ha", 1, "", filepath.Join(e2ethanos.ContainerSharedDir, promRulesSubDir, "*.yaml")),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	qBuilder := e2ethanos.NewQuerierBuilder(e, "query")
	qUninit := qBuilder.BuildUninitiated()

	queryCfg := []httpconfig.Config{
		{
			EndpointsConfig: httpconfig.EndpointsConfig{
				StaticAddresses: []string{qUninit.InternalEndpoint("http")},
				Scheme:          "http",
			},
		},
	}

	// Recreate rulers with the corresponding query config.
	r1, err := e2ethanos.NewTSDBRuler(e, "rule1", thanosRulesSubDir, nil, queryCfg)
	testutil.Ok(t, err)
	r2, err := e2ethanos.NewTSDBRuler(e, "rule2", thanosRulesSubDir, nil, queryCfg)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(r1, r2))

	stores := []string{sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc"), r1.InternalEndpoint("grpc"), r2.InternalEndpoint("grpc")}
	q, err := qBuilder.WithRuleAddresses(stores...).Initiate(qUninit, stores...)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(4), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics()))

	ruleAndAssert(t, ctx, q.Endpoint("http"), "", []*rulespb.RuleGroup{
		{
			Name: "example_abort",
			File: "/shared/rules/rules.yaml",
			Rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:  "TestAlert_AbortOnPartialResponse",
					State: rulespb.AlertState_FIRING,
					Query: "absent(some_metric)",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "prometheus", Value: "ha"},
						{Name: "severity", Value: "page"},
					}},
				}),
			},
		},
		{
			Name: "example_abort",
			File: "/shared/thanos-rules/rules-0.yaml",
			Rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:  "TestAlert_AbortOnPartialResponse",
					State: rulespb.AlertState_FIRING,
					Query: "absent(some_metric)",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "severity", Value: "page"},
					}},
				}),
			},
		},
		{
			Name: "example_warn",
			File: "/shared/thanos-rules/rules-1.yaml",
			Rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:  "TestAlert_WarnOnPartialResponse",
					State: rulespb.AlertState_FIRING,
					Query: "absent(some_metric)",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "severity", Value: "page"},
					}},
				}),
			},
		},
	})
}

func ruleAndAssert(t *testing.T, ctx context.Context, addr, typ string, want []*rulespb.RuleGroup) {
	t.Helper()

	fmt.Println("ruleAndAssert: Waiting for results for rules type", typ)
	var result []*rulespb.RuleGroup

	logger := log.NewLogfmtLogger(os.Stdout)
	testutil.Ok(t, runutil.RetryWithLog(logger, time.Second, ctx.Done(), func() error {
		res, err := promclient.NewDefaultClient().RulesInGRPC(ctx, mustURLParse(t, "http://"+addr), typ)
		if err != nil {
			return err
		}

		if len(result) != len(res) {
			fmt.Println("ruleAndAssert: new result:", res)
			result = res
		}

		if len(res) != len(want) {
			return errors.Errorf("unexpected result size, want %d; got: %d result: %v", len(want), len(res), res)
		}

		for ig, g := range res {
			res[ig].LastEvaluation = time.Time{}
			res[ig].EvaluationDurationSeconds = 0
			res[ig].Interval = 0
			res[ig].PartialResponseStrategy = 0

			sort.Slice(g.Rules, func(i, j int) bool { return g.Rules[i].Compare(g.Rules[j]) < 0 })

			for ir, r := range g.Rules {
				if alert := r.GetAlert(); alert != nil {
					res[ig].Rules[ir] = rulespb.NewAlertingRule(&rulespb.Alert{
						Name:   alert.Name,
						State:  alert.State,
						Query:  alert.Query,
						Labels: alert.Labels,
					})
				} else if rec := r.GetRecording(); rec != nil {
					res[ig].Rules[ir] = rulespb.NewAlertingRule(&rulespb.Alert{
						Name:   rec.Name,
						Query:  rec.Query,
						Labels: rec.Labels,
					})
				}
			}
		}

		if !reflect.DeepEqual(want, res) {
			return errors.Errorf("unexpected result\nwant %v\ngot: %v", want, res)
		}

		return nil
	}))
}
