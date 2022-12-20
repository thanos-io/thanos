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
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/rules"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/httpconfig"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestRulesAPI_Fanout(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("rules-fanout")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	qBuilder := e2ethanos.NewQuerierBuilder(e, "query")

	// Use querier work dir for shared resources (easiest to obtain).
	promRulesSubDir := filepath.Join("rules")
	testutil.Ok(t, os.MkdirAll(filepath.Join(qBuilder.Dir(), promRulesSubDir), os.ModePerm))
	// Create the abort_on_partial_response alert for Prometheus.
	// We don't create the warn_on_partial_response alert as Prometheus has strict yaml unmarshalling.
	createRuleFile(t, filepath.Join(qBuilder.Dir(), promRulesSubDir, "rules.yaml"), testAlertRuleAbortOnPartialResponse)

	thanosRulesSubDir := filepath.Join("thanos-rules")
	testutil.Ok(t, os.MkdirAll(filepath.Join(qBuilder.Dir(), thanosRulesSubDir), os.ModePerm))
	createRuleFiles(t, filepath.Join(qBuilder.Dir(), thanosRulesSubDir))
	// We create a rule group with limit.
	createRuleFile(t, filepath.Join(qBuilder.Dir(), thanosRulesSubDir, "rules-with-limit.yaml"), testAlertRuleWithLimit)

	// 2x Prometheus.
	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(
		e,
		"prom1",
		e2ethanos.DefaultPromConfig("ha", 0, "", filepath.Join(qBuilder.InternalDir(), promRulesSubDir, "*.yaml"), e2ethanos.LocalPrometheusTarget),
		"",
		e2ethanos.DefaultPrometheusImage(), "",
	)
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(
		e,
		"prom2",
		e2ethanos.DefaultPromConfig("ha", 1, "", filepath.Join(qBuilder.InternalDir(), promRulesSubDir, "*.yaml"), e2ethanos.LocalPrometheusTarget),
		"",
		e2ethanos.DefaultPrometheusImage(), "",
	)
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	queryCfg := []httpconfig.Config{
		{
			EndpointsConfig: httpconfig.EndpointsConfig{
				StaticAddresses: []string{qBuilder.InternalEndpoint("http")},
				Scheme:          "http",
			},
		},
	}

	// Recreate rulers with the corresponding query config.
	r1 := e2ethanos.NewRulerBuilder(e, "rule1").InitTSDB(filepath.Join(qBuilder.InternalDir(), thanosRulesSubDir), queryCfg)
	r2 := e2ethanos.NewRulerBuilder(e, "rule2").InitTSDB(filepath.Join(qBuilder.InternalDir(), thanosRulesSubDir), queryCfg)
	testutil.Ok(t, e2e.StartAndWaitReady(r1, r2))

	stores := []string{sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc"), r1.InternalEndpoint("grpc"), r2.InternalEndpoint("grpc")}
	q := qBuilder.
		WithStoreAddresses(stores...).
		WithRuleAddresses(stores...).
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(4), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

	ruleAndAssert(t, ctx, q.Endpoint("http"), "", []*rulespb.RuleGroup{
		{
			Name: "example_abort",
			File: "/shared/data/querier-query/rules/rules.yaml",
			Rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:  "TestAlert_AbortOnPartialResponse",
					State: rulespb.AlertState_FIRING,
					Query: "absent(some_metric)",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "prometheus", Value: "ha"},
						{Name: "severity", Value: "page"},
					}},
					Health: string(rules.HealthGood),
				}),
			},
		},
		{
			Name: "example_abort",
			File: "/shared/data/querier-query/thanos-rules/rules-0.yaml",
			Rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:  "TestAlert_AbortOnPartialResponse",
					State: rulespb.AlertState_FIRING,
					Query: "absent(some_metric)",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "severity", Value: "page"},
					}},
					Health: string(rules.HealthGood),
				}),
			},
		},
		{
			Name: "example_warn",
			File: "/shared/data/querier-query/thanos-rules/rules-1.yaml",
			Rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:  "TestAlert_WarnOnPartialResponse",
					State: rulespb.AlertState_FIRING,
					Query: "absent(some_metric)",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "severity", Value: "page"},
					}},
					Health: string(rules.HealthGood),
				}),
			},
		},
		{
			Name:  "example_with_limit",
			File:  "/shared/data/querier-query/thanos-rules/rules-with-limit.yaml",
			Limit: 1,
			Rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:  "TestAlert_WithLimit",
					State: rulespb.AlertState_INACTIVE,
					Query: `promhttp_metric_handler_requests_total`,
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "severity", Value: "page"},
					}},
					Health: string(rules.HealthBad),
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
		res, err := promclient.NewDefaultClient().RulesInGRPC(ctx, urlParse(t, "http://"+addr), typ)
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
						Health: alert.Health,
					})
				} else if rec := r.GetRecording(); rec != nil {
					res[ig].Rules[ir] = rulespb.NewAlertingRule(&rulespb.Alert{
						Name:   rec.Name,
						Query:  rec.Query,
						Labels: rec.Labels,
						Health: rec.Health,
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
