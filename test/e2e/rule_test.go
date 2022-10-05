// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2emon "github.com/efficientgo/e2e/monitoring"
	common_cfg "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/thanos-io/thanos/pkg/errors"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/alert"
	"github.com/thanos-io/thanos/pkg/httpconfig"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

const (
	testAlertRuleAbortOnPartialResponse = `
groups:
- name: example_abort
  interval: 1s
  # Abort should be a default: partial_response_strategy: "ABORT"
  rules:
  - alert: TestAlert_AbortOnPartialResponse
    # It must be based on actual metrics otherwise call to StoreAPI would be not involved.
    expr: absent(some_metric)
    labels:
      severity: page
    annotations:
      summary: "I always complain, but I don't allow partial response in query."
`
	testAlertRuleWarnOnPartialResponse = `
groups:
- name: example_warn
  interval: 1s
  partial_response_strategy: "WARN"
  rules:
  - alert: TestAlert_WarnOnPartialResponse
    # It must be based on actual metric, otherwise call to StoreAPI would be not involved.
    expr: absent(some_metric)
    labels:
      severity: page
    annotations:
      summary: "I always complain and allow partial response in query."
`
	testAlertRuleAddedLaterWebHandler = `
groups:
- name: example
  interval: 1s
  partial_response_strategy: "WARN"
  rules:
  - alert: TestAlert_HasBeenLoadedViaWebHandler
    # It must be based on actual metric, otherwise call to StoreAPI would be not involved.
    expr: absent(some_metric)
    labels:
      severity: page
    annotations:
      summary: "I always complain and I have been loaded via /-/reload."
`
	testAlertRuleAddedLaterSignal = `
groups:
- name: example
  interval: 1s
  partial_response_strategy: "WARN"
  rules:
  - alert: TestAlert_HasBeenLoadedViaWebHandler
    # It must be based on actual metric, otherwise call to StoreAPI would be not involved.
    expr: absent(some_metric)
    labels:
      severity: page
    annotations:
      summary: "I always complain and I have been loaded via sighup signal."
- name: example2
  interval: 1s
  partial_response_strategy: "WARN"
  rules:
  - alert: TestAlert_HasBeenLoadedViaWebHandler
    # It must be based on actual metric, otherwise call to StoreAPI would be not involved.
    expr: absent(some_metric)
    labels:
      severity: page
    annotations:
      summary: "I always complain and I have been loaded via sighup signal."
`
	testAlertRuleWithLimit = `
groups:
- name: example_with_limit
  interval: 1s
  partial_response_strategy: "WARN"
  limit: 1
  rules:
  - alert: TestAlert_WithLimit
    expr: 'promhttp_metric_handler_requests_total' # It has more than one labels.
    labels:
      severity: page
    annotations:
      summary: "with limit"
`

	testRuleRecordAbsentMetric = `
groups:
- name: example_record_rules
  interval: 1s
  rules:
  - record: test_absent_metric
    expr: absent(nonexistent{job='thanos-receive'})
`
	amTimeout = model.Duration(10 * time.Second)
)

type rulesResp struct {
	Status string
	Data   *rulespb.RuleGroups
}

func createRuleFile(t *testing.T, path, content string) {
	t.Helper()
	err := os.WriteFile(path, []byte(content), 0666)
	testutil.Ok(t, err)
}

func createRuleFiles(t *testing.T, dir string) {
	t.Helper()

	for i, rule := range []string{testAlertRuleAbortOnPartialResponse, testAlertRuleWarnOnPartialResponse} {
		createRuleFile(t, filepath.Join(dir, fmt.Sprintf("rules-%d.yaml", i)), rule)
	}
}

func reloadRulesHTTP(t *testing.T, ctx context.Context, endpoint string) {
	req, err := http.NewRequestWithContext(ctx, "POST", "http://"+endpoint+"/-/reload", io.NopCloser(bytes.NewReader(nil)))
	testutil.Ok(t, err)
	resp, err := http.DefaultClient.Do(req)
	testutil.Ok(t, err)
	defer resp.Body.Close()
	testutil.Equals(t, 200, resp.StatusCode)
}

func reloadRulesSignal(t *testing.T, r *e2emon.InstrumentedRunnable) {
	c := e2e.NewCommand("kill", "-1", "1")
	testutil.Ok(t, r.Exec(c))
}

func checkReloadSuccessful(t *testing.T, ctx context.Context, endpoint string, expectedRulegroupCount int) {
	data := rulesResp{}
	errCount := 0

	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() error {
		req, err := http.NewRequestWithContext(ctx, "GET", "http://"+endpoint+"/api/v1/rules", io.NopCloser(bytes.NewReader(nil)))
		if err != nil {
			errCount++
			return err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			errCount++
			return err
		}

		if resp.StatusCode != 200 {
			errCount++
			return errors.Newf("statuscode is not 200, got %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			errCount++
			return errors.Wrapf(err, "error reading body")
		}

		if err := resp.Body.Close(); err != nil {
			errCount++
			return err
		}

		if err := json.Unmarshal(body, &data); err != nil {
			errCount++
			return errors.Wrapf(err, "error unmarshaling body")
		}

		if data.Status != "success" {
			errCount++
			return errors.Newf("response status is not success, got %s", data.Status)
		}

		if len(data.Data.Groups) == expectedRulegroupCount {
			return nil
		}

		errCount++
		return errors.Newf("different number of rulegroups: expected %d, got %d", expectedRulegroupCount, len(data.Data.Groups))
	}))

	testutil.Assert(t, len(data.Data.Groups) == expectedRulegroupCount, fmt.Sprintf("expected there to be %d rule groups but got %d. encountered %d errors", expectedRulegroupCount, len(data.Data.Groups), errCount))
}

func rulegroupCorrectData(t *testing.T, ctx context.Context, endpoint string) {
	req, err := http.NewRequestWithContext(ctx, "GET", "http://"+endpoint+"/api/v1/rules", io.NopCloser(bytes.NewReader(nil)))
	testutil.Ok(t, err)
	resp, err := http.DefaultClient.Do(req)
	testutil.Ok(t, err)
	testutil.Equals(t, 200, resp.StatusCode)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	testutil.Ok(t, err)

	var data = rulesResp{}

	testutil.Ok(t, json.Unmarshal(body, &data))
	testutil.Equals(t, "success", data.Status)

	testutil.Assert(t, len(data.Data.Groups) > 0, "expected there to be some rule groups")

	for _, g := range data.Data.Groups {
		testutil.Assert(t, g.EvaluationDurationSeconds > 0, "expected it to take more than zero seconds to evaluate")
		testutil.Assert(t, !g.LastEvaluation.IsZero(), "expected the rule group to be evaluated at least once")
	}
}

func writeTargets(t *testing.T, path string, addrs ...string) {
	t.Helper()

	var tgs []model.LabelSet
	for _, a := range addrs {
		tgs = append(
			tgs,
			model.LabelSet{
				model.AddressLabel: model.LabelValue(a),
			},
		)
	}
	b, err := yaml.Marshal([]*targetgroup.Group{{Targets: tgs}})
	testutil.Ok(t, err)

	testutil.Ok(t, os.WriteFile(path+".tmp", b, 0660))
	testutil.Ok(t, os.Rename(path+".tmp", path))
}

func TestRule(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e-test-rule")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	am1 := e2ethanos.NewAlertmanager(e, "1")
	am2 := e2ethanos.NewAlertmanager(e, "2")
	testutil.Ok(t, e2e.StartAndWaitReady(am1, am2))

	rFuture := e2ethanos.NewRulerBuilder(e, "1")

	amTargetsSubDir := filepath.Join("rules_am_targets")
	testutil.Ok(t, os.MkdirAll(filepath.Join(rFuture.Dir(), amTargetsSubDir), os.ModePerm))
	queryTargetsSubDir := filepath.Join("rules_query_targets")
	testutil.Ok(t, os.MkdirAll(filepath.Join(rFuture.Dir(), queryTargetsSubDir), os.ModePerm))

	rulesSubDir := filepath.Join("rules")
	rulesPath := filepath.Join(rFuture.Dir(), rulesSubDir)
	testutil.Ok(t, os.MkdirAll(rulesPath, os.ModePerm))
	createRuleFiles(t, rulesPath)

	r := rFuture.WithAlertManagerConfig([]alert.AlertmanagerConfig{
		{
			EndpointsConfig: httpconfig.EndpointsConfig{
				FileSDConfigs: []httpconfig.FileSDConfig{
					{
						// FileSD which will be used to register discover dynamically am1.
						Files:           []string{filepath.Join(rFuture.InternalDir(), amTargetsSubDir, "*.yaml")},
						RefreshInterval: model.Duration(time.Second),
					},
				},
				StaticAddresses: []string{
					am2.InternalEndpoint("http"),
				},
				Scheme: "http",
			},
			Timeout:    amTimeout,
			APIVersion: alert.APIv1,
		},
	}).InitTSDB(filepath.Join(rFuture.InternalDir(), rulesSubDir), []httpconfig.Config{
		{
			EndpointsConfig: httpconfig.EndpointsConfig{
				// We test Statically Addressed queries in other tests. Focus on FileSD here.
				FileSDConfigs: []httpconfig.FileSDConfig{
					{
						// FileSD which will be used to register discover dynamically q.
						Files:           []string{filepath.Join(rFuture.InternalDir(), queryTargetsSubDir, "*.yaml")},
						RefreshInterval: model.Duration(time.Second),
					},
				},
				Scheme: "http",
			},
		},
	})
	testutil.Ok(t, e2e.StartAndWaitReady(r))

	q := e2ethanos.NewQuerierBuilder(e, "1", r.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	t.Run("no query configured", func(t *testing.T) {
		// Check for a few evaluations, check all of them failed.
		testutil.Ok(t, r.WaitSumMetrics(e2emon.Greater(10), "prometheus_rule_evaluations_total"))
		testutil.Ok(t, r.WaitSumMetrics(e2emon.EqualsAmongTwo, "prometheus_rule_evaluations_total", "prometheus_rule_evaluation_failures_total"))

		// No alerts sent.
		testutil.Ok(t, r.WaitSumMetrics(e2emon.Equals(0), "thanos_alert_sender_alerts_dropped_total"))
	})

	var currentFailures float64
	t.Run("attach query", func(t *testing.T) {
		// Attach querier to target files.
		writeTargets(t, filepath.Join(rFuture.Dir(), queryTargetsSubDir, "targets.yaml"), q.InternalEndpoint("http"))

		testutil.Ok(t, r.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_rule_query_apis_dns_provider_results"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, r.WaitSumMetrics(e2emon.Equals(1), "thanos_rule_alertmanagers_dns_provider_results"))

		var currentVal float64
		testutil.Ok(t, r.WaitSumMetrics(func(sums ...float64) bool {
			currentVal = sums[0]
			currentFailures = sums[1]
			return true
		}, "prometheus_rule_evaluations_total", "prometheus_rule_evaluation_failures_total"))

		// Check for a few evaluations, check all of them failed.
		testutil.Ok(t, r.WaitSumMetrics(e2emon.Greater(currentVal+4), "prometheus_rule_evaluations_total"))
		// No failures.
		testutil.Ok(t, r.WaitSumMetrics(e2emon.Equals(currentFailures), "prometheus_rule_evaluation_failures_total"))

		// Alerts sent.
		testutil.Ok(t, r.WaitSumMetrics(e2emon.Equals(0), "thanos_alert_sender_alerts_dropped_total"))
		testutil.Ok(t, r.WaitSumMetrics(e2emon.Greater(4), "thanos_alert_sender_alerts_sent_total"))

		// Alerts received.
		testutil.Ok(t, am2.WaitSumMetrics(e2emon.Equals(2), "alertmanager_alerts"))
		testutil.Ok(t, am2.WaitSumMetrics(e2emon.Greater(4), "alertmanager_alerts_received_total"))
		testutil.Ok(t, am2.WaitSumMetrics(e2emon.Equals(0), "alertmanager_alerts_invalid_total"))

		// am1 not connected, so should not receive anything.
		testutil.Ok(t, am1.WaitSumMetrics(e2emon.Equals(0), "alertmanager_alerts"))
		testutil.Ok(t, am1.WaitSumMetrics(e2emon.Equals(0), "alertmanager_alerts_received_total"))
		testutil.Ok(t, am1.WaitSumMetrics(e2emon.Equals(0), "alertmanager_alerts_invalid_total"))
	})
	t.Run("attach am1", func(t *testing.T) {
		// Attach am1 to target files.
		writeTargets(t, filepath.Join(rFuture.Dir(), amTargetsSubDir, "targets.yaml"), am1.InternalEndpoint("http"))

		testutil.Ok(t, r.WaitSumMetrics(e2emon.Equals(1), "thanos_rule_query_apis_dns_provider_results"))
		testutil.Ok(t, r.WaitSumMetrics(e2emon.Equals(2), "thanos_rule_alertmanagers_dns_provider_results"))

		testutil.Ok(t, r.WaitSumMetrics(e2emon.Equals(currentFailures), "prometheus_rule_evaluation_failures_total"))

		var currentVal float64
		testutil.Ok(t, am2.WaitSumMetrics(func(sums ...float64) bool {
			currentVal = sums[0]
			return true
		}, "alertmanager_alerts_received_total"))

		// Alerts received by both am1 and am2.
		testutil.Ok(t, am2.WaitSumMetrics(e2emon.Equals(2), "alertmanager_alerts"))
		testutil.Ok(t, am2.WaitSumMetrics(e2emon.Greater(currentVal+4), "alertmanager_alerts_received_total"))
		testutil.Ok(t, am2.WaitSumMetrics(e2emon.Equals(0), "alertmanager_alerts_invalid_total"))

		testutil.Ok(t, am1.WaitSumMetrics(e2emon.Equals(2), "alertmanager_alerts"))
		testutil.Ok(t, am1.WaitSumMetrics(e2emon.Greater(4), "alertmanager_alerts_received_total"))
		testutil.Ok(t, am1.WaitSumMetrics(e2emon.Equals(0), "alertmanager_alerts_invalid_total"))
	})

	t.Run("am1 drops again", func(t *testing.T) {
		testutil.Ok(t, os.RemoveAll(filepath.Join(rFuture.Dir(), amTargetsSubDir, "targets.yaml")))

		testutil.Ok(t, r.WaitSumMetrics(e2emon.Equals(1), "thanos_rule_query_apis_dns_provider_results"))
		testutil.Ok(t, r.WaitSumMetrics(e2emon.Equals(1), "thanos_rule_alertmanagers_dns_provider_results"))
		testutil.Ok(t, r.WaitSumMetrics(e2emon.Equals(currentFailures), "prometheus_rule_evaluation_failures_total"))

		var currentValAm1 float64
		testutil.Ok(t, am1.WaitSumMetrics(func(sums ...float64) bool {
			currentValAm1 = sums[0]
			return true
		}, "alertmanager_alerts_received_total"))

		var currentValAm2 float64
		testutil.Ok(t, am2.WaitSumMetrics(func(sums ...float64) bool {
			currentValAm2 = sums[0]
			return true
		}, "alertmanager_alerts_received_total"))

		// Alerts received by both am1 and am2.
		testutil.Ok(t, am2.WaitSumMetrics(e2emon.Equals(2), "alertmanager_alerts"))
		testutil.Ok(t, am2.WaitSumMetrics(e2emon.Greater(currentValAm2+4), "alertmanager_alerts_received_total"))
		testutil.Ok(t, am2.WaitSumMetrics(e2emon.Equals(0), "alertmanager_alerts_invalid_total"))

		// Am1 should not receive more alerts.
		testutil.Ok(t, am1.WaitSumMetrics(e2emon.Equals(currentValAm1), "alertmanager_alerts_received_total"))
	})

	t.Run("duplicate am", func(t *testing.T) {
		// am2 is already registered in static addresses.
		writeTargets(t, filepath.Join(rFuture.Dir(), amTargetsSubDir, "targets.yaml"), am2.InternalEndpoint("http"))

		testutil.Ok(t, r.WaitSumMetrics(e2emon.Equals(1), "thanos_rule_query_apis_dns_provider_results"))
		testutil.Ok(t, r.WaitSumMetrics(e2emon.Equals(1), "thanos_rule_alertmanagers_dns_provider_results"))
	})

	t.Run("rule groups have last evaluation and evaluation duration set", func(t *testing.T) {
		rulegroupCorrectData(t, ctx, r.Endpoint("http"))
	})

	t.Run("signal reload works", func(t *testing.T) {
		// Add a new rule via sending sighup
		createRuleFile(t, filepath.Join(rulesPath, "newrule.yaml"), testAlertRuleAddedLaterSignal)
		reloadRulesSignal(t, r)
		checkReloadSuccessful(t, ctx, r.Endpoint("http"), 4)
	})

	t.Run("http reload works", func(t *testing.T) {
		// Add a new rule via /-/reload.
		createRuleFile(t, filepath.Join(rulesPath, "newrule.yaml"), testAlertRuleAddedLaterWebHandler)
		reloadRulesHTTP(t, ctx, r.Endpoint("http"))
		checkReloadSuccessful(t, ctx, r.Endpoint("http"), 3)
	})

	t.Run("query alerts", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string { return "ALERTS" }, time.Now, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"__name__":   "ALERTS",
				"severity":   "page",
				"alertname":  "TestAlert_AbortOnPartialResponse",
				"alertstate": "firing",
				"replica":    "1",
			},
			{
				"__name__":   "ALERTS",
				"severity":   "page",
				"alertname":  "TestAlert_HasBeenLoadedViaWebHandler",
				"alertstate": "firing",
				"replica":    "1",
			},
			{
				"__name__":   "ALERTS",
				"severity":   "page",
				"alertname":  "TestAlert_WarnOnPartialResponse",
				"alertstate": "firing",
				"replica":    "1",
			},
		})

		expAlertLabels := []model.LabelSet{
			{
				"severity":  "page",
				"alertname": "TestAlert_AbortOnPartialResponse",
				"replica":   "1",
			},
			{
				"severity":  "page",
				"alertname": "TestAlert_HasBeenLoadedViaWebHandler",
				"replica":   "1",
			},
			{
				"severity":  "page",
				"alertname": "TestAlert_WarnOnPartialResponse",
				"replica":   "1",
			},
		}

		alrts, err := promclient.NewDefaultClient().AlertmanagerAlerts(ctx, urlParse(t, "http://"+am2.Endpoint("http")))
		testutil.Ok(t, err)

		testutil.Equals(t, len(expAlertLabels), len(alrts))
		for i, a := range alrts {
			testutil.Assert(t, a.Labels.Equal(expAlertLabels[i]), "unexpected labels %s", a.Labels)
		}
	})
}

// TestRule_CanRemoteWriteData checks that Thanos Ruler can be run in stateless mode
// where it remote_writes rule evaluations to a Prometheus remote-write endpoint (typically
// a Thanos Receiver).
func TestRule_CanRemoteWriteData(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("rule-rw")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	rFuture := e2ethanos.NewRulerBuilder(e, "1")
	rulesSubDir := "rules"
	rulesPath := filepath.Join(rFuture.Dir(), rulesSubDir)
	testutil.Ok(t, os.MkdirAll(rulesPath, os.ModePerm))

	for i, rule := range []string{testRuleRecordAbsentMetric, testAlertRuleWarnOnPartialResponse} {
		createRuleFile(t, filepath.Join(rulesPath, fmt.Sprintf("rules-%d.yaml", i)), rule)
	}

	am := e2ethanos.NewAlertmanager(e, "1")
	testutil.Ok(t, e2e.StartAndWaitReady(am))

	receiver := e2ethanos.NewReceiveBuilder(e, "1").WithIngestionEnabled().Init()
	testutil.Ok(t, e2e.StartAndWaitReady(receiver))
	rwURL := urlParse(t, e2ethanos.RemoteWriteEndpoint(receiver.InternalEndpoint("remote-write")))

	receiver2 := e2ethanos.NewReceiveBuilder(e, "2").WithIngestionEnabled().Init()
	testutil.Ok(t, e2e.StartAndWaitReady(receiver2))
	rwURL2 := urlParse(t, e2ethanos.RemoteWriteEndpoint(receiver2.InternalEndpoint("remote-write")))

	q := e2ethanos.NewQuerierBuilder(e, "1", receiver.InternalEndpoint("grpc"), receiver2.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	r := rFuture.WithAlertManagerConfig([]alert.AlertmanagerConfig{
		{
			EndpointsConfig: httpconfig.EndpointsConfig{
				StaticAddresses: []string{
					am.InternalEndpoint("http"),
				},
				Scheme: "http",
			},
			Timeout:    amTimeout,
			APIVersion: alert.APIv1,
		},
	}).InitStateless(filepath.Join(rFuture.InternalDir(), rulesSubDir), []httpconfig.Config{
		{
			EndpointsConfig: httpconfig.EndpointsConfig{
				StaticAddresses: []string{
					q.InternalEndpoint("http"),
				},
				Scheme: "http",
			},
		},
	}, []*config.RemoteWriteConfig{
		{URL: &common_cfg.URL{URL: rwURL}, Name: "thanos-receiver"},
		{URL: &common_cfg.URL{URL: rwURL2}, Name: "thanos-receiver2"},
	})
	testutil.Ok(t, e2e.StartAndWaitReady(r))

	// Wait until remote write samples are written to receivers successfully.
	testutil.Ok(t, r.WaitSumMetricsWithOptions(e2emon.GreaterOrEqual(1), []string{"prometheus_remote_storage_samples_total"}, e2emon.WaitMissingMetrics()))

	t.Run("can fetch remote-written samples from receiver", func(t *testing.T) {
		testRecordedSamples := func() string { return "test_absent_metric" }
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), testRecordedSamples, time.Now, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"__name__":  "test_absent_metric",
				"job":       "thanos-receive",
				"receive":   model.LabelValue(receiver.Name()),
				"replica":   "1",
				"tenant_id": "default-tenant",
			},
			{
				"__name__":  "test_absent_metric",
				"job":       "thanos-receive",
				"receive":   model.LabelValue(receiver2.Name()),
				"replica":   "1",
				"tenant_id": "default-tenant",
			},
		})
	})
}

// TestRule_CanPersistWALData checks that in stateless mode, Thanos Ruler can persist rule evaluations
// which couldn't be sent to the remote write endpoint (e.g because receiver isn't available).
func TestRule_CanPersistWALData(t *testing.T) {
	//TODO: Implement test with unavailable remote-write endpoint(receiver)
}

// Test Ruler behavior on different storepb.PartialResponseStrategy when having partial response from single `failingStoreAPI`.
func TestRulePartialResponse(t *testing.T) {
	t.Skip("TODO: Allow HTTP ports from binaries running on host to be accessible.")

	// TODO: Implement with failing store.
}
