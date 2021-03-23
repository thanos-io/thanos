// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/alert"
	http_util "github.com/thanos-io/thanos/pkg/http"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

const (
	testAlertRuleAbortOnPartialResponse = `
groups:
- name: example_abort
  interval: 100ms
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
  interval: 100ms
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
  interval: 100ms
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
)

func createRuleFile(t *testing.T, path, content string) {
	t.Helper()
	err := ioutil.WriteFile(path, []byte(content), 0666)
	testutil.Ok(t, err)
}

func createRuleFiles(t *testing.T, dir string) {
	t.Helper()

	for i, rule := range []string{testAlertRuleAbortOnPartialResponse, testAlertRuleWarnOnPartialResponse} {
		createRuleFile(t, filepath.Join(dir, fmt.Sprintf("rules-%d.yaml", i)), rule)
	}
}

func reloadRulesHTTP(t *testing.T, ctx context.Context, endpoint string) {
	req, err := http.NewRequestWithContext(ctx, "POST", "http://"+endpoint+"/-/reload", ioutil.NopCloser(bytes.NewReader(nil)))
	testutil.Ok(t, err)
	resp, err := http.DefaultClient.Do(req)
	testutil.Ok(t, err)
	defer resp.Body.Close()
	testutil.Equals(t, 200, resp.StatusCode)
}

func rulegroupCorrectData(t *testing.T, ctx context.Context, endpoint string) {
	req, err := http.NewRequestWithContext(ctx, "GET", "http://"+endpoint+"/api/v1/rules", ioutil.NopCloser(bytes.NewReader(nil)))
	testutil.Ok(t, err)
	resp, err := http.DefaultClient.Do(req)
	testutil.Ok(t, err)
	testutil.Equals(t, 200, resp.StatusCode)
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	testutil.Ok(t, err)

	var data struct {
		Status string
		Data   *rulespb.RuleGroups
	}

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

	testutil.Ok(t, ioutil.WriteFile(path+".tmp", b, 0660))
	testutil.Ok(t, os.Rename(path+".tmp", path))
}

type mockAlertmanager struct {
	path      string
	token     string
	mtx       sync.Mutex
	alerts    []*model.Alert
	lastError error
}

func newMockAlertmanager(path string, token string) *mockAlertmanager {
	return &mockAlertmanager{
		path:   path,
		token:  token,
		alerts: make([]*model.Alert, 0),
	}
}

func (m *mockAlertmanager) setLastError(err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.lastError = err
}

func (m *mockAlertmanager) LastError() error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.lastError
}

func (m *mockAlertmanager) Alerts() []*model.Alert {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.alerts
}

func (m *mockAlertmanager) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		m.setLastError(errors.Errorf("invalid method: %s", req.Method))
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if req.URL.Path != m.path {
		m.setLastError(errors.Errorf("invalid path: %s", req.URL.Path))
		resp.WriteHeader(http.StatusNotFound)
		return
	}

	if m.token != "" {
		auth := req.Header.Get("Authorization")
		if auth != fmt.Sprintf("Bearer %s", m.token) {
			m.setLastError(errors.Errorf("invalid auth: %s", req.URL.Path))
			resp.WriteHeader(http.StatusForbidden)
			return
		}
	}

	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		m.setLastError(err)
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	var alerts []*model.Alert
	if err := json.Unmarshal(b, &alerts); err != nil {
		m.setLastError(err)
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	m.mtx.Lock()
	m.alerts = append(m.alerts, alerts...)
	m.mtx.Unlock()
}

// TestRule_AlertmanagerHTTPClient verifies that Thanos Ruler can send alerts to
// Alertmanager in various setups:
// * Plain HTTP.
// * HTTPS with custom CA.
// * API with a prefix.
// * API protected by bearer token authentication.
//
// Because Alertmanager supports HTTP only and no authentication, the test uses
// a mocked server instead of the "real" Alertmanager service.
// The other end-to-end tests exercise against the "real" Alertmanager
// implementation.
func TestRule_AlertmanagerHTTPClient(t *testing.T) {
	t.Skip("TODO: Allow HTTP ports from binaries running on host to be accessible.")

	s, err := e2e.NewScenario("e2e_test_rule_am_http_client")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	tlsSubDir := filepath.Join("tls")
	testutil.Ok(t, os.MkdirAll(filepath.Join(s.SharedDir(), tlsSubDir), os.ModePerm))

	// API v1 with plain HTTP and a prefix.
	handler1 := newMockAlertmanager("/prefix/api/v1/alerts", "")
	srv1 := httptest.NewServer(handler1)
	t.Cleanup(srv1.Close)

	// API v2 with HTTPS and authentication.
	handler2 := newMockAlertmanager("/api/v2/alerts", "secret")
	srv2 := httptest.NewTLSServer(handler2)
	t.Cleanup(srv2.Close)

	var out bytes.Buffer
	testutil.Ok(t, pem.Encode(&out, &pem.Block{Type: "CERTIFICATE", Bytes: srv2.TLS.Certificates[0].Certificate[0]}))
	caFile := filepath.Join(s.SharedDir(), tlsSubDir, "ca.crt")
	testutil.Ok(t, ioutil.WriteFile(caFile, out.Bytes(), 0640))

	rulesSubDir := filepath.Join("rules")
	testutil.Ok(t, os.MkdirAll(filepath.Join(s.SharedDir(), rulesSubDir), os.ModePerm))
	createRuleFiles(t, filepath.Join(s.SharedDir(), rulesSubDir))

	r, err := e2ethanos.NewRuler(s.SharedDir(), "1", rulesSubDir, []alert.AlertmanagerConfig{
		{
			EndpointsConfig: http_util.EndpointsConfig{
				StaticAddresses: []string{srv1.Listener.Addr().String()},
				Scheme:          "http",
				PathPrefix:      "/prefix/",
			},
			Timeout:    model.Duration(time.Second),
			APIVersion: alert.APIv1,
		},
		{
			HTTPClientConfig: http_util.ClientConfig{
				TLSConfig: http_util.TLSConfig{
					CAFile: filepath.Join(e2e.ContainerSharedDir, tlsSubDir, "ca.crt"),
				},
				BearerToken: "secret",
			},
			EndpointsConfig: http_util.EndpointsConfig{
				StaticAddresses: []string{srv2.Listener.Addr().String()},
				Scheme:          "https",
			},
			Timeout:    model.Duration(time.Second),
			APIVersion: alert.APIv2,
		},
	}, []query.Config{
		{
			EndpointsConfig: http_util.EndpointsConfig{
				StaticAddresses: func() []string {
					q, err := e2ethanos.NewQuerier(s.SharedDir(), "1", nil, nil, nil, nil, nil, "", "")
					testutil.Ok(t, err)
					return []string{q.NetworkHTTPEndpointFor(s.NetworkName())}
				}(),
				Scheme: "http",
			},
		},
	})
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(r))

	q, err := e2ethanos.NewQuerier(s.SharedDir(), "1", []string{r.GRPCNetworkEndpoint()}, nil, nil, nil, nil, "", "")
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() (err error) {
		for i, am := range []*mockAlertmanager{handler1, handler2} {
			if len(am.Alerts()) == 0 {
				return errors.Errorf("no alert received from handler%d, last error: %v", i, am.LastError())
			}
		}

		return nil
	}))
}

func TestRule(t *testing.T) {
	t.Skip("Flaky test. Fix it. See: https://github.com/thanos-io/thanos/issues/3425.")
	t.Parallel()

	s, err := e2e.NewScenario("e2e_test_rule")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	// Prepare work dirs.
	rulesSubDir := filepath.Join("rules")
	rulesPath := filepath.Join(s.SharedDir(), rulesSubDir)
	testutil.Ok(t, os.MkdirAll(rulesPath, os.ModePerm))
	createRuleFiles(t, rulesPath)
	amTargetsSubDir := filepath.Join("rules_am_targets")
	testutil.Ok(t, os.MkdirAll(filepath.Join(s.SharedDir(), amTargetsSubDir), os.ModePerm))
	queryTargetsSubDir := filepath.Join("rules_query_targets")
	testutil.Ok(t, os.MkdirAll(filepath.Join(s.SharedDir(), queryTargetsSubDir), os.ModePerm))

	am1, err := e2ethanos.NewAlertmanager(s.SharedDir(), "1")
	testutil.Ok(t, err)
	am2, err := e2ethanos.NewAlertmanager(s.SharedDir(), "2")
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(am1, am2))

	r, err := e2ethanos.NewRuler(s.SharedDir(), "1", rulesSubDir, []alert.AlertmanagerConfig{
		{
			EndpointsConfig: http_util.EndpointsConfig{
				FileSDConfigs: []http_util.FileSDConfig{
					{
						// FileSD which will be used to register discover dynamically am1.
						Files:           []string{filepath.Join(e2e.ContainerSharedDir, amTargetsSubDir, "*.yaml")},
						RefreshInterval: model.Duration(time.Second),
					},
				},
				StaticAddresses: []string{
					am2.NetworkHTTPEndpoint(),
				},
				Scheme: "http",
			},
			Timeout:    model.Duration(time.Second),
			APIVersion: alert.APIv1,
		},
	}, []query.Config{
		{
			EndpointsConfig: http_util.EndpointsConfig{
				// We test Statically Addressed queries in other tests. Focus on FileSD here.
				FileSDConfigs: []http_util.FileSDConfig{
					{
						// FileSD which will be used to register discover dynamically q.
						Files:           []string{filepath.Join(e2e.ContainerSharedDir, queryTargetsSubDir, "*.yaml")},
						RefreshInterval: model.Duration(time.Second),
					},
				},
				Scheme: "http",
			},
		},
	})
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(r))

	q, err := e2ethanos.NewQuerier(s.SharedDir(), "1", []string{r.GRPCNetworkEndpoint()}, nil, nil, nil, nil, "", "")
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	t.Run("no query configured", func(t *testing.T) {
		// Check for a few evaluations, check all of them failed.
		testutil.Ok(t, r.WaitSumMetrics(e2e.Greater(10), "prometheus_rule_evaluations_total"))
		testutil.Ok(t, r.WaitSumMetrics(e2e.EqualsAmongTwo, "prometheus_rule_evaluations_total", "prometheus_rule_evaluation_failures_total"))

		// No alerts sent.
		testutil.Ok(t, r.WaitSumMetrics(e2e.Equals(0), "thanos_alert_sender_alerts_dropped_total"))
	})

	var currentFailures float64
	t.Run("attach query", func(t *testing.T) {
		// Attach querier to target files.
		writeTargets(t, filepath.Join(s.SharedDir(), queryTargetsSubDir, "targets.yaml"), q.NetworkHTTPEndpoint())

		testutil.Ok(t, r.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"thanos_rule_query_apis_dns_provider_results"}, e2e.WaitMissingMetrics))
		testutil.Ok(t, r.WaitSumMetrics(e2e.Equals(1), "thanos_rule_alertmanagers_dns_provider_results"))

		var currentVal float64
		testutil.Ok(t, r.WaitSumMetrics(func(sums ...float64) bool {
			currentVal = sums[0]
			currentFailures = sums[1]
			return true
		}, "prometheus_rule_evaluations_total", "prometheus_rule_evaluation_failures_total"))

		// Check for a few evaluations, check all of them failed.
		testutil.Ok(t, r.WaitSumMetrics(e2e.Greater(currentVal+4), "prometheus_rule_evaluations_total"))
		// No failures.
		testutil.Ok(t, r.WaitSumMetrics(e2e.Equals(currentFailures), "prometheus_rule_evaluation_failures_total"))

		// Alerts sent.
		testutil.Ok(t, r.WaitSumMetrics(e2e.Equals(0), "thanos_alert_sender_alerts_dropped_total"))
		testutil.Ok(t, r.WaitSumMetrics(e2e.Greater(4), "thanos_alert_sender_alerts_sent_total"))

		// Alerts received.
		testutil.Ok(t, am2.WaitSumMetrics(e2e.Equals(2), "alertmanager_alerts"))
		testutil.Ok(t, am2.WaitSumMetrics(e2e.Greater(4), "alertmanager_alerts_received_total"))
		testutil.Ok(t, am2.WaitSumMetrics(e2e.Equals(0), "alertmanager_alerts_invalid_total"))

		// am1 not connected, so should not receive anything.
		testutil.Ok(t, am1.WaitSumMetrics(e2e.Equals(0), "alertmanager_alerts"))
		testutil.Ok(t, am1.WaitSumMetrics(e2e.Equals(0), "alertmanager_alerts_received_total"))
		testutil.Ok(t, am1.WaitSumMetrics(e2e.Equals(0), "alertmanager_alerts_invalid_total"))
	})
	t.Run("attach am1", func(t *testing.T) {
		// Attach am1 to target files.
		writeTargets(t, filepath.Join(s.SharedDir(), amTargetsSubDir, "targets.yaml"), am1.NetworkHTTPEndpoint())

		testutil.Ok(t, r.WaitSumMetrics(e2e.Equals(1), "thanos_rule_query_apis_dns_provider_results"))
		testutil.Ok(t, r.WaitSumMetrics(e2e.Equals(2), "thanos_rule_alertmanagers_dns_provider_results"))

		testutil.Ok(t, r.WaitSumMetrics(e2e.Equals(currentFailures), "prometheus_rule_evaluation_failures_total"))

		var currentVal float64
		testutil.Ok(t, am2.WaitSumMetrics(func(sums ...float64) bool {
			currentVal = sums[0]
			return true
		}, "alertmanager_alerts_received_total"))

		// Alerts received by both am1 and am2.
		testutil.Ok(t, am2.WaitSumMetrics(e2e.Equals(2), "alertmanager_alerts"))
		testutil.Ok(t, am2.WaitSumMetrics(e2e.Greater(currentVal+4), "alertmanager_alerts_received_total"))
		testutil.Ok(t, am2.WaitSumMetrics(e2e.Equals(0), "alertmanager_alerts_invalid_total"))

		testutil.Ok(t, am1.WaitSumMetrics(e2e.Equals(2), "alertmanager_alerts"))
		testutil.Ok(t, am1.WaitSumMetrics(e2e.Greater(4), "alertmanager_alerts_received_total"))
		testutil.Ok(t, am1.WaitSumMetrics(e2e.Equals(0), "alertmanager_alerts_invalid_total"))
	})

	t.Run("am1 drops again", func(t *testing.T) {
		testutil.Ok(t, os.RemoveAll(filepath.Join(s.SharedDir(), amTargetsSubDir, "targets.yaml")))

		testutil.Ok(t, r.WaitSumMetrics(e2e.Equals(1), "thanos_rule_query_apis_dns_provider_results"))
		testutil.Ok(t, r.WaitSumMetrics(e2e.Equals(1), "thanos_rule_alertmanagers_dns_provider_results"))
		testutil.Ok(t, r.WaitSumMetrics(e2e.Equals(currentFailures), "prometheus_rule_evaluation_failures_total"))

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
		testutil.Ok(t, am2.WaitSumMetrics(e2e.Equals(2), "alertmanager_alerts"))
		testutil.Ok(t, am2.WaitSumMetrics(e2e.Greater(currentValAm2+4), "alertmanager_alerts_received_total"))
		testutil.Ok(t, am2.WaitSumMetrics(e2e.Equals(0), "alertmanager_alerts_invalid_total"))

		// Am1 should not receive more alerts.
		testutil.Ok(t, am1.WaitSumMetrics(e2e.Equals(currentValAm1), "alertmanager_alerts_received_total"))
	})

	t.Run("duplicate am ", func(t *testing.T) {
		// am2 is already registered in static addresses.
		writeTargets(t, filepath.Join(s.SharedDir(), amTargetsSubDir, "targets.yaml"), am2.NetworkHTTPEndpoint())

		testutil.Ok(t, r.WaitSumMetrics(e2e.Equals(1), "thanos_rule_query_apis_dns_provider_results"))
		testutil.Ok(t, r.WaitSumMetrics(e2e.Equals(1), "thanos_rule_alertmanagers_dns_provider_results"))
	})

	t.Run("rule groups have last evaluation and evaluation duration set", func(t *testing.T) {
		rulegroupCorrectData(t, ctx, r.HTTPEndpoint())
	})

	t.Run("reload works", func(t *testing.T) {
		// Add a new rule via /-/reload.
		// TODO(GiedriusS): add a test for reloading via SIGHUP. Need to extend e2e framework to expose PIDs.

		createRuleFile(t, fmt.Sprintf("%s/newrule.yaml", rulesPath), testAlertRuleAddedLaterWebHandler)
		reloadRulesHTTP(t, ctx, r.HTTPEndpoint())
	})

	queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), "ALERTS", promclient.QueryOptions{
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

	alrts, err := promclient.NewDefaultClient().AlertmanagerAlerts(ctx, mustURLParse(t, "http://"+am2.HTTPEndpoint()))
	testutil.Ok(t, err)

	testutil.Equals(t, len(expAlertLabels), len(alrts))
	for i, a := range alrts {
		testutil.Assert(t, a.Labels.Equal(expAlertLabels[i]), "unexpected labels %s", a.Labels)
	}
}

// Test Ruler behavior on different storepb.PartialResponseStrategy when having partial response from single `failingStoreAPI`.
func TestRulePartialResponse(t *testing.T) {
	t.Skip("TODO: Allow HTTP ports from binaries running on host to be accessible.")

	// TODO: Implement with failing store.
}
