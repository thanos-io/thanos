// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	common_cfg "github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/config"
	"github.com/thanos-io/thanos/pkg/alert"
	"github.com/thanos-io/thanos/pkg/httpconfig"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

// TestPromQLCompliance tests PromQL compatibility against https://github.com/prometheus/compliance/tree/main/promql.
// NOTE: This requires dockerization of compliance framework: https://github.com/prometheus/compliance/pull/46
// Test requires at least ~11m, so run this with `-test.timeout 9999m`.
func TestPromQLCompliance(t *testing.T) {
	t.Skip("This is interactive test, it requires time to build up (scrape) the data. The data is also obtain from remote promlab servers.")

	e, err := e2e.NewDockerEnvironment("compatibility")
	testutil.Ok(t, err)
	t.Cleanup(e.Close)

	// Start receive + Querier.
	receiverRunnable := e2ethanos.NewIngestingReceiver(e, "receive")
	queryReceive := e2edb.NewThanosQuerier(e, "query_receive", []string{receiverRunnable.InternalEndpoint("grpc")})
	testutil.Ok(t, e2e.StartAndWaitReady(receiverRunnable, queryReceive))

	// Start reference Prometheus.
	prom := e2edb.NewPrometheus(e, "prom")
	testutil.Ok(t, prom.SetConfig(`
global:
  scrape_interval:     5s
  evaluation_interval: 5s
  external_labels:
    prometheus: 1

remote_write:
  - url: "`+e2ethanos.RemoteWriteEndpoint(receiverRunnable.InternalEndpoint("remote-write"))+`"

scrape_configs:
- job_name: 'demo'
  static_configs:
    - targets:
      - 'demo.promlabs.com:10000'
      - 'demo.promlabs.com:10001'
      - 'demo.promlabs.com:10002'
`,
	))
	testutil.Ok(t, e2e.StartAndWaitReady(prom))

	// Start sidecar + Querier
	sidecar := e2edb.NewThanosSidecar(e, "sidecar", prom, e2edb.WithImage("thanos"))
	querySidecar := e2edb.NewThanosQuerier(e, "query_sidecar", []string{sidecar.InternalEndpoint("grpc")}, e2edb.WithImage("thanos"))
	testutil.Ok(t, e2e.StartAndWaitReady(sidecar, querySidecar))

	// Start noop promql-compliance-tester. See https://github.com/prometheus/compliance/tree/main/promql on how to build local docker image.
	compliance := e.Runnable("promql-compliance-tester").Init(e2e.StartOptions{
		Image:   "promql-compliance-tester:latest",
		Command: e2e.NewCommandWithoutEntrypoint("tail", "-f", "/dev/null"),
	})
	testutil.Ok(t, e2e.StartAndWaitReady(compliance))

	// Wait 10 minutes for Prometheus to scrape relevant data.
	time.Sleep(10 * time.Minute)

	t.Run("receive", func(t *testing.T) {
		testutil.Ok(t, ioutil.WriteFile(filepath.Join(compliance.Dir(), "receive.yaml"),
			[]byte(promQLCompatConfig(prom, queryReceive, []string{"prometheus", "receive", "tenant_id"})), os.ModePerm))

		testutil.Ok(t, compliance.Exec(e2e.NewCommand(
			"/promql-compliance-tester",
			"-config-file", filepath.Join(compliance.InternalDir(), "receive.yaml"),
			"-config-file", "/promql-test-queries.yml",
		)))
	})
	t.Run("sidecar", func(t *testing.T) {
		testutil.Ok(t, ioutil.WriteFile(filepath.Join(compliance.Dir(), "sidecar.yaml"),
			[]byte(promQLCompatConfig(prom, querySidecar, []string{"prometheus"})), os.ModePerm))

		testutil.Ok(t, compliance.Exec(e2e.NewCommand(
			"/promql-compliance-tester",
			"-config-file", filepath.Join(compliance.InternalDir(), "sidecar.yaml"),
			"-config-file", "/promql-test-queries.yml",
		)))
	})
}

// nolint (it's still used in skipped test).
func promQLCompatConfig(reference *e2edb.Prometheus, target e2e.Runnable, dropLabels []string) string {
	return `reference_target_config:
  query_url: 'http://` + reference.InternalEndpoint("http") + `'

test_target_config:
  query_url: 'http://` + target.InternalEndpoint("http") + `'

query_tweaks:
  - note: 'Thanos requires adding "external_labels" to distinguish Prometheus servers, leading to extra labels in query results that need to be stripped before comparing results.'
    no_bug: true
    drop_result_labels:
` + func() (ret string) {
		for _, l := range dropLabels {
			ret += `      - ` + l + "\n"
		}
		return ret
	}()
}

// TestAlertCompliance tests Alert compatibility against https://github.com/prometheus/compliance/blob/main/alert_generator.
// NOTE: This requires a dockerization of compliance framework: https://github.com/prometheus/compliance/pull/46
func TestAlertCompliance(t *testing.T) {
	//t.Skip("This is an interactive test, using https://github.com/prometheus/compliance/tree/main/alert_generator. This tool is not optimized for CI runs (e.g. it infinitely retries")

	t.Run("staleness ruler", func(t *testing.T) {
		e, err := e2e.NewDockerEnvironment("alert_compatibility")
		testutil.Ok(t, err)
		t.Cleanup(e.Close)

		// Start receive + Querier.
		receive := e2ethanos.NewIngestingReceiver(e, "receive")
		querierBuilder := e2ethanos.NewQuerierBuilder(e, "query", receive.InternalEndpoint("grpc"))

		compliance := e.Runnable("alert_generator_compliance_tester").WithPorts(map[string]int{"http": 8080}).Init(e2e.StartOptions{
			Image:   "alert_generator_compliance_tester:latest",
			Command: e2e.NewCommandRunUntilStop(),
		})
		ruler := e2ethanos.NewStatelessRuler(e, "1", "rules", []alert.AlertmanagerConfig{
			{
				EndpointsConfig: httpconfig.EndpointsConfig{
					StaticAddresses: []string{compliance.InternalEndpoint("http")},
					Scheme:          "http",
				},
				Timeout:    amTimeout,
				APIVersion: alert.APIv1,
			},
		}, []httpconfig.Config{
			{
				EndpointsConfig: httpconfig.EndpointsConfig{
					StaticAddresses: []string{
						querierBuilder.Future().InternalEndpoint("http"),
					},
					Scheme: "http",
				},
			},
		}, []*config.RemoteWriteConfig{
			{URL: &common_cfg.URL{URL: urlParse(t, e2ethanos.RemoteWriteEndpoint(receive.InternalEndpoint("remote-write")))}, Name: "thanos-receiver"},
		})
		query := querierBuilder.WithRuleAddresses(ruler.InternalEndpoint("grpc")).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(receive, query, ruler, compliance))

		// Pull rules.yaml:
		{
			var stdout bytes.Buffer
			testutil.Ok(t, compliance.Exec(e2e.NewCommand("cat", "/rules.yaml"), e2e.WithExecOptionStdout(&stdout)))
			testutil.Ok(t, os.MkdirAll(filepath.Join(ruler.Dir(), "rules"), os.ModePerm))
			testutil.Ok(t, os.WriteFile(filepath.Join(ruler.Dir(), "rules", "rules.yaml"), stdout.Bytes(), os.ModePerm))
		}

		testutil.Ok(t, ioutil.WriteFile(filepath.Join(compliance.Dir(), "test-thanos.yaml"), []byte(alertCompatConfig(receive, query)), os.ModePerm))

		fmt.Println(alertCompatConfig(receive, query))

		testutil.Ok(t, compliance.Exec(e2e.NewCommand(
			"/alert_generator_compliance_tester", "-config-file", filepath.Join(compliance.InternalDir(), "test-thanos.yaml")),
		))
	})
}

// nolint (it's still used in skipped test).
func alertCompatConfig(receive e2e.Runnable, query e2e.Runnable) string {
	return fmt.Sprintf(`settings:
  remote_write_url: '%s'
  query_base_url: 'http://%s'
  rules_and_alerts_api_base_url: 'http://%s'
  alert_reception_server_port: 8080
  alert_message_parser: default
`, e2ethanos.RemoteWriteEndpoint(receive.InternalEndpoint("http")), query.InternalEndpoint("http"), query.InternalEndpoint("http"))
}
