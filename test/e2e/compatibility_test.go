// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
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
	receiverRunnable := e2ethanos.NewReceiveBuilder(e, "receive").WithIngestionEnabled().Init()
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
