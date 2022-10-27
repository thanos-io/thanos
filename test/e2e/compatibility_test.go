// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/efficientgo/e2e/monitoring/promconfig"
	sdconfig "github.com/efficientgo/e2e/monitoring/promconfig/discovery/config"
	"github.com/efficientgo/e2e/monitoring/promconfig/discovery/targetgroup"
	e2eobs "github.com/efficientgo/e2e/observable"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"

	"github.com/thanos-io/thanos/pkg/alert"
	"github.com/thanos-io/thanos/pkg/httpconfig"
	"github.com/thanos-io/thanos/pkg/queryfrontend"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

// TestPromQLCompliance tests PromQL compatibility against https://github.com/prometheus/compliance/tree/main/promql.
// NOTE: This requires dockerization of compliance framework: https://github.com/prometheus/compliance/pull/46
// Test requires at least ~11m, so run this with `-test.timeout 9999m`.
func TestPromQLCompliance(t *testing.T) {
	testPromQLCompliance(t, false, store.EagerRetrieval)
}

// TestPromQLComplianceWithLazy tests PromQL compatibility against https://github.com/prometheus/compliance/tree/main/promql.
// NOTE: This requires dockerization of compliance framework: https://github.com/prometheus/compliance/pull/46
// Test requires at least ~11m, so run this with `-test.timeout 9999m`.
// This uses lazy evaluation to test out how it works in comparison to eager.
func TestPromQLComplianceWithLazy(t *testing.T) {
	testPromQLCompliance(t, false, store.LazyRetrieval)
}

// TestPromQLComplianceWithQueryFrontend tests PromQL compatibility with query frontend with sharding enabled.
func TestPromQLComplianceWithShardingQueryFrontend(t *testing.T) {
	testPromQLCompliance(t, true, store.EagerRetrieval)
}

func testPromQLCompliance(t *testing.T, queryFrontend bool, retrievalStrategy store.RetrievalStrategy) {
	t.Skip("This is interactive test, it requires time to build up (scrape) the data. The data is also obtain from remote promlab servers.")

	e, err := e2e.NewDockerEnvironment("compatibility")
	testutil.Ok(t, err)
	t.Cleanup(e.Close)

	// Start receive + Querier.
	receiverRunnable := e2ethanos.NewReceiveBuilder(e, "receive").WithIngestionEnabled().Init()
	queryReceive := e2edb.NewThanosQuerier(e, "query_receive", []string{receiverRunnable.InternalEndpoint("grpc")})
	testutil.Ok(t, e2e.StartAndWaitReady(receiverRunnable, queryReceive))

	rwURL, err := url.Parse(e2ethanos.RemoteWriteEndpoint(receiverRunnable.InternalEndpoint("remote-write")))
	testutil.Ok(t, err)
	// Start reference Prometheus.
	prom := e2edb.NewPrometheus(e, "prom")
	testutil.Ok(t, prom.SetConfig(promconfig.Config{
		GlobalConfig: promconfig.GlobalConfig{
			EvaluationInterval: model.Duration(5 * time.Second),
			ScrapeInterval:     model.Duration(5 * time.Second),
			ExternalLabels: map[model.LabelName]model.LabelValue{
				"prometheus": "1",
			},
		},
		RemoteWriteConfigs: []*promconfig.RemoteWriteConfig{
			{
				URL: &config_util.URL{URL: rwURL},
			},
		},
		ScrapeConfigs: []*promconfig.ScrapeConfig{
			{
				JobName: "demo",
				ServiceDiscoveryConfig: sdconfig.ServiceDiscoveryConfig{
					StaticConfigs: []*targetgroup.Group{
						{
							Source: "demo.promlabs.com:10000",
						},
						{
							Source: "demo.promlabs.com:10001",
						},
						{
							Source: "demo.promlabs.com:10002",
						},
					},
				},
			},
		},
	}))
	testutil.Ok(t, e2e.StartAndWaitReady(prom))

	// Start sidecar + Querier
	sidecar := e2edb.NewThanosSidecar(e, "sidecar", prom, e2edb.WithImage("thanos"))
	extraOpts := []e2edb.Option{e2edb.WithImage("thanos"), e2edb.WithFlagOverride(map[string]string{"--grpc.proxy-strategy": string(retrievalStrategy)})}
	querySidecar := e2edb.NewThanosQuerier(e, "query_sidecar", []string{sidecar.InternalEndpoint("grpc")}, extraOpts...)
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
		queryTargetRunnable := queryReceive
		if queryFrontend {
			qf := newQueryFrontendRunnable(e, "query_frontend_receive", queryReceive.InternalEndpoint("http"))
			testutil.Ok(t, e2e.StartAndWaitReady(qf))
			queryTargetRunnable = qf
		}

		testutil.Ok(t, os.WriteFile(filepath.Join(compliance.Dir(), "receive.yaml"),
			[]byte(promQLCompatConfig(prom, queryTargetRunnable, []string{"prometheus", "receive", "tenant_id"})), os.ModePerm))

		testutil.Ok(t, compliance.Exec(e2e.NewCommand(
			"/promql-compliance-tester",
			"-config-file", filepath.Join(compliance.InternalDir(), "receive.yaml"),
			"-config-file", "/promql-test-queries.yml",
		)))
	})
	t.Run("sidecar", func(t *testing.T) {
		queryTargetRunnable := querySidecar
		if queryFrontend {
			qf := newQueryFrontendRunnable(e, "query_frontend_sidecar", queryReceive.InternalEndpoint("http"))
			testutil.Ok(t, e2e.StartAndWaitReady(qf))
			queryTargetRunnable = qf
		}

		testutil.Ok(t, os.WriteFile(filepath.Join(compliance.Dir(), "sidecar.yaml"),
			[]byte(promQLCompatConfig(prom, queryTargetRunnable, []string{"prometheus"})), os.ModePerm))

		testutil.Ok(t, compliance.Exec(e2e.NewCommand(
			"/promql-compliance-tester",
			"-config-file", filepath.Join(compliance.InternalDir(), "sidecar.yaml"),
			"-config-file", "/promql-test-queries.yml",
		)))
	})
}

// nolint (it's still used in skipped test).
func promQLCompatConfig(reference *e2emon.Prometheus, target e2e.Runnable, dropLabels []string) string {
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
	t.Skip("This is an interactive test, using https://github.com/prometheus/compliance/tree/main/alert_generator. This tool is not optimized for CI runs (e.g. it infinitely retries, takes 38 minutes)")

	t.Run("stateful ruler", func(t *testing.T) {
		e, err := e2e.NewDockerEnvironment("alert-compatibility")
		testutil.Ok(t, err)
		t.Cleanup(e.Close)

		// Start receive + Querier.
		receive := e2ethanos.NewReceiveBuilder(e, "receive").WithIngestionEnabled().Init()
		querierBuilder := e2ethanos.NewQuerierBuilder(e, "query")

		compliance := e.Runnable("alert_generator_compliance_tester").WithPorts(map[string]int{"http": 8080}).Init(e2e.StartOptions{
			Image:   "alert_generator_compliance_tester:latest",
			Command: e2e.NewCommandRunUntilStop(),
		})

		rFuture := e2ethanos.NewRulerBuilder(e, "1")
		ruler := rFuture.WithAlertManagerConfig([]alert.AlertmanagerConfig{
			{
				EndpointsConfig: httpconfig.EndpointsConfig{
					StaticAddresses: []string{compliance.InternalEndpoint("http")},
					Scheme:          "http",
				},
				Timeout:    amTimeout,
				APIVersion: alert.APIv1,
			},
		}).
			// Use default resend delay and eval interval, as the compliance spec requires this.
			WithResendDelay("1m").
			WithEvalInterval("1m").
			WithReplicaLabel("").
			InitTSDB(filepath.Join(rFuture.InternalDir(), "rules"), []httpconfig.Config{
				{
					EndpointsConfig: httpconfig.EndpointsConfig{
						StaticAddresses: []string{
							querierBuilder.InternalEndpoint("http"),
						},
						Scheme: "http",
					},
				},
			})

		query := querierBuilder.
			WithStoreAddresses(receive.InternalEndpoint("grpc")).
			WithRuleAddresses(ruler.InternalEndpoint("grpc")).
			// We deduplicate by this, since alert compatibility tool requires clean metric without labels
			// attached by receivers.
			WithReplicaLabels("receive", "tenant_id").
			Init()
		testutil.Ok(t, e2e.StartAndWaitReady(receive, query, ruler, compliance))

		// Pull rules.yaml:
		{
			var stdout bytes.Buffer
			testutil.Ok(t, compliance.Exec(e2e.NewCommand("cat", "/rules.yaml"), e2e.WithExecOptionStdout(&stdout)))
			testutil.Ok(t, os.MkdirAll(filepath.Join(ruler.Dir(), "rules"), os.ModePerm))
			testutil.Ok(t, os.WriteFile(filepath.Join(ruler.Dir(), "rules", "rules.yaml"), stdout.Bytes(), os.ModePerm))

			// Reload ruler.
			resp, err := http.Post("http://"+ruler.Endpoint("http")+"/-/reload", "", nil)
			testutil.Ok(t, err)
			defer func() {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}()
			testutil.Equals(t, http.StatusOK, resp.StatusCode)
		}
		testutil.Ok(t, os.WriteFile(filepath.Join(compliance.Dir(), "test-thanos.yaml"), []byte(alertCompatConfig(receive, query)), os.ModePerm))

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
`, e2ethanos.RemoteWriteEndpoint(receive.InternalEndpoint("remote-write")), query.InternalEndpoint("http"), query.InternalEndpoint("http"))
}

func newQueryFrontendRunnable(e e2e.Environment, name, downstreamURL string) *e2eobs.Observable {
	inMemoryCacheConfig := queryfrontend.CacheProviderConfig{
		Type: queryfrontend.INMEMORY,
		Config: queryfrontend.InMemoryResponseCacheConfig{
			MaxSizeItems: 1000,
			Validity:     time.Hour,
		},
	}
	config := queryfrontend.Config{
		QueryRangeConfig: queryfrontend.QueryRangeConfig{
			AlignRangeWithStep: false,
		},
		NumShards: 3,
	}
	return e2ethanos.NewQueryFrontend(e, name, downstreamURL, config, inMemoryCacheConfig)
}
