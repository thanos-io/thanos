// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	"github.com/efficientgo/e2e"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/rules"

	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

// NOTE: by using aggregation all results are now unsorted.
const queryUpWithoutInstance = "sum(up) without (instance)"

// defaultPromConfig returns Prometheus config that sets Prometheus to:
// * expose 2 external labels, source and replica.
// * scrape fake target. This will produce up == 0 metric which we can assert on.
// * optionally remote write endpoint to write into.
func defaultPromConfig(name string, replica int, remoteWriteEndpoint, ruleFile string, scrapeTargets ...string) string {
	targets := "localhost:9090"
	if len(scrapeTargets) > 0 {
		targets = strings.Join(scrapeTargets, ",")
	}
	config := fmt.Sprintf(`
global:
  external_labels:
    prometheus: %v
    replica: %v
scrape_configs:
- job_name: 'myself'
  # Quick scrapes for test purposes.
  scrape_interval: 1s
  scrape_timeout: 1s
  static_configs:
  - targets: [%s]
  relabel_configs:
  - source_labels: ['__address__']
    regex: '^.+:80$'
    action: drop
`, name, replica, targets)

	if remoteWriteEndpoint != "" {
		config = fmt.Sprintf(`
%s
remote_write:
- url: "%s"
  # Don't spam receiver on mistake.
  queue_config:
    min_backoff: 2s
    max_backoff: 10s
`, config, remoteWriteEndpoint)
	}

	if ruleFile != "" {
		config = fmt.Sprintf(`
%s
rule_files:
-  "%s"
`, config, ruleFile)
	}

	return config
}

func defaultWebConfig() string {
	// username: test, secret: test(bcrypt hash)
	return `
basic_auth_users:
  test: $2y$10$IsC9GG9U61sPCuDwwwcnPuMRyzx62cIcdNRs4SIdKwgWihfX4IC.C
`
}

func sortResults(res model.Vector) {
	sort.Slice(res, func(i, j int) bool {
		return res[i].String() < res[j].String()
	})
}

func TestQuery(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_query")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	receiver := e2ethanos.NewUninitiatedReceiver(e, "1")
	receiverRunnable, err := e2ethanos.NewRoutingAndIngestingReceiverFromService(receiver, e.SharedDir(), 1)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(receiverRunnable))

	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(e, "alone", defaultPromConfig("prom-alone", 0, "", ""), "", e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(e, "remote-and-sidecar", defaultPromConfig("prom-both-remote-write-and-sidecar", 1234, e2ethanos.RemoteWriteEndpoint(receiver.InternalEndpoint("remote-write")), ""), "", e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	prom3, sidecar3, err := e2ethanos.NewPrometheusWithSidecar(e, "ha1", defaultPromConfig("prom-ha", 0, "", filepath.Join(e2ethanos.ContainerSharedDir, "", "*.yaml")), "", e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	prom4, sidecar4, err := e2ethanos.NewPrometheusWithSidecar(e, "ha2", defaultPromConfig("prom-ha", 1, "", filepath.Join(e2ethanos.ContainerSharedDir, "", "*.yaml")), "", e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2, prom3, sidecar3, prom4, sidecar4))

	// Querier. Both fileSD and directly by flags.
	q, err := e2ethanos.NewQuerierBuilder(e, "1", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc"), receiver.InternalEndpoint("grpc")).
		WithFileSDStoreAddresses(sidecar3.InternalEndpoint("grpc"), sidecar4.InternalEndpoint("grpc")).Build()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(5), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics()))

	queryAndAssertSeries(t, ctx, q.Endpoint("http"), queryUpWithoutInstance, time.Now, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "prom-alone",
			"replica":    "0",
		},
		{
			"job":        "myself",
			"prometheus": "prom-both-remote-write-and-sidecar",
			"receive":    "receive-1",
			"replica":    "1234",
			"tenant_id":  "default-tenant",
		},
		{
			"job":        "myself",
			"prometheus": "prom-both-remote-write-and-sidecar",
			"replica":    "1234",
		},
		{
			"job":        "myself",
			"prometheus": "prom-ha",
			"replica":    "0",
		},
		{
			"job":        "myself",
			"prometheus": "prom-ha",
			"replica":    "1",
		},
	})

	// With deduplication.
	queryAndAssertSeries(t, ctx, q.Endpoint("http"), queryUpWithoutInstance, time.Now, promclient.QueryOptions{
		Deduplicate: true,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "prom-alone",
		},
		{
			"job":        "myself",
			"prometheus": "prom-both-remote-write-and-sidecar",
			"receive":    "receive-1",
			"tenant_id":  "default-tenant",
		},
		{
			"job":        "myself",
			"prometheus": "prom-both-remote-write-and-sidecar",
		},
		{
			"job":        "myself",
			"prometheus": "prom-ha",
		},
	})
}

func TestQueryExternalPrefixWithoutReverseProxy(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_query_route_prefix")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	externalPrefix := "test"

	q, err := e2ethanos.NewQuerierBuilder(e, "1").
		WithExternalPrefix(externalPrefix).Build()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	checkNetworkRequests(t, "http://"+q.Endpoint("http")+"/"+externalPrefix+"/graph")
}

func TestQueryExternalPrefix(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_query_external_prefix")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	externalPrefix := "thanos"

	q, err := e2ethanos.NewQuerierBuilder(e, "1").
		WithExternalPrefix(externalPrefix).Build()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	querierURL := mustURLParse(t, "http://"+q.Endpoint("http")+"/"+externalPrefix)

	querierProxy := httptest.NewServer(e2ethanos.NewSingleHostReverseProxy(querierURL, externalPrefix))
	t.Cleanup(querierProxy.Close)

	checkNetworkRequests(t, querierProxy.URL+"/"+externalPrefix+"/graph")
}

func TestQueryExternalPrefixAndRoutePrefix(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_query_external_prefix_and_route_prefix")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	externalPrefix := "thanos"
	routePrefix := "test"

	q, err := e2ethanos.NewQuerierBuilder(e, "1").
		WithRoutePrefix(routePrefix).
		WithExternalPrefix(externalPrefix).
		Build()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	querierURL := mustURLParse(t, "http://"+q.Endpoint("http")+"/"+routePrefix)

	querierProxy := httptest.NewServer(e2ethanos.NewSingleHostReverseProxy(querierURL, externalPrefix))
	t.Cleanup(querierProxy.Close)

	checkNetworkRequests(t, querierProxy.URL+"/"+externalPrefix+"/graph")
}

func TestQueryLabelNames(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_query_label_names")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	receiver := e2ethanos.NewUninitiatedReceiver(e, "1")
	receiverRunnable, err := e2ethanos.NewRoutingAndIngestingReceiverFromService(receiver, e.SharedDir(), 1)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(receiverRunnable))

	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(e, "alone", defaultPromConfig("prom-alone", 0, "", ""), "", e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(e, "remote-and-sidecar", defaultPromConfig("prom-both-remote-write-and-sidecar", 1234, e2ethanos.RemoteWriteEndpoint(receiver.InternalEndpoint("remote-write")), ""), "", e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	q, err := e2ethanos.NewQuerierBuilder(e, "1", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc"), receiver.InternalEndpoint("grpc")).Build()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	now := time.Now()
	labelNames(t, ctx, q.Endpoint("http"), nil, timestamp.FromTime(now.Add(-time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
		return len(res) > 0
	})

	// Outside time range.
	labelNames(t, ctx, q.Endpoint("http"), nil, timestamp.FromTime(now.Add(-24*time.Hour)), timestamp.FromTime(now.Add(-23*time.Hour)), func(res []string) bool {
		return len(res) == 0
	})

	labelNames(t, ctx, q.Endpoint("http"), []*labels.Matcher{{Type: labels.MatchEqual, Name: "__name__", Value: "up"}},
		timestamp.FromTime(now.Add(-time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
			// Expected result: [__name__, instance, job, prometheus, replica, receive, tenant_id]
			// Pre-labelnames pushdown we've done Select() over all series and picked out the label names hence they all had external labels.
			// With labelnames pushdown we had to extend the LabelNames() call to enrich the response with the external labelset when there is more than one label.
			return len(res) == 7
		},
	)

	// There is no matched series.
	labelNames(t, ctx, q.Endpoint("http"), []*labels.Matcher{{Type: labels.MatchEqual, Name: "__name__", Value: "foobar"}},
		timestamp.FromTime(now.Add(-time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
			return len(res) == 0
		},
	)
}

func TestQueryLabelValues(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_query_label_values")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	receiver := e2ethanos.NewUninitiatedReceiver(e, "1")
	receiverRunnable, err := e2ethanos.NewRoutingAndIngestingReceiverFromService(receiver, e.SharedDir(), 1)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(receiverRunnable))

	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(e, "alone", defaultPromConfig("prom-alone", 0, "", ""), "", e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(e, "remote-and-sidecar", defaultPromConfig("prom-both-remote-write-and-sidecar", 1234, e2ethanos.RemoteWriteEndpoint(receiver.InternalEndpoint("remote-write")), ""), "", e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	q, err := e2ethanos.NewQuerierBuilder(e, "1", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc"), receiver.InternalEndpoint("grpc")).Build()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	now := time.Now()
	labelValues(t, ctx, q.Endpoint("http"), "instance", nil, timestamp.FromTime(now.Add(-time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
		return len(res) == 1 && res[0] == "localhost:9090"
	})

	// Outside time range.
	labelValues(t, ctx, q.Endpoint("http"), "instance", nil, timestamp.FromTime(now.Add(-24*time.Hour)), timestamp.FromTime(now.Add(-23*time.Hour)), func(res []string) bool {
		return len(res) == 0
	})

	labelValues(t, ctx, q.Endpoint("http"), "__name__", []*labels.Matcher{{Type: labels.MatchEqual, Name: "__name__", Value: "up"}},
		timestamp.FromTime(now.Add(-time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
			return len(res) == 1 && res[0] == "up"
		},
	)

	labelValues(t, ctx, q.Endpoint("http"), "__name__", []*labels.Matcher{{Type: labels.MatchEqual, Name: "__name__", Value: "foobar"}},
		timestamp.FromTime(now.Add(-time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
			return len(res) == 0
		},
	)
}

func TestQueryWithAuthorizedSidecar(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_query_authorized_sidecar")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom, sidecar, err := e2ethanos.NewPrometheusWithSidecar(e, "alone", defaultPromConfig("prom-alone", 0, "", ""), defaultWebConfig(), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	q, err := e2ethanos.NewQuerierBuilder(e, "1", []string{sidecar.InternalEndpoint("grpc")}...).Build()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics()))

	queryAndAssertSeries(t, ctx, q.Endpoint("http"), queryUpWithoutInstance, time.Now, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "prom-alone",
			"replica":    "0",
		},
	})
}

func TestQueryCompatibilityWithPreInfoAPI(t *testing.T) {
	t.Parallel()

	for i, tcase := range []struct {
		queryImage   string
		sidecarImage string
	}{
		{
			queryImage:   e2ethanos.DefaultImage(),
			sidecarImage: "quay.io/thanos/thanos:v0.22.0", // Thanos components from version before 0.23 does not have new InfoAPI.
		},
		{
			queryImage:   "quay.io/thanos/thanos:v0.22.0", // Thanos querier from version before 0.23 did not know about InfoAPI.
			sidecarImage: e2ethanos.DefaultImage(),
		},
	} {
		i := i
		t.Run(fmt.Sprintf("%+v", tcase), func(t *testing.T) {
			e, err := e2e.NewDockerEnvironment(fmt.Sprintf("e2e_test_query_comp_query_%d", i))
			testutil.Ok(t, err)
			t.Cleanup(e2ethanos.CleanScenario(t, e))

			promRulesSubDir := filepath.Join("rules")
			testutil.Ok(t, os.MkdirAll(filepath.Join(e.SharedDir(), promRulesSubDir), os.ModePerm))
			// Create the abort_on_partial_response alert for Prometheus.
			// We don't create the warn_on_partial_response alert as Prometheus has strict yaml unmarshalling.
			createRuleFile(t, filepath.Join(e.SharedDir(), promRulesSubDir, "rules.yaml"), testAlertRuleAbortOnPartialResponse)

			qBuilder := e2ethanos.NewQuerierBuilder(e, "1")
			qUninit := qBuilder.BuildUninitiated()

			p1, s1, err := e2ethanos.NewPrometheusWithSidecarCustomImage(
				e,
				"p1",
				defaultPromConfig("p1", 0, "", filepath.Join(e2ethanos.ContainerSharedDir, promRulesSubDir, "*.yaml"), "localhost:9090", qUninit.InternalEndpoint("http")),
				"",
				e2ethanos.DefaultPrometheusImage(),
				tcase.sidecarImage,
				e2ethanos.FeatureExemplarStorage,
			)
			testutil.Ok(t, err)
			testutil.Ok(t, e2e.StartAndWaitReady(p1, s1))

			// Newest querier with old --rules --meta etc flags.
			q, err := qBuilder.
				WithMetadataAddresses(s1.InternalEndpoint("grpc")).
				WithExemplarAddresses(s1.InternalEndpoint("grpc")).
				WithTargetAddresses(s1.InternalEndpoint("grpc")).
				WithRuleAddresses(s1.InternalEndpoint("grpc")).
				WithTracingConfig(fmt.Sprintf(`type: JAEGER
config:
  sampler_type: const
  sampler_param: 1
  service_name: %s`, qUninit.Name())). // Use fake tracing config to trigger exemplar.
				WithImage(tcase.queryImage).
				Initiate(qUninit, s1.InternalEndpoint("grpc"))
			testutil.Ok(t, err)
			testutil.Ok(t, e2e.StartAndWaitReady(q))

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			t.Cleanup(cancel)

			// We should have single TCP connection, since all APIs are against the same server.
			testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics()))

			queryAndAssertSeries(t, ctx, q.Endpoint("http"), queryUpWithoutInstance, time.Now, promclient.QueryOptions{
				Deduplicate: false,
			}, []model.Metric{
				{
					"job":        "myself",
					"prometheus": "p1",
					"replica":    "0",
				},
			})

			// We expect rule and other APIs to work.

			// Metadata.
			{
				var promMeta map[string][]metadatapb.Meta
				// Wait metadata response to be ready as Prometheus gets metadata after scrape.
				testutil.Ok(t, runutil.Retry(3*time.Second, ctx.Done(), func() error {
					promMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, mustURLParse(t, "http://"+p1.Endpoint("http")), "", -1)
					testutil.Ok(t, err)
					if len(promMeta) > 0 {
						return nil
					}
					return fmt.Errorf("empty metadata response from Prometheus")
				}))

				thanosMeta, err := promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.Endpoint("http")), "", -1)
				testutil.Ok(t, err)
				testutil.Assert(t, len(thanosMeta) > 0, "got empty metadata response from Thanos")

				// Metadata response from Prometheus and Thanos Querier should be the same after deduplication.
				metadataEqual(t, thanosMeta, promMeta)
			}

			// Exemplars.
			{
				now := time.Now()
				start := timestamp.FromTime(now.Add(-time.Hour))
				end := timestamp.FromTime(now.Add(time.Hour))

				// Send HTTP requests to thanos query to trigger exemplars.
				labelNames(t, ctx, q.Endpoint("http"), nil, start, end, func(res []string) bool {
					return true
				})

				queryExemplars(t, ctx, q.Endpoint("http"), `http_request_duration_seconds_bucket{handler="label_names"}`, start, end, exemplarsOnExpectedSeries(map[string]string{
					"__name__":   "http_request_duration_seconds_bucket",
					"handler":    "label_names",
					"job":        "myself",
					"method":     "get",
					"prometheus": "p1",
				}))
			}

			// Targets.
			{
				targetAndAssert(t, ctx, q.Endpoint("http"), "", &targetspb.TargetDiscovery{
					ActiveTargets: []*targetspb.ActiveTarget{
						{
							DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "__address__", Value: fmt.Sprintf("e2e_test_query_comp_query_%d-querier-1:8080", i)},
								{Name: "__metrics_path__", Value: "/metrics"},
								{Name: "__scheme__", Value: "http"},
								{Name: "job", Value: "myself"},
								{Name: "prometheus", Value: "p1"},
							}},
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "instance", Value: fmt.Sprintf("e2e_test_query_comp_query_%d-querier-1:8080", i)},
								{Name: "job", Value: "myself"},
								{Name: "prometheus", Value: "p1"},
							}},
							ScrapePool: "myself",
							ScrapeUrl:  fmt.Sprintf("http://e2e_test_query_comp_query_%d-querier-1:8080/metrics", i),
							Health:     targetspb.TargetHealth_UP,
						},
						{
							DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "__address__", Value: "localhost:9090"},
								{Name: "__metrics_path__", Value: "/metrics"},
								{Name: "__scheme__", Value: "http"},
								{Name: "job", Value: "myself"},
								{Name: "prometheus", Value: "p1"},
							}},
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "instance", Value: "localhost:9090"},
								{Name: "job", Value: "myself"},
								{Name: "prometheus", Value: "p1"},
							}},
							ScrapePool: "myself",
							ScrapeUrl:  "http://localhost:9090/metrics",
							Health:     targetspb.TargetHealth_UP,
						},
					},
					DroppedTargets: []*targetspb.DroppedTarget{},
				})
			}

			// Rules.
			{
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
									{Name: "prometheus", Value: "p1"},
									{Name: "severity", Value: "page"},
								}},
								Health: string(rules.HealthGood),
							}),
						},
					},
				})
			}
		})
	}
}

func checkNetworkRequests(t *testing.T, addr string) {
	ctx, cancel := chromedp.NewContext(context.Background())
	t.Cleanup(cancel)

	testutil.Ok(t, runutil.Retry(1*time.Minute, ctx.Done(), func() error {
		var networkErrors []string

		// Listen for failed network requests and push them to an array.
		chromedp.ListenTarget(ctx, func(ev interface{}) {
			switch ev := ev.(type) {
			case *network.EventLoadingFailed:
				networkErrors = append(networkErrors, ev.ErrorText)
			}
		})

		err := chromedp.Run(ctx,
			network.Enable(),
			chromedp.Navigate(addr),
			chromedp.WaitVisible(`body`),
		)

		if err != nil {
			return err
		}

		if len(networkErrors) > 0 {
			err = fmt.Errorf("some network requests failed: %s", strings.Join(networkErrors, "; "))
		}
		return err
	}))
}

func mustURLParse(t testing.TB, addr string) *url.URL {
	u, err := url.Parse(addr)
	testutil.Ok(t, err)

	return u
}

func instantQuery(t *testing.T, ctx context.Context, addr, q string, ts func() time.Time, opts promclient.QueryOptions, expectedSeriesLen int) model.Vector {
	t.Helper()

	fmt.Println("queryAndAssert: Waiting for", expectedSeriesLen, "results for query", q)
	var result model.Vector

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	testutil.Ok(t, runutil.RetryWithLog(logger, 5*time.Second, ctx.Done(), func() error {
		res, warnings, err := promclient.NewDefaultClient().QueryInstant(ctx, mustURLParse(t, "http://"+addr), q, ts(), opts)
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		if len(res) != expectedSeriesLen {
			return errors.Errorf("unexpected result size, expected %d; result %d: %v", expectedSeriesLen, len(res), res)
		}
		result = res
		return nil
	}))
	sortResults(result)
	return result
}

func queryAndAssertSeries(t *testing.T, ctx context.Context, addr, q string, ts func() time.Time, opts promclient.QueryOptions, expected []model.Metric) {
	t.Helper()

	result := instantQuery(t, ctx, addr, q, ts, opts, len(expected))
	for i, exp := range expected {
		testutil.Equals(t, exp, result[i].Metric)
	}
}

func queryAndAssert(t *testing.T, ctx context.Context, addr, q string, opts promclient.QueryOptions, expected model.Vector) {
	t.Helper()

	sortResults(expected)
	result := instantQuery(t, ctx, addr, q, time.Now, opts, len(expected))
	for _, r := range result {
		r.Timestamp = 0 // Does not matter for us.
	}
	testutil.Equals(t, expected, result)
}

func labelNames(t *testing.T, ctx context.Context, addr string, matchers []*labels.Matcher, start, end int64, check func(res []string) bool) {
	t.Helper()

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	testutil.Ok(t, runutil.RetryWithLog(logger, 2*time.Second, ctx.Done(), func() error {
		res, err := promclient.NewDefaultClient().LabelNamesInGRPC(ctx, mustURLParse(t, "http://"+addr), matchers, start, end)
		if err != nil {
			return err
		}
		if check(res) {
			return nil
		}

		return errors.Errorf("unexpected results %v", res)
	}))
}

//nolint:unparam
func labelValues(t *testing.T, ctx context.Context, addr, label string, matchers []*labels.Matcher, start, end int64, check func(res []string) bool) {
	t.Helper()

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	testutil.Ok(t, runutil.RetryWithLog(logger, 2*time.Second, ctx.Done(), func() error {
		res, err := promclient.NewDefaultClient().LabelValuesInGRPC(ctx, mustURLParse(t, "http://"+addr), label, matchers, start, end)
		if err != nil {
			return err
		}
		if check(res) {
			return nil
		}

		return errors.Errorf("unexpected results %v", res)
	}))
}

func series(t *testing.T, ctx context.Context, addr string, matchers []*labels.Matcher, start, end int64, check func(res []map[string]string) bool) {
	t.Helper()

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	testutil.Ok(t, runutil.RetryWithLog(logger, 2*time.Second, ctx.Done(), func() error {
		res, err := promclient.NewDefaultClient().SeriesInGRPC(ctx, mustURLParse(t, "http://"+addr), matchers, start, end)
		if err != nil {
			return err
		}
		if check(res) {
			return nil
		}

		return errors.Errorf("unexpected results %v", res)
	}))
}

//nolint:unparam
func rangeQuery(t *testing.T, ctx context.Context, addr, q string, start, end, step int64, opts promclient.QueryOptions, check func(res model.Matrix) error) {
	t.Helper()

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	testutil.Ok(t, runutil.RetryWithLog(logger, time.Second, ctx.Done(), func() error {
		res, warnings, err := promclient.NewDefaultClient().QueryRange(ctx, mustURLParse(t, "http://"+addr), q, start, end, step, opts)
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		if err := check(res); err != nil {
			return errors.Wrap(err, "result check failed")
		}

		return nil
	}))
}

func queryExemplars(t *testing.T, ctx context.Context, addr, q string, start, end int64, check func(data []*exemplarspb.ExemplarData) error) {
	t.Helper()

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	u := mustURLParse(t, "http://"+addr)
	testutil.Ok(t, runutil.RetryWithLog(logger, time.Second, ctx.Done(), func() error {
		res, err := promclient.NewDefaultClient().ExemplarsInGRPC(ctx, u, q, start, end)
		if err != nil {
			return err
		}

		if err := check(res); err != nil {
			return errors.Wrap(err, "exemplar check failed")
		}

		return nil
	}))
}
