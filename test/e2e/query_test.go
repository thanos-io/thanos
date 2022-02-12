// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	"github.com/efficientgo/e2e"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/rules"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

// NOTE: by using aggregation all results are now unsorted.
var queryUpWithoutInstance = func() string { return "sum(up) without (instance)" }

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

func TestSidecarNotReady(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_query")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom, sidecar, err := e2ethanos.NewPrometheusWithSidecar(e, "alone", defaultPromConfig("prom-alone", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))
	testutil.Ok(t, prom.Stop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Sidecar should not be ready - it cannot accept traffic if Prometheus is down.
	testutil.Ok(t, runutil.Retry(1*time.Second, ctx.Done(), func() (rerr error) {
		req, err := http.NewRequestWithContext(ctx, "GET", "http://"+sidecar.Endpoint("http")+"/-/ready", nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer runutil.CloseWithErrCapture(&rerr, resp.Body, "closing resp body")

		if resp.StatusCode == 200 {
			return fmt.Errorf("got status code %d", resp.StatusCode)
		}
		return nil
	}))
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

	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(e, "alone", defaultPromConfig("prom-alone", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(e, "remote-and-sidecar", defaultPromConfig("prom-both-remote-write-and-sidecar", 1234, e2ethanos.RemoteWriteEndpoint(receiver.InternalEndpoint("remote-write")), ""), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, err)
	prom3, sidecar3, err := e2ethanos.NewPrometheusWithSidecar(e, "ha1", defaultPromConfig("prom-ha", 0, "", filepath.Join(e2ethanos.ContainerSharedDir, "", "*.yaml")), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, err)
	prom4, sidecar4, err := e2ethanos.NewPrometheusWithSidecar(e, "ha2", defaultPromConfig("prom-ha", 1, "", filepath.Join(e2ethanos.ContainerSharedDir, "", "*.yaml")), "", e2ethanos.DefaultPrometheusImage(), "")
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

	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(e, "alone", defaultPromConfig("prom-alone", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(e, "remote-and-sidecar", defaultPromConfig("prom-both-remote-write-and-sidecar", 1234, e2ethanos.RemoteWriteEndpoint(receiver.InternalEndpoint("remote-write")), ""), "", e2ethanos.DefaultPrometheusImage(), "")
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

	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(e, "alone", defaultPromConfig("prom-alone", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(e, "remote-and-sidecar", defaultPromConfig("prom-both-remote-write-and-sidecar", 1234, e2ethanos.RemoteWriteEndpoint(receiver.InternalEndpoint("remote-write")), ""), "", e2ethanos.DefaultPrometheusImage(), "")
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

	prom, sidecar, err := e2ethanos.NewPrometheusWithSidecar(e, "alone", defaultPromConfig("prom-alone", 0, "", ""), defaultWebConfig(), e2ethanos.DefaultPrometheusImage(), "")
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
				"",
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

type fakeMetricSample struct {
	label             string
	value             int64
	timestampUnixNano int64
}

func newSample(s fakeMetricSample) model.Sample {
	return model.Sample{
		Metric: map[model.LabelName]model.LabelValue{
			"__name__": "my_fake_metric",
			"instance": model.LabelValue(s.label),
		},
		Value:     model.SampleValue(s.value),
		Timestamp: model.TimeFromUnixNano(s.timestampUnixNano),
	}
}

// Regression test for https://github.com/thanos-io/thanos/issues/5033.
// Tests whether queries work with mixed sources, and with functions
// that we are pushing down: min, max, min_over_time, max_over_time,
// group.
func TestSidecarStorePushdown(t *testing.T) {
	t.Parallel()

	// Build up.
	e, err := e2e.NewDockerEnvironment("e2e_sidecar_store_pushdown")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(e, "p1", defaultPromConfig("p1", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1))

	const bucket = "store_gateway_test"
	m, err := e2ethanos.NewMinio(e, "thanos-minio", bucket)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	dir := filepath.Join(e.SharedDir(), "tmp")
	testutil.Ok(t, os.MkdirAll(filepath.Join(e.SharedDir(), dir), os.ModePerm))

	series := []labels.Labels{labels.FromStrings("__name__", "my_fake_metric", "instance", "foo")}
	extLset := labels.FromStrings("prometheus", "p1", "replica", "0")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	now := time.Now()
	id1, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset, 0, metadata.NoneFunc)
	testutil.Ok(t, err)

	l := log.NewLogfmtLogger(os.Stdout)
	bkt, err := s3.NewBucketWithConfig(l, e2ethanos.NewS3Config(bucket, m.Endpoint("https"), e.SharedDir()), "test")
	testutil.Ok(t, err)

	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id1.String()), id1.String()))

	s1, err := e2ethanos.NewStoreGW(
		e,
		"1",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("https"), e2ethanos.ContainerSharedDir),
		},
		"",
		nil,
	)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(s1))

	q, err := e2ethanos.NewQuerierBuilder(e, "1", s1.InternalEndpoint("grpc"), sidecar1.InternalEndpoint("grpc")).WithEnabledFeatures([]string{"query-pushdown"}).Build()
	testutil.Ok(t, err)

	testutil.Ok(t, e2e.StartAndWaitReady(q))
	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(1), "thanos_blocks_meta_synced"))

	testutil.Ok(t, synthesizeSamples(ctx, prom1, []fakeMetricSample{
		{
			label: "foo",
			value: 123,
		},
	}))

	queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string {
		return "max_over_time(my_fake_metric[2h])"
	}, time.Now, promclient.QueryOptions{
		Deduplicate: true,
	}, []model.Metric{
		{
			"instance":   "foo",
			"prometheus": "p1",
		},
	})

	queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string {
		return "max(my_fake_metric) by (__name__, instance)"
	}, time.Now, promclient.QueryOptions{
		Deduplicate: true,
	}, []model.Metric{
		{
			"instance": "foo",
			"__name__": "my_fake_metric",
		},
	})

	queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string {
		return "min_over_time(my_fake_metric[2h])"
	}, time.Now, promclient.QueryOptions{
		Deduplicate: true,
	}, []model.Metric{
		{
			"instance":   "foo",
			"prometheus": "p1",
		},
	})

	queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string {
		return "min(my_fake_metric) by (instance, __name__)"
	}, time.Now, promclient.QueryOptions{
		Deduplicate: true,
	}, []model.Metric{
		{
			"instance": "foo",
			"__name__": "my_fake_metric",
		},
	})

	queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string {
		return "group(my_fake_metric) by (__name__, instance)"
	}, time.Now, promclient.QueryOptions{
		Deduplicate: true,
	}, []model.Metric{
		{
			"instance": "foo",
			"__name__": "my_fake_metric",
		},
	})

}

func TestSidecarQueryEvaluation(t *testing.T) {
	t.Parallel()

	timeNow := time.Now().UnixNano()

	ts := []struct {
		prom1Samples []fakeMetricSample
		prom2Samples []fakeMetricSample
		query        string
		result       model.Vector
	}{
		{
			query:        "max (my_fake_metric)",
			prom1Samples: []fakeMetricSample{{"i1", 1, timeNow}, {"i2", 5, timeNow}, {"i3", 9, timeNow}},
			prom2Samples: []fakeMetricSample{{"i1", 3, timeNow}, {"i2", 4, timeNow}, {"i3", 10, timeNow}},
			result: []*model.Sample{
				{
					Metric: map[model.LabelName]model.LabelValue{},
					Value:  10,
				},
			},
		},
		{
			query:        "max by (instance) (my_fake_metric)",
			prom1Samples: []fakeMetricSample{{"i1", 1, timeNow}, {"i2", 5, timeNow}, {"i3", 9, timeNow}},
			prom2Samples: []fakeMetricSample{{"i1", 3, timeNow}, {"i2", 4, timeNow}, {"i3", 10, timeNow}},
			result: []*model.Sample{
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i1"},
					Value:  3,
				},
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i2"},
					Value:  5,
				},
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i3"},
					Value:  10,
				},
			},
		},
		{
			query:        "group by (instance) (my_fake_metric)",
			prom1Samples: []fakeMetricSample{{"i1", 1, timeNow}, {"i2", 5, timeNow}, {"i3", 9, timeNow}},
			prom2Samples: []fakeMetricSample{{"i1", 3, timeNow}, {"i2", 4, timeNow}},
			result: []*model.Sample{
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i1"},
					Value:  1,
				},
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i2"},
					Value:  1,
				},
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i3"},
					Value:  1,
				},
			},
		},
		{
			query:        "max_over_time(my_fake_metric[10m])",
			prom1Samples: []fakeMetricSample{{"i1", 1, timeNow}, {"i2", 5, timeNow}},
			prom2Samples: []fakeMetricSample{{"i1", 3, timeNow}},
			result: []*model.Sample{
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i1", "prometheus": "p1"},
					Value:  1,
				},
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i1", "prometheus": "p2"},
					Value:  3,
				},
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i2", "prometheus": "p1"},
					Value:  5,
				},
			},
		},
		{
			query:        "min_over_time(my_fake_metric[10m])",
			prom1Samples: []fakeMetricSample{{"i1", 1, timeNow}, {"i2", 5, timeNow}},
			prom2Samples: []fakeMetricSample{{"i1", 3, timeNow}},
			result: []*model.Sample{
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i1", "prometheus": "p1"},
					Value:  1,
				},
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i1", "prometheus": "p2"},
					Value:  3,
				},
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i2", "prometheus": "p1"},
					Value:  5,
				},
			},
		},
	}

	for _, tc := range ts {
		t.Run(tc.query, func(t *testing.T) {
			e, err := e2e.NewDockerEnvironment("e2e_test_query_pushdown")
			testutil.Ok(t, err)
			t.Cleanup(e2ethanos.CleanScenario(t, e))

			prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(e, "p1", defaultPromConfig("p1", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
			testutil.Ok(t, err)
			testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1))

			prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(e, "p2", defaultPromConfig("p2", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
			testutil.Ok(t, err)
			testutil.Ok(t, e2e.StartAndWaitReady(prom2, sidecar2))

			endpoints := []string{
				sidecar1.InternalEndpoint("grpc"),
				sidecar2.InternalEndpoint("grpc"),
			}
			q, err := e2ethanos.
				NewQuerierBuilder(e, "1", endpoints...).
				WithEnabledFeatures([]string{"query-pushdown"}).
				Build()
			testutil.Ok(t, err)
			testutil.Ok(t, e2e.StartAndWaitReady(q))

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			t.Cleanup(cancel)

			testutil.Ok(t, synthesizeSamples(ctx, prom1, tc.prom1Samples))
			testutil.Ok(t, synthesizeSamples(ctx, prom2, tc.prom2Samples))

			testQuery := func() string { return tc.query }
			queryAndAssert(t, ctx, q.Endpoint("http"), testQuery, time.Now, promclient.QueryOptions{
				Deduplicate: true,
			}, tc.result)
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

func instantQuery(t testing.TB, ctx context.Context, addr string, q func() string, ts func() time.Time, opts promclient.QueryOptions, expectedSeriesLen int) model.Vector {
	t.Helper()

	fmt.Println("queryAndAssert: Waiting for", expectedSeriesLen, "results for query", q())
	var result model.Vector

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	testutil.Ok(t, runutil.RetryWithLog(logger, 5*time.Second, ctx.Done(), func() error {
		res, warnings, err := promclient.NewDefaultClient().QueryInstant(ctx, mustURLParse(t, "http://"+addr), q(), ts(), opts)
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

func queryAndAssertSeries(t *testing.T, ctx context.Context, addr string, q func() string, ts func() time.Time, opts promclient.QueryOptions, expected []model.Metric) {
	t.Helper()

	result := instantQuery(t, ctx, addr, q, ts, opts, len(expected))
	for i, exp := range expected {
		testutil.Equals(t, exp, result[i].Metric)
	}
}

func queryAndAssert(t *testing.T, ctx context.Context, addr string, q func() string, ts func() time.Time, opts promclient.QueryOptions, expected model.Vector) {
	t.Helper()

	sortResults(expected)
	result := instantQuery(t, ctx, addr, q, ts, opts, len(expected))
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
func rangeQuery(t *testing.T, ctx context.Context, addr string, q func() string, start, end, step int64, opts promclient.QueryOptions, check func(res model.Matrix) error) {
	t.Helper()

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	testutil.Ok(t, runutil.RetryWithLog(logger, time.Second, ctx.Done(), func() error {
		res, warnings, err := promclient.NewDefaultClient().QueryRange(ctx, mustURLParse(t, "http://"+addr), q(), start, end, step, opts)
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

func synthesizeSamples(ctx context.Context, prometheus *e2e.InstrumentedRunnable, testSamples []fakeMetricSample) error {
	samples := make([]model.Sample, len(testSamples))
	for i, s := range testSamples {
		samples[i] = newSample(s)
	}

	remoteWriteURL, err := url.Parse("http://" + prometheus.Endpoint("http") + "/api/v1/write")
	if err != nil {
		return err
	}

	client, err := remote.NewWriteClient("remote-write-client", &remote.ClientConfig{
		URL:     &config_util.URL{URL: remoteWriteURL},
		Timeout: model.Duration(30 * time.Second),
	})
	if err != nil {
		return err
	}

	samplespb := make([]prompb.TimeSeries, 0, len(samples))
	for _, sample := range samples {
		labelspb := make([]prompb.Label, 0, len(sample.Metric))
		for labelKey, labelValue := range sample.Metric {
			labelspb = append(labelspb, prompb.Label{
				Name:  string(labelKey),
				Value: string(labelValue),
			})
		}
		samplespb = append(samplespb, prompb.TimeSeries{
			Labels: labelspb,
			Samples: []prompb.Sample{
				{
					Value:     float64(sample.Value),
					Timestamp: sample.Timestamp.Time().Unix() * 1000,
				},
			},
		})
	}

	sample := &prompb.WriteRequest{
		Timeseries: samplespb,
	}

	var buf []byte
	pBuf := proto.NewBuffer(nil)
	if err := pBuf.Marshal(sample); err != nil {
		return err
	}

	compressed := snappy.Encode(buf, pBuf.Bytes())
	return client.Store(ctx, compressed)
}

func TestSidecarQueryEvaluationWithDedup(t *testing.T) {
	t.Parallel()

	timeNow := time.Now().UnixNano()

	ts := []struct {
		prom1Samples []fakeMetricSample
		prom2Samples []fakeMetricSample
		query        string
		result       model.Vector
	}{
		{
			query:        "max (my_fake_metric)",
			prom1Samples: []fakeMetricSample{{"i1", 1, timeNow}, {"i2", 5, timeNow}, {"i3", 9, timeNow}},
			prom2Samples: []fakeMetricSample{{"i1", 3, timeNow}, {"i2", 4, timeNow}, {"i3", 10, timeNow}},
			result: []*model.Sample{
				{
					Metric: map[model.LabelName]model.LabelValue{},
					Value:  10,
				},
			},
		},
		{
			query:        "max by (instance) (my_fake_metric)",
			prom1Samples: []fakeMetricSample{{"i1", 1, timeNow}, {"i2", 5, timeNow}, {"i3", 9, timeNow}},
			prom2Samples: []fakeMetricSample{{"i1", 3, timeNow}, {"i2", 4, timeNow}, {"i3", 10, timeNow}},
			result: []*model.Sample{
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i1"},
					Value:  3,
				},
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i2"},
					Value:  5,
				},
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i3"},
					Value:  10,
				},
			},
		},
		{
			query:        "group by (instance) (my_fake_metric)",
			prom1Samples: []fakeMetricSample{{"i1", 1, timeNow}, {"i2", 5, timeNow}, {"i3", 9, timeNow}},
			prom2Samples: []fakeMetricSample{{"i1", 3, timeNow}, {"i2", 4, timeNow}},
			result: []*model.Sample{
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i1"},
					Value:  1,
				},
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i2"},
					Value:  1,
				},
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i3"},
					Value:  1,
				},
			},
		},
		{
			query:        "max_over_time(my_fake_metric[10m])",
			prom1Samples: []fakeMetricSample{{"i1", 1, timeNow}, {"i2", 5, timeNow}},
			prom2Samples: []fakeMetricSample{{"i1", 3, timeNow}},
			result: []*model.Sample{
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i1", "prometheus": "p1"},
					Value:  3,
				},
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i2", "prometheus": "p1"},
					Value:  5,
				},
			},
		},
		{
			query:        "min_over_time(my_fake_metric[10m])",
			prom1Samples: []fakeMetricSample{{"i1", 1, timeNow}, {"i2", 5, timeNow}},
			prom2Samples: []fakeMetricSample{{"i1", 3, timeNow}},
			result: []*model.Sample{
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i1", "prometheus": "p1"},
					Value:  1,
				},
				{
					Metric: map[model.LabelName]model.LabelValue{"instance": "i2", "prometheus": "p1"},
					Value:  5,
				},
			},
		},
	}

	for _, tc := range ts {
		t.Run(tc.query, func(t *testing.T) {
			e, err := e2e.NewDockerEnvironment("e2e_test_query_pushdown_dedup")
			testutil.Ok(t, err)
			t.Cleanup(e2ethanos.CleanScenario(t, e))

			prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(e, "p1", defaultPromConfig("p1", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
			testutil.Ok(t, err)
			testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1))

			prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(e, "p2", defaultPromConfig("p1", 1, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
			testutil.Ok(t, err)
			testutil.Ok(t, e2e.StartAndWaitReady(prom2, sidecar2))

			endpoints := []string{
				sidecar1.InternalEndpoint("grpc"),
				sidecar2.InternalEndpoint("grpc"),
			}
			q, err := e2ethanos.
				NewQuerierBuilder(e, "1", endpoints...).
				WithEnabledFeatures([]string{"query-pushdown"}).
				Build()
			testutil.Ok(t, err)
			testutil.Ok(t, e2e.StartAndWaitReady(q))

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			t.Cleanup(cancel)

			testutil.Ok(t, synthesizeSamples(ctx, prom1, tc.prom1Samples))
			testutil.Ok(t, synthesizeSamples(ctx, prom2, tc.prom2Samples))

			testQuery := func() string { return tc.query }
			queryAndAssert(t, ctx, q.Endpoint("http"), testQuery, time.Now, promclient.QueryOptions{
				Deduplicate: true,
			}, tc.result)
		})
	}
}

// TestSidecarStoreAlignmentPushdown tests how pushdown works with
// --min-time and --max-time.
func TestSidecarAlignmentPushdown(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_query_pushdown_min_max_time")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	now := time.Now()

	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(e, "p1", defaultPromConfig("p1", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), now.Add(time.Duration(-1)*time.Hour).Format(time.RFC3339), now.Format(time.RFC3339), "remote-write-receiver")
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1))

	endpoints := []string{
		sidecar1.InternalEndpoint("grpc"),
	}
	q1, err := e2ethanos.
		NewQuerierBuilder(e, "1", endpoints...).
		Build()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q1))
	q2, err := e2ethanos.
		NewQuerierBuilder(e, "2", endpoints...).
		WithEnabledFeatures([]string{"query-pushdown"}).
		Build()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q2))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	for i := now.Add(time.Duration(-3) * time.Hour); i.Before(now); i = i.Add(30 * time.Second) {
		testutil.Ok(t, synthesizeSamples(ctx, prom1, []fakeMetricSample{
			{
				label:             "test",
				value:             1,
				timestampUnixNano: i.UnixNano(),
			},
		}))

	}

	// This query should have identical requests.
	testQuery := func() string { return `max_over_time({instance="test"}[5m])` }

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	var expectedRes model.Matrix
	testutil.Ok(t, runutil.RetryWithLog(logger, time.Second, ctx.Done(), func() error {
		res, warnings, err := promclient.NewDefaultClient().QueryRange(ctx, mustURLParse(t, "http://"+q1.Endpoint("http")), testQuery(),
			timestamp.FromTime(now.Add(time.Duration(-7*24)*time.Hour)),
			timestamp.FromTime(now),
			2419, // Taken from UI.
			promclient.QueryOptions{
				Deduplicate: true,
			})
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		expectedRes = res
		return nil
	}))

	rangeQuery(t, ctx, q2.Endpoint("http"), testQuery, timestamp.FromTime(now.Add(time.Duration(-7*24)*time.Hour)),
		timestamp.FromTime(now),
		2419, // Taken from UI.
		promclient.QueryOptions{
			Deduplicate: true,
		}, func(res model.Matrix) error {
			if !reflect.DeepEqual(res, expectedRes) {
				return fmt.Errorf("unexpected results (got %v but expected %v)", res, expectedRes)
			}
			return nil
		})
}
