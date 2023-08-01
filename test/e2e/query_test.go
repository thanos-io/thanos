// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage/remote"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	"github.com/thanos-io/objstore/providers/s3"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	prompb_copy "github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

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

func TestQueryServiceAttribute(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("queryserviceattr")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	newJaegerRunnable := e.Runnable("jaeger").
		WithPorts(
			map[string]int{
				"http":                      16686,
				"http.admin":                14269,
				"jaeger.thrift-model.proto": 14250,
				"jaeger.thrift":             14268,
			}).
		Init(e2e.StartOptions{
			Image:     "jaegertracing/all-in-one:1.33",
			Readiness: e2e.NewHTTPReadinessProbe("http.admin", "/", 200, 200),
		})
	newJaeger := e2emon.AsInstrumented(newJaegerRunnable, "http.admin")
	testutil.Ok(t, e2e.StartAndWaitReady(newJaeger))

	otelcolConfig := fmt.Sprintf(`---
receivers:
  otlp:
    protocols:
      grpc:
      http:

processors:
  batch:

exporters:
  logging:
    loglevel: debug
  jaeger:
    endpoint: %s
    tls:
      insecure: true

extensions:
  health_check:
  pprof:
  zpages:

service:
  extensions: [health_check,pprof,zpages]
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [logging, jaeger]`, newJaeger.InternalEndpoint("jaeger.thrift-model.proto"))

	t.Log(otelcolConfig)

	otelcol := e.Runnable("otelcol").WithPorts(map[string]int{
		"grpc": 4317,
		"http": 4318,
	})

	otelcolFuture := otelcol.Future()

	testutil.Ok(t, os.WriteFile(filepath.Join(e.SharedDir(), "otelcol.yml"), []byte(otelcolConfig), os.ModePerm))
	testutil.Ok(t, os.WriteFile(filepath.Join(e.SharedDir(), "spans.json"), []byte(``), os.ModePerm))

	otelcolRunnable := otelcolFuture.Init(e2e.StartOptions{
		Image: "otel/opentelemetry-collector-contrib:0.80.0",
		Volumes: []string{
			fmt.Sprintf("%s:/otelcol.yml:ro", filepath.Join(e.SharedDir(), "otelcol.yml")),
		},
		Command: e2e.NewCommandWithoutEntrypoint("/otelcol-contrib", "--config", "/otelcol.yml"),
	})

	testutil.Ok(t, e2e.StartAndWaitReady(otelcolRunnable))

	q := e2ethanos.NewQuerierBuilder(e, "queriertester").WithTracingConfig(fmt.Sprintf(`type: OTLP
config:
  client_type: grpc
  insecure: true
  endpoint: %s`, otelcolRunnable.InternalEndpoint("grpc"))).WithEnvVars(map[string]string{
		"OTEL_RESOURCE_ATTRIBUTES": "service.name=thanos-query",
	}).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	instantQuery(t,
		context.Background(),
		q.Endpoint("http"),
		func() string { return "time()" },
		time.Now,
		promclient.QueryOptions{},
		1)

	url := "http://" + strings.TrimSpace(newJaeger.Endpoint("http")+"/api/traces?service=thanos-query")
	request, err := http.NewRequest("GET", url, nil)
	testutil.Ok(t, err)

	testutil.Ok(t, runutil.Retry(1*time.Second, make(<-chan struct{}), func() error {
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return err
		}

		if response.StatusCode != http.StatusOK {
			return errors.New("status code not OK")
		}

		defer response.Body.Close()

		body, err := io.ReadAll(response.Body)
		testutil.Ok(t, err)

		resp := string(body)
		if strings.Contains(resp, `"data":[]`) {
			return errors.New("no data returned")
		}

		testutil.Assert(t, strings.Contains(resp, `"serviceName":"thanos-query"`))
		return nil
	}))
}

func TestSidecarNotReady(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("sidecar-notReady")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "alone", e2ethanos.DefaultPromConfig("prom-alone", 0, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "")
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

	e, err := e2e.NewDockerEnvironment("e2e-test-query")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	receiver := e2ethanos.NewReceiveBuilder(e, "1").WithIngestionEnabled().Init()
	testutil.Ok(t, e2e.StartAndWaitReady(receiver))

	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "alone", e2ethanos.DefaultPromConfig("prom-alone", 0, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "")
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "remote-and-sidecar", e2ethanos.DefaultPromConfig("prom-both-remote-write-and-sidecar", 1234, e2ethanos.RemoteWriteEndpoint(receiver.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "")
	prom3, sidecar3 := e2ethanos.NewPrometheusWithSidecar(e, "ha1", e2ethanos.DefaultPromConfig("prom-ha", 0, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "")
	prom4, sidecar4 := e2ethanos.NewPrometheusWithSidecar(e, "ha2", e2ethanos.DefaultPromConfig("prom-ha", 1, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2, prom3, sidecar3, prom4, sidecar4))

	// Querier. Both fileSD and directly by flags.
	q := e2ethanos.NewQuerierBuilder(e, "1", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc"), receiver.InternalEndpoint("grpc")).
		WithFileSDStoreAddresses(sidecar3.InternalEndpoint("grpc"), sidecar4.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(5), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

	queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
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
	queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
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

	e, err := e2e.NewDockerEnvironment("route-prefix")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	externalPrefix := "test"

	q := e2ethanos.NewQuerierBuilder(e, "1").
		WithExternalPrefix(externalPrefix).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	checkNetworkRequests(t, "http://"+q.Endpoint("http")+"/"+externalPrefix+"/graph")
}

func TestQueryExternalPrefix(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("external-prefix")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	externalPrefix := "thanos"

	q := e2ethanos.NewQuerierBuilder(e, "1").
		WithExternalPrefix(externalPrefix).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	querierURL := urlParse(t, "http://"+q.Endpoint("http")+"/"+externalPrefix)

	querierProxy := httptest.NewServer(e2ethanos.NewSingleHostReverseProxy(querierURL, externalPrefix))
	t.Cleanup(querierProxy.Close)

	checkNetworkRequests(t, querierProxy.URL+"/"+externalPrefix+"/graph")
}

func TestQueryExternalPrefixAndRoutePrefix(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("prefix")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	externalPrefix := "thanos"
	routePrefix := "test"

	q := e2ethanos.NewQuerierBuilder(e, "1").
		WithRoutePrefix(routePrefix).
		WithExternalPrefix(externalPrefix).
		Init()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	querierURL := urlParse(t, "http://"+q.Endpoint("http")+"/"+routePrefix)

	querierProxy := httptest.NewServer(e2ethanos.NewSingleHostReverseProxy(querierURL, externalPrefix))
	t.Cleanup(querierProxy.Close)

	checkNetworkRequests(t, querierProxy.URL+"/"+externalPrefix+"/graph")
}

func TestQueryLabelNames(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("label-names")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	receiver := e2ethanos.NewReceiveBuilder(e, "1").WithIngestionEnabled().Init()
	testutil.Ok(t, e2e.StartAndWaitReady(receiver))

	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "alone", e2ethanos.DefaultPromConfig("prom-alone", 0, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "")
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "remote-and-sidecar", e2ethanos.DefaultPromConfig("prom-both-remote-write-and-sidecar", 1234, e2ethanos.RemoteWriteEndpoint(receiver.InternalEndpoint("remote-write")), "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	q := e2ethanos.NewQuerierBuilder(e, "1", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc"), receiver.InternalEndpoint("grpc")).Init()
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

	e, err := e2e.NewDockerEnvironment("label-values")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	receiver := e2ethanos.NewReceiveBuilder(e, "1").WithIngestionEnabled().Init()
	testutil.Ok(t, e2e.StartAndWaitReady(receiver))

	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "alone", e2ethanos.DefaultPromConfig("prom-alone", 0, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "")
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "remote-and-sidecar", e2ethanos.DefaultPromConfig("prom-both-remote-write-and-sidecar", 1234, e2ethanos.RemoteWriteEndpoint(receiver.InternalEndpoint("remote-write")), ""), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	q := e2ethanos.NewQuerierBuilder(e, "1", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc"), receiver.InternalEndpoint("grpc")).Init()
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

	e, err := e2e.NewDockerEnvironment("sidecar-auth")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "alone", e2ethanos.DefaultPromConfig("prom-alone", 0, "", "", e2ethanos.LocalPrometheusTarget), defaultWebConfig(), e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	q := e2ethanos.NewQuerierBuilder(e, "1", []string{sidecar.InternalEndpoint("grpc")}...).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

	queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
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
	if runtime.GOARCH != "amd64" {
		t.Skip("Skip pre-info API test because of lack of multi-arch image for Thanos v0.22.0.")
	}

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
			e, err := e2e.NewDockerEnvironment(fmt.Sprintf("query-comp-%d", i))
			testutil.Ok(t, err)
			t.Cleanup(e2ethanos.CleanScenario(t, e))

			qBuilder := e2ethanos.NewQuerierBuilder(e, "1")

			// Use qBuilder work dir to share rules.
			promRulesSubDir := "rules"
			testutil.Ok(t, os.MkdirAll(filepath.Join(qBuilder.Dir(), promRulesSubDir), os.ModePerm))
			// Create the abort_on_partial_response alert for Prometheus.
			// We don't create the warn_on_partial_response alert as Prometheus has strict yaml unmarshalling.
			createRuleFile(t, filepath.Join(qBuilder.Dir(), promRulesSubDir, "rules.yaml"), testAlertRuleAbortOnPartialResponse)

			p1, s1 := e2ethanos.NewPrometheusWithSidecarCustomImage(
				e,
				"p1",
				e2ethanos.DefaultPromConfig("p1", 0, "", filepath.Join(qBuilder.InternalDir(), promRulesSubDir, "*.yaml"), e2ethanos.LocalPrometheusTarget, qBuilder.InternalEndpoint("http")),
				"",
				e2ethanos.DefaultPrometheusImage(),
				"",
				tcase.sidecarImage,
				e2ethanos.FeatureExemplarStorage,
			)
			testutil.Ok(t, e2e.StartAndWaitReady(p1, s1))

			// Newest querier with old --rules --meta etc flags.
			q := qBuilder.
				WithStoreAddresses(s1.InternalEndpoint("grpc")).
				WithMetadataAddresses(s1.InternalEndpoint("grpc")).
				WithExemplarAddresses(s1.InternalEndpoint("grpc")).
				WithTargetAddresses(s1.InternalEndpoint("grpc")).
				WithRuleAddresses(s1.InternalEndpoint("grpc")).
				WithTracingConfig(fmt.Sprintf(`type: JAEGER
config:
  sampler_type: const
  sampler_param: 1
  service_name: %s`, qBuilder.Name())). // Use fake tracing config to trigger exemplar.
				WithImage(tcase.queryImage).
				Init()
			testutil.Ok(t, e2e.StartAndWaitReady(q))

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			t.Cleanup(cancel)

			// We should have single TCP connection, since all APIs are against the same server.
			testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

			queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
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
					promMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, urlParse(t, "http://"+p1.Endpoint("http")), "", -1)
					testutil.Ok(t, err)
					if len(promMeta) > 0 {
						return nil
					}
					return fmt.Errorf("empty metadata response from Prometheus")
				}))

				thanosMeta, err := promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, urlParse(t, "http://"+q.Endpoint("http")), "", -1)
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
								{Name: "__address__", Value: "localhost:9090"},
								{Name: "__metrics_path__", Value: "/metrics"},
								{Name: "__scheme__", Value: "http"},
								{Name: "__scrape_interval__", Value: "1s"},
								{Name: "__scrape_timeout__", Value: "1s"},
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
						{
							DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "__address__", Value: fmt.Sprintf("query-comp-%d-querier-1:8080", i)},
								{Name: "__metrics_path__", Value: "/metrics"},
								{Name: "__scheme__", Value: "http"},
								{Name: "__scrape_interval__", Value: "1s"},
								{Name: "__scrape_timeout__", Value: "1s"},
								{Name: "job", Value: "myself"},
								{Name: "prometheus", Value: "p1"},
							}},
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "instance", Value: fmt.Sprintf("query-comp-%d-querier-1:8080", i)},
								{Name: "job", Value: "myself"},
								{Name: "prometheus", Value: "p1"},
							}},
							ScrapePool: "myself",
							ScrapeUrl:  fmt.Sprintf("http://query-comp-%d-querier-1:8080/metrics", i),
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
						File: q.Dir() + "/rules/rules.yaml",
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

func TestQueryStoreMetrics(t *testing.T) {
	t.Parallel()

	// Build up.
	e, err := e2e.New(e2e.WithName("storemetrics01"))
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	bucket := "store-gw-test"
	minio := e2edb.NewMinio(e, "thanos-minio", bucket, e2edb.WithMinioTLS())
	testutil.Ok(t, e2e.StartAndWaitReady(minio))

	l := log.NewLogfmtLogger(os.Stdout)
	bkt, err := s3.NewBucketWithConfig(l, e2ethanos.NewS3Config(bucket, minio.Endpoint("http"), minio.Dir()), "test")
	testutil.Ok(t, err)

	// Preparing 3 different blocks for the tests.
	{
		blockSizes := []struct {
			samples int
			series  int
			name    string
		}{
			{samples: 10, series: 1, name: "one_series"},
			{samples: 10, series: 1001, name: "thousand_one_series"},
			{samples: 10, series: 10001, name: "inf_series"},
		}
		now := time.Now()
		externalLabels := labels.FromStrings("prometheus", "p1", "replica", "0")
		dir := filepath.Join(e.SharedDir(), "tmp")
		testutil.Ok(t, os.MkdirAll(filepath.Join(e.SharedDir(), dir), os.ModePerm))
		for _, blockSize := range blockSizes {
			series := make([]labels.Labels, blockSize.series)
			for i := 0; i < blockSize.series; i++ {
				bigSeriesLabels := labels.FromStrings("__name__", blockSize.name, "instance", fmt.Sprintf("foo_%d", i))
				series[i] = bigSeriesLabels
			}
			blockID, err := e2eutil.CreateBlockWithBlockDelay(ctx,
				dir,
				series,
				blockSize.samples,
				timestamp.FromTime(now),
				timestamp.FromTime(now.Add(2*time.Hour)),
				30*time.Minute,
				externalLabels,
				0,
				metadata.NoneFunc,
			)
			testutil.Ok(t, err)
			testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, blockID.String()), blockID.String()))
		}
	}

	storeGW := e2ethanos.NewStoreGW(
		e,
		"s1",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket, minio.InternalEndpoint("http"), minio.InternalDir()),
		},
		"",
		"",
		nil,
	)

	sampleBuckets := []float64{100, 1000, 10000, 100000}
	seriesBuckets := []float64{10, 100, 1000, 10000}
	querier := e2ethanos.NewQuerierBuilder(e, "1", storeGW.InternalEndpoint("grpc")).WithTelemetryQuantiles(nil, sampleBuckets, seriesBuckets).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(storeGW, querier))
	testutil.Ok(t, storeGW.WaitSumMetrics(e2emon.Equals(3), "thanos_blocks_meta_synced"))

	// Querying the series in the previously created blocks to ensure we produce Store API query metrics.
	{
		instantQuery(t, ctx, querier.Endpoint("http"), func() string {
			return "max_over_time(one_series{instance='foo_0'}[2h])"
		}, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, 1)
		testutil.Ok(t, err)

		instantQuery(t, ctx, querier.Endpoint("http"), func() string {
			return "max_over_time(thousand_one_series[2h])"
		}, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, 1001)
		testutil.Ok(t, err)

		instantQuery(t, ctx, querier.Endpoint("http"), func() string {
			return "max_over_time(inf_series[2h])"
		}, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, 10001)
		testutil.Ok(t, err)
	}

	mon, err := e2emon.Start(e)
	testutil.Ok(t, err)

	queryWaitAndAssert(t, ctx, mon.GetMonitoringRunnable().Endpoint(e2edb.AccessPortName), func() string {
		return "thanos_store_api_query_duration_seconds_count{samples_le='100000',series_le='10000'}"
	}, time.Now, promclient.QueryOptions{
		Deduplicate: true,
	}, model.Vector{
		&model.Sample{
			Metric: model.Metric{
				"__name__":   "thanos_store_api_query_duration_seconds_count",
				"instance":   "storemetrics01-querier-1:8080",
				"job":        "querier-1",
				"samples_le": "100000",
				"series_le":  "10000",
			},
			Value: model.SampleValue(1),
		},
	})

	queryWaitAndAssert(t, ctx, mon.GetMonitoringRunnable().Endpoint(e2edb.AccessPortName), func() string {
		return "thanos_store_api_query_duration_seconds_count{samples_le='100',series_le='10'}"
	}, time.Now, promclient.QueryOptions{
		Deduplicate: true,
	}, model.Vector{
		&model.Sample{
			Metric: model.Metric{
				"__name__":   "thanos_store_api_query_duration_seconds_count",
				"instance":   "storemetrics01-querier-1:8080",
				"job":        "querier-1",
				"samples_le": "100",
				"series_le":  "10",
			},
			Value: model.SampleValue(1),
		},
	})

	queryWaitAndAssert(t, ctx, mon.GetMonitoringRunnable().Endpoint(e2edb.AccessPortName), func() string {
		return "thanos_store_api_query_duration_seconds_count{samples_le='+Inf',series_le='+Inf'}"
	}, time.Now, promclient.QueryOptions{
		Deduplicate: true,
	}, model.Vector{
		&model.Sample{
			Metric: model.Metric{
				"__name__":   "thanos_store_api_query_duration_seconds_count",
				"instance":   "storemetrics01-querier-1:8080",
				"job":        "querier-1",
				"samples_le": "+Inf",
				"series_le":  "+Inf",
			},
			Value: model.SampleValue(1),
		},
	})

}

// Regression test for https://github.com/thanos-io/thanos/issues/5033.
// Tests whether queries work with mixed sources, and with functions
// that we are pushing down: min, max, min_over_time, max_over_time,
// group.
func TestSidecarStorePushdown(t *testing.T) {
	t.Parallel()

	// Build up.
	e, err := e2e.NewDockerEnvironment("sidecar-pushdown")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "p1", e2ethanos.DefaultPromConfig("p1", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1))

	const bucket = "store-gateway-test"
	m := e2edb.NewMinio(e, "thanos-minio", bucket, e2edb.WithMinioTLS())
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
	bkt, err := s3.NewBucketWithConfig(l, e2ethanos.NewS3Config(bucket, m.Endpoint("http"), m.Dir()), "test")
	testutil.Ok(t, err)
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id1.String()), id1.String()))

	s1 := e2ethanos.NewStoreGW(
		e,
		"1",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("http"), m.InternalDir()),
		},
		"",
		"",
		nil,
	)
	testutil.Ok(t, e2e.StartAndWaitReady(s1))

	q := e2ethanos.NewQuerierBuilder(e, "1", s1.InternalEndpoint("grpc"), sidecar1.InternalEndpoint("grpc")).WithEnabledFeatures([]string{"query-pushdown"}).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))
	testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(1), "thanos_blocks_meta_synced"))

	testutil.Ok(t, synthesizeFakeMetricSamples(ctx, prom1, []fakeMetricSample{
		{
			label:             "foo",
			value:             123,
			timestampUnixNano: now.UnixNano(),
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

type seriesWithLabels struct {
	intLabels labels.Labels
	extLabels labels.Labels
}

func TestQueryStoreDedup(t *testing.T) {
	t.Parallel()

	e, err := e2e.New(e2e.WithName("storededup"))
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	bucket := "store-gw-dedup-test"
	minio := e2edb.NewMinio(e, "thanos-minio", bucket, e2edb.WithMinioTLS())
	testutil.Ok(t, e2e.StartAndWaitReady(minio))

	l := log.NewLogfmtLogger(os.Stdout)
	bkt, err := s3.NewBucketWithConfig(l, e2ethanos.NewS3Config(bucket, minio.Endpoint("http"), minio.Dir()), "test")
	testutil.Ok(t, err)

	storeGW := e2ethanos.NewStoreGW(
		e,
		"s1",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket, minio.InternalEndpoint("http"), minio.InternalDir()),
		},
		"",
		"",
		nil,
	)
	testutil.Ok(t, e2e.StartAndWaitReady(storeGW))

	tests := []struct {
		extReplicaLabel  string
		intReplicaLabel  string
		desc             string
		blockFinderLabel string
		series           []seriesWithLabels
		expectedSeries   int
	}{
		{
			desc:            "Deduplication works with external label",
			extReplicaLabel: "replica",
			series: []seriesWithLabels{
				{
					intLabels: labels.FromStrings("__name__", "simple_series"),
					extLabels: labels.FromStrings("replica", "a"),
				},
				{
					intLabels: labels.FromStrings("__name__", "simple_series"),
					extLabels: labels.FromStrings("replica", "b"),
				},
			},
			blockFinderLabel: "dedupext",
			expectedSeries:   1,
		},
		{
			desc:            "Deduplication works on external label with resorting required",
			extReplicaLabel: "a",
			series: []seriesWithLabels{
				{
					intLabels: labels.FromStrings("__name__", "simple_series"),
					extLabels: labels.FromStrings("a", "1", "b", "1"),
				},
				{
					intLabels: labels.FromStrings("__name__", "simple_series"),
					extLabels: labels.FromStrings("a", "1", "b", "2"),
				},
				{
					intLabels: labels.FromStrings("__name__", "simple_series"),
					extLabels: labels.FromStrings("a", "2", "b", "1"),
				},
				{
					intLabels: labels.FromStrings("__name__", "simple_series"),
					extLabels: labels.FromStrings("a", "2", "b", "2"),
				},
			},
			blockFinderLabel: "dedupextresort",
			expectedSeries:   2,
		},
		{
			desc:            "Deduplication works with internal label",
			intReplicaLabel: "replica",
			series: []seriesWithLabels{
				{
					intLabels: labels.FromStrings("__name__", "simple_series", "replica", "a"),
				},
				{
					intLabels: labels.FromStrings("__name__", "simple_series", "replica", "b"),
				},
			},
			blockFinderLabel: "dedupint",
			expectedSeries:   1,
		},
		// This is a regression test for the bug outlined in https://github.com/thanos-io/thanos/issues/6257.
		{
			desc:            "Deduplication works on internal label with resorting required",
			intReplicaLabel: "a",
			series: []seriesWithLabels{
				{
					intLabels: labels.FromStrings("__name__", "simple_series", "a", "1", "b", "1"),
				},
				{
					intLabels: labels.FromStrings("__name__", "simple_series", "a", "1", "b", "2"),
				},
				{
					intLabels: labels.FromStrings("__name__", "simple_series", "a", "2", "b", "1"),
				},
				{
					intLabels: labels.FromStrings("__name__", "simple_series", "a", "2", "b", "2"),
				},
			},
			blockFinderLabel: "dedupintresort",
			expectedSeries:   2,
		},
		// This is a regression test for the bug outlined in https://github.com/thanos-io/thanos/issues/6257.
		{
			desc:            "Deduplication works with extra internal label",
			intReplicaLabel: "replica",
			series: []seriesWithLabels{
				{
					intLabels: labels.FromStrings("__name__", "simple_series", "replica", "a", "my_label", "1"),
				},
				{
					intLabels: labels.FromStrings("__name__", "simple_series", "replica", "a", "my_label", "2"),
				},
				{
					intLabels: labels.FromStrings("__name__", "simple_series", "replica", "b", "my_label", "1"),
				},
				{
					intLabels: labels.FromStrings("__name__", "simple_series", "replica", "b", "my_label", "2"),
				},
			},
			blockFinderLabel: "dedupintextra",
			expectedSeries:   2,
		},
		// This is a regression test for the bug outlined in https://github.com/thanos-io/thanos/issues/6257.
		{
			desc:            "Deduplication works with both internal and external label",
			intReplicaLabel: "replica",
			extReplicaLabel: "receive_replica",
			series: []seriesWithLabels{
				{
					intLabels: labels.FromStrings("__name__", "simple_series", "replica", "a"),
					extLabels: labels.FromStrings("replica", "a"),
				},
				{
					intLabels: labels.FromStrings("__name__", "simple_series", "replica", "b"),
					extLabels: labels.FromStrings("replica", "b"),
				},
			},
			blockFinderLabel: "dedupintext",
			expectedSeries:   1,
		},
	}

	// Prepare and upload all the blocks that will be used to S3.
	var totalBlocks int
	for _, tt := range tests {
		createSimpleReplicatedBlocksInS3(ctx, t, e, l, bkt, tt.series, tt.blockFinderLabel)
		totalBlocks += len(tt.series)
	}
	testutil.Ok(t, storeGW.WaitSumMetrics(e2emon.Equals(float64(totalBlocks)), "thanos_blocks_meta_synced"))

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			querierBuilder := e2ethanos.NewQuerierBuilder(e, tt.blockFinderLabel, storeGW.InternalEndpoint("grpc")).WithProxyStrategy("lazy")
			var replicaLabels []string
			if tt.intReplicaLabel != "" {
				replicaLabels = append(replicaLabels, tt.intReplicaLabel)
			}
			if tt.extReplicaLabel != "" {
				replicaLabels = append(replicaLabels, tt.extReplicaLabel)
			}
			if len(replicaLabels) > 0 {
				sort.Strings(replicaLabels)
				querierBuilder = querierBuilder.WithReplicaLabels(replicaLabels...)
			}
			querier := querierBuilder.Init()
			testutil.Ok(t, e2e.StartAndWaitReady(querier))

			expectedSeries := tt.expectedSeries
			instantQuery(t, ctx, querier.Endpoint("http"), func() string {
				return fmt.Sprintf("max_over_time(simple_series{block_finder='%s'}[2h])", tt.blockFinderLabel)
			}, time.Now, promclient.QueryOptions{
				Deduplicate: true,
			}, expectedSeries)
			testutil.Ok(t, err)
			testutil.Ok(t, querier.Stop())
		})
	}
}

// createSimpleReplicatedBlocksInS3 creates blocks in S3 with the series. If blockFinderLabel is not empty,
// it will be added to the block's labels to easily find the blocks on with queries later.
func createSimpleReplicatedBlocksInS3(
	ctx context.Context,
	t *testing.T,
	dockerEnv *e2e.DockerEnvironment,
	logger log.Logger,
	bucket *s3.Bucket,
	series []seriesWithLabels,
	blockFinderLabel string,
) {
	now := time.Now()
	dir := filepath.Join(dockerEnv.SharedDir(), "tmp")
	testutil.Ok(t, os.MkdirAll(dir, os.ModePerm))
	for _, s := range series {
		intLabels := s.intLabels
		if blockFinderLabel != "" {
			intLabels = labels.NewBuilder(s.intLabels.Copy()).Set("block_finder", blockFinderLabel).Labels()
		}
		blockID, err := e2eutil.CreateBlockWithBlockDelay(ctx,
			dir,
			[]labels.Labels{intLabels},
			1,
			timestamp.FromTime(now),
			timestamp.FromTime(now.Add(2*time.Hour)),
			30*time.Minute,
			s.extLabels,
			0,
			metadata.NoneFunc,
		)
		testutil.Ok(t, err)
		blockPath := path.Join(dir, blockID.String())
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bucket, blockPath, blockID.String()))
	}
}

func TestSidecarQueryDedup(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("sidecar-dedup")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)

	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "p1", e2ethanos.DefaultPromConfig("p1", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "p2", e2ethanos.DefaultPromConfig("p1", 1, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
	prom3, sidecar3 := e2ethanos.NewPrometheusWithSidecar(e, "p3", e2ethanos.DefaultPromConfig("p2", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2, prom3, sidecar3))

	{
		samples := []seriesWithLabels{
			{intLabels: labels.FromStrings("__name__", "my_fake_metric", "a", "1", "b", "1", "instance", "1")},
			{intLabels: labels.FromStrings("__name__", "my_fake_metric", "a", "1", "b", "2", "instance", "1")},
			{intLabels: labels.FromStrings("__name__", "my_fake_metric", "a", "2", "b", "1", "instance", "1")},
			{intLabels: labels.FromStrings("__name__", "my_fake_metric", "a", "2", "b", "2", "instance", "1")},
			// Now replicating based on the "instance" label.
			{intLabels: labels.FromStrings("__name__", "my_fake_metric", "a", "1", "b", "1", "instance", "2")},
			{intLabels: labels.FromStrings("__name__", "my_fake_metric", "a", "1", "b", "2", "instance", "2")},
			{intLabels: labels.FromStrings("__name__", "my_fake_metric", "a", "2", "b", "1", "instance", "2")},
			{intLabels: labels.FromStrings("__name__", "my_fake_metric", "a", "2", "b", "2", "instance", "2")},
		}
		testutil.Ok(t, remoteWriteSeriesWithLabels(ctx, prom1, samples))
		testutil.Ok(t, remoteWriteSeriesWithLabels(ctx, prom2, samples))
	}

	{
		samples := []seriesWithLabels{
			{intLabels: labels.FromStrings("__name__", "my_fake_metric", "a", "1", "b", "1")},
			{intLabels: labels.FromStrings("__name__", "my_fake_metric", "a", "1", "b", "2")},
			{intLabels: labels.FromStrings("__name__", "my_fake_metric", "a", "2", "b", "1")},
			{intLabels: labels.FromStrings("__name__", "my_fake_metric", "a", "2", "b", "2")},
		}
		testutil.Ok(t, remoteWriteSeriesWithLabels(ctx, prom3, samples))
	}

	query1 := e2ethanos.NewQuerierBuilder(e, "1", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc")).
		WithReplicaLabels("replica", "instance").
		Init()
	query2 := e2ethanos.NewQuerierBuilder(e, "2", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc")).
		WithReplicaLabels("replica").
		Init()
	query3 := e2ethanos.NewQuerierBuilder(e, "3", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc")).
		WithReplicaLabels("instance").
		Init()
	query4 := e2ethanos.NewQuerierBuilder(e, "4", sidecar3.InternalEndpoint("grpc")).
		WithReplicaLabels("a").
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(query1, query2, query3, query4))

	t.Run("deduplication on internal and external labels", func(t *testing.T) {
		// Uses both an internal and external labels as replica labels
		instantQuery(t, ctx, query1.Endpoint("http"), func() string {
			return "my_fake_metric"
		}, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, 4)
	})

	t.Run("deduplication on external label", func(t *testing.T) {
		// Uses "replica" as replica label, which is an external label when being captured by Thanos Sidecar.
		instantQuery(t, ctx, query2.Endpoint("http"), func() string {
			return "my_fake_metric"
		}, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, 8)
	})

	t.Run("deduplication on internal label", func(t *testing.T) {
		// Uses "instance" as replica label, which is an internal label from the samples used.
		instantQuery(t, ctx, query3.Endpoint("http"), func() string {
			return "my_fake_metric"
		}, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, 8)
	})

	t.Run("deduplication on internal label with reorder", func(t *testing.T) {
		// Uses "a" as replica label, which is an internal label from the samples used.
		// This is a regression test for the bug outlined in https://github.com/thanos-io/thanos/issues/6257.
		// Until the bug was fixed, this testcase would return 4 samples instead of 2.
		instantQuery(t, ctx, query4.Endpoint("http"), func() string {
			return "my_fake_metric"
		}, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, 2)
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
			e, err := e2e.NewDockerEnvironment("query-pushdown")
			testutil.Ok(t, err)
			t.Cleanup(e2ethanos.CleanScenario(t, e))

			prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "p1", e2ethanos.DefaultPromConfig("p1", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
			testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1))

			prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "p2", e2ethanos.DefaultPromConfig("p2", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
			testutil.Ok(t, e2e.StartAndWaitReady(prom2, sidecar2))

			endpoints := []string{
				sidecar1.InternalEndpoint("grpc"),
				sidecar2.InternalEndpoint("grpc"),
			}
			q := e2ethanos.
				NewQuerierBuilder(e, "1", endpoints...).
				WithEnabledFeatures([]string{"query-pushdown"}).
				Init()
			testutil.Ok(t, e2e.StartAndWaitReady(q))

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			t.Cleanup(cancel)

			testutil.Ok(t, synthesizeFakeMetricSamples(ctx, prom1, tc.prom1Samples))
			testutil.Ok(t, synthesizeFakeMetricSamples(ctx, prom2, tc.prom2Samples))

			testQuery := func() string { return tc.query }
			queryAndAssert(t, ctx, q.Endpoint("http"), testQuery, time.Now, promclient.QueryOptions{
				Deduplicate: true,
			}, tc.result)
		})
	}
}

// An emptyCtx is never canceled, has no values, and has no deadline. It is not
// struct{}, since vars of this type must have distinct addresses.
type emptyCtx int

func (*emptyCtx) Deadline() (deadline time.Time, ok bool) {
	return
}

func (*emptyCtx) Done() <-chan struct{} {
	return nil
}

func (*emptyCtx) Err() error {
	return nil
}

func (*emptyCtx) Value(key any) any {
	return nil
}

func (e *emptyCtx) String() string {
	return "Context"
}

func checkNetworkRequests(t *testing.T, addr string) {
	ctx, cancel := chromedp.NewContext(new(emptyCtx))
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

func urlParse(t testing.TB, addr string) *url.URL {
	u, err := url.Parse(addr)
	testutil.Ok(t, err)

	return u
}

func instantQuery(t testing.TB, ctx context.Context, addr string, q func() string, ts func() time.Time, opts promclient.QueryOptions, expectedSeriesLen int) (model.Vector, *promclient.Explanation) {
	t.Helper()

	var result model.Vector

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	_ = logger.Log(
		"caller", "instantQuery",
		"msg", fmt.Sprintf("Waiting for %d results for query %s", expectedSeriesLen, q()),
	)

	var explanation *promclient.Explanation
	testutil.Ok(t, runutil.RetryWithLog(logger, 5*time.Second, ctx.Done(), func() error {
		res, rexplanation, err := simpleInstantQuery(t, ctx, addr, q, ts, opts, expectedSeriesLen)
		if err != nil {
			return err
		}
		result = res
		explanation = rexplanation
		return nil
	}))
	sortResults(result)
	return result, explanation
}

func simpleInstantQuery(t testing.TB, ctx context.Context, addr string, q func() string, ts func() time.Time, opts promclient.QueryOptions, expectedSeriesLen int) (model.Vector, *promclient.Explanation, error) {
	res, warnings, explanation, err := promclient.NewDefaultClient().QueryInstant(ctx, urlParse(t, "http://"+addr), q(), ts(), opts)
	if err != nil {
		return nil, nil, err
	}

	if len(warnings) > 0 {
		return nil, nil, errors.Errorf("unexpected warnings %s", warnings)
	}

	if len(res) != expectedSeriesLen {
		return nil, nil, errors.Errorf("unexpected result size, expected %d; result %d: %v", expectedSeriesLen, len(res), res)
	}

	sortResults(res)
	return res, explanation, nil
}

func queryWaitAndAssert(t *testing.T, ctx context.Context, addr string, q func() string, ts func() time.Time, opts promclient.QueryOptions, expected model.Vector) {
	t.Helper()

	var result model.Vector

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	_ = logger.Log(
		"caller", "queryWaitAndAssert",
		"msg", fmt.Sprintf("Waiting for %d results for query %s", len(expected), q()),
	)
	testutil.Ok(t, runutil.RetryWithLog(logger, 10*time.Second, ctx.Done(), func() error {
		res, warnings, _, err := promclient.NewDefaultClient().QueryInstant(ctx, urlParse(t, "http://"+addr), q(), ts(), opts)
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		if len(res) != len(expected) {
			return errors.Errorf("unexpected result size, expected %d; result %d: %v", len(expected), len(res), res)
		}
		result = res
		sortResults(result)
		for _, r := range result {
			r.Timestamp = 0 // Does not matter for us.
		}

		// Retry if not expected result
		if reflect.DeepEqual(expected, result) {
			return nil
		}
		return errors.New("series are different")
	}))

	testutil.Equals(t, expected, result)
}

func queryAndAssertSeries(t *testing.T, ctx context.Context, addr string, q func() string, ts func() time.Time, opts promclient.QueryOptions, expected []model.Metric) {
	t.Helper()

	result, _ := instantQuery(t, ctx, addr, q, ts, opts, len(expected))
	for i, exp := range expected {
		testutil.Equals(t, exp, result[i].Metric)
	}
}

func queryAndAssert(t *testing.T, ctx context.Context, addr string, q func() string, ts func() time.Time, opts promclient.QueryOptions, expected model.Vector) {
	t.Helper()

	sortResults(expected)
	result, _ := instantQuery(t, ctx, addr, q, ts, opts, len(expected))
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
		res, err := promclient.NewDefaultClient().LabelNamesInGRPC(ctx, urlParse(t, "http://"+addr), matchers, start, end)
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
		res, err := promclient.NewDefaultClient().LabelValuesInGRPC(ctx, urlParse(t, "http://"+addr), label, matchers, start, end)
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
		res, err := promclient.NewDefaultClient().SeriesInGRPC(ctx, urlParse(t, "http://"+addr), matchers, start, end)
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
func rangeQuery(t *testing.T, ctx context.Context, addr string, q func() string, start, end, step int64, opts promclient.QueryOptions, check func(res model.Matrix) error) *promclient.Explanation {
	t.Helper()

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	var retExplanation *promclient.Explanation
	testutil.Ok(t, runutil.RetryWithLog(logger, time.Second, ctx.Done(), func() error {
		res, warnings, explanation, err := promclient.NewDefaultClient().QueryRange(ctx, urlParse(t, "http://"+addr), q(), start, end, step, opts)
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		if err := check(res); err != nil {
			return errors.Wrap(err, "result check failed")
		}

		retExplanation = explanation

		return nil
	}))

	return retExplanation
}

func queryExemplars(t *testing.T, ctx context.Context, addr, q string, start, end int64, check func(data []*exemplarspb.ExemplarData) error) {
	t.Helper()

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	u := urlParse(t, "http://"+addr)
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

func synthesizeFakeMetricSamples(ctx context.Context, prometheus *e2emon.InstrumentedRunnable, testSamples []fakeMetricSample) error {
	samples := make([]model.Sample, len(testSamples))
	for i, s := range testSamples {
		samples[i] = newSample(s)
	}

	return synthesizeSamples(ctx, prometheus, samples)
}

func synthesizeSamples(ctx context.Context, prometheus *e2emon.InstrumentedRunnable, samples []model.Sample) error {
	rawRemoteWriteURL := "http://" + prometheus.Endpoint("http") + "/api/v1/write"

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

	writeRequest := &prompb.WriteRequest{
		Timeseries: samplespb,
	}

	return storeWriteRequest(ctx, rawRemoteWriteURL, writeRequest)
}

func remoteWriteSeriesWithLabels(ctx context.Context, prometheus *e2emon.InstrumentedRunnable, series []seriesWithLabels) error {
	rawRemoteWriteURL := "http://" + prometheus.Endpoint("http") + "/api/v1/write"

	samplespb := make([]prompb.TimeSeries, 0, len(series))
	r := rand.New(rand.NewSource(int64(len(series))))
	for _, serie := range series {
		labelspb := make([]prompb.Label, 0, len(serie.intLabels))
		for labelKey, labelValue := range serie.intLabels.Map() {
			labelspb = append(labelspb, prompb.Label{
				Name:  labelKey,
				Value: labelValue,
			})
		}
		samplespb = append(samplespb, prompb.TimeSeries{
			Labels: labelspb,
			Samples: []prompb.Sample{
				{
					Value:     r.Float64(),
					Timestamp: time.Now().Unix() * 1000,
				},
			},
		})
	}

	writeRequest := &prompb.WriteRequest{
		Timeseries: samplespb,
	}

	return storeWriteRequest(ctx, rawRemoteWriteURL, writeRequest)
}

func storeWriteRequest(ctx context.Context, rawRemoteWriteURL string, req *prompb.WriteRequest) error {
	remoteWriteURL, err := url.Parse(rawRemoteWriteURL)
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

	var buf []byte
	pBuf := proto.NewBuffer(nil)
	if err := pBuf.Marshal(req); err != nil {
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
			e, err := e2e.NewDockerEnvironment("pushdown-dedup")
			testutil.Ok(t, err)
			t.Cleanup(e2ethanos.CleanScenario(t, e))

			prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "p1", e2ethanos.DefaultPromConfig("p1", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
			testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1))

			prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "p2", e2ethanos.DefaultPromConfig("p1", 1, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
			testutil.Ok(t, e2e.StartAndWaitReady(prom2, sidecar2))

			endpoints := []string{
				sidecar1.InternalEndpoint("grpc"),
				sidecar2.InternalEndpoint("grpc"),
			}
			q := e2ethanos.
				NewQuerierBuilder(e, "1", endpoints...).
				WithEnabledFeatures([]string{"query-pushdown"}).
				Init()
			testutil.Ok(t, err)
			testutil.Ok(t, e2e.StartAndWaitReady(q))

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			t.Cleanup(cancel)

			testutil.Ok(t, synthesizeFakeMetricSamples(ctx, prom1, tc.prom1Samples))
			testutil.Ok(t, synthesizeFakeMetricSamples(ctx, prom2, tc.prom2Samples))

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

	e, err := e2e.NewDockerEnvironment("pushdown-min-max")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	now := time.Now()

	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "p1", e2ethanos.DefaultPromConfig("p1", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), now.Add(time.Duration(-1)*time.Hour).Format(time.RFC3339), now.Format(time.RFC3339), "remote-write-receiver")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1))

	endpoints := []string{
		sidecar1.InternalEndpoint("grpc"),
	}
	q1 := e2ethanos.
		NewQuerierBuilder(e, "1", endpoints...).
		Init()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q1))
	q2 := e2ethanos.
		NewQuerierBuilder(e, "2", endpoints...).
		WithEnabledFeatures([]string{"query-pushdown"}).
		Init()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q2))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	samples := make([]fakeMetricSample, 0)
	for i := now.Add(time.Duration(-3) * time.Hour); i.Before(now); i = i.Add(30 * time.Second) {
		samples = append(samples, fakeMetricSample{
			label:             "test",
			value:             1,
			timestampUnixNano: i.UnixNano(),
		})
	}

	testutil.Ok(t, synthesizeFakeMetricSamples(ctx, prom1, samples))

	// This query should have identical requests.
	testQuery := func() string { return `max_over_time({instance="test"}[5m])` }

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	var expectedRes model.Matrix
	testutil.Ok(t, runutil.RetryWithLog(logger, time.Second, ctx.Done(), func() error {
		res, warnings, _, err := promclient.NewDefaultClient().QueryRange(ctx, urlParse(t, "http://"+q1.Endpoint("http")), testQuery(),
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

		if len(res) == 0 {
			return errors.Errorf("got empty result")
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

func TestGrpcInstantQuery(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("grpc-api-instant")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	promConfig := e2ethanos.DefaultPromConfig("p1", 0, "", "")
	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "p1", promConfig, "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	endpoints := []string{
		sidecar.InternalEndpoint("grpc"),
	}
	querier := e2ethanos.
		NewQuerierBuilder(e, "1", endpoints...).
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	now := time.Now()
	samples := []fakeMetricSample{
		{
			label:             "test",
			value:             1,
			timestampUnixNano: now.UnixNano(),
		},
		{
			label:             "test",
			value:             2,
			timestampUnixNano: now.Add(time.Hour).UnixNano(),
		},
	}
	ctx := context.Background()
	testutil.Ok(t, synthesizeFakeMetricSamples(ctx, prom, samples))

	grpcConn, err := grpc.Dial(querier.Endpoint("grpc"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	testutil.Ok(t, err)
	queryClient := querypb.NewQueryClient(grpcConn)

	queries := []struct {
		time           time.Time
		expectedResult float64
	}{
		{
			time:           now,
			expectedResult: 1,
		},
		{
			time:           now.Add(time.Hour),
			expectedResult: 2,
		},
	}

	for _, query := range queries {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		err = runutil.Retry(5*time.Second, ctx.Done(), func() error {
			result, err := queryClient.Query(ctx, &querypb.QueryRequest{
				Query:       "my_fake_metric",
				TimeSeconds: query.time.Unix(),
			})

			if err != nil {
				return err
			}

			var warnings string
			var series []*prompb_copy.TimeSeries
			for {
				msg, err := result.Recv()
				if err == io.EOF {
					break
				}

				s := msg.GetTimeseries()
				if s != nil {
					series = append(series, s)
				}
				w := msg.GetWarnings()
				if w != "" {
					warnings = w
				}
			}

			if warnings != "" {
				return fmt.Errorf("got warnings, expected none")
			}

			if len(series) != 1 {
				return fmt.Errorf("got empty result from querier")
			}

			if len(series[0].Samples) != 1 {
				return fmt.Errorf("got empty timeseries from querier")
			}

			if series[0].Samples[0].Value != query.expectedResult {
				return fmt.Errorf("got invalid result from querier")
			}

			return nil
		})
		testutil.Ok(t, err)
		cancel()
	}
}

func TestGrpcQueryRange(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("grpc-api-range")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	promConfig := e2ethanos.DefaultPromConfig("p1", 0, "", "")
	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "p1", promConfig, "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	endpoints := []string{
		sidecar.InternalEndpoint("grpc"),
	}
	querier := e2ethanos.
		NewQuerierBuilder(e, "1", endpoints...).
		Init()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	now := time.Now()
	samples := []fakeMetricSample{
		{
			label:             "test",
			value:             1,
			timestampUnixNano: now.UnixNano(),
		},
		{
			label:             "test",
			value:             2,
			timestampUnixNano: now.Add(time.Second * 15).UnixNano(),
		},
		{
			label:             "test",
			value:             3,
			timestampUnixNano: now.Add(time.Second * 30).UnixNano(),
		},
		{
			label:             "test",
			value:             4,
			timestampUnixNano: now.Add(time.Second * 45).UnixNano(),
		},
		{
			label:             "test",
			value:             5,
			timestampUnixNano: now.Add(time.Minute).UnixNano(),
		},
	}
	ctx := context.Background()
	testutil.Ok(t, synthesizeFakeMetricSamples(ctx, prom, samples))

	grpcConn, err := grpc.Dial(querier.Endpoint("grpc"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	testutil.Ok(t, err)
	queryClient := querypb.NewQueryClient(grpcConn)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	err = runutil.Retry(5*time.Second, ctx.Done(), func() error {
		result, err := queryClient.QueryRange(ctx, &querypb.QueryRangeRequest{
			Query:            "my_fake_metric",
			StartTimeSeconds: now.Unix(),
			EndTimeSeconds:   now.Add(time.Minute).Unix(),
			IntervalSeconds:  15,
		})

		if err != nil {
			return err
		}

		var warnings string
		var series []*prompb_copy.TimeSeries
		for {
			msg, err := result.Recv()
			if err == io.EOF {
				break
			}

			s := msg.GetTimeseries()
			if s != nil {
				series = append(series, s)
			}
			w := msg.GetWarnings()
			if w != "" {
				warnings = w
			}
		}
		if warnings != "" {
			return fmt.Errorf("got warnings, expected none")
		}

		if len(series) != 1 {
			return fmt.Errorf("got empty result from querier")
		}

		if len(series[0].Samples) != 5 {
			return fmt.Errorf("got empty timeseries from querier")
		}

		return nil
	})
	testutil.Ok(t, err)
}

// Repro for https://github.com/thanos-io/thanos/pull/5296#issuecomment-1217875271.
func TestConnectedQueriesWithLazyProxy(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("lazy-proxy")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	promConfig := e2ethanos.DefaultPromConfig("p1", 0, "", "", e2ethanos.LocalPrometheusTarget)
	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "p1", promConfig, "", e2ethanos.DefaultPrometheusImage(), "")

	querier1 := e2ethanos.NewQuerierBuilder(e, "1", sidecar.InternalEndpoint("grpc")).WithProxyStrategy("lazy").WithDisablePartialResponses(true).Init()
	querier2 := e2ethanos.NewQuerierBuilder(e, "2", querier1.InternalEndpoint("grpc")).WithProxyStrategy("lazy").WithDisablePartialResponses(true).Init()

	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar, querier1, querier2))
	testutil.Ok(t, querier2.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

	result, _ := instantQuery(t, context.Background(), querier2.Endpoint("http"), func() string {
		return "sum(up)"
	}, time.Now, promclient.QueryOptions{}, 1)
	testutil.Equals(t, model.SampleValue(1.0), result[0].Value)

	instantQuery(t, context.Background(), querier2.Endpoint("http"), func() string {
		return "sum(metric_that_does_not_exist)"
	}, time.Now, promclient.QueryOptions{}, 0)

}

func TestSidecarPrefersExtLabels(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("sidecar-extlbls")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	promCfg := `global:
  external_labels:
    region: test`

	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "p1", promCfg, "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	endpoints := []string{
		sidecar.InternalEndpoint("grpc"),
	}
	querier := e2ethanos.
		NewQuerierBuilder(e, "1", endpoints...).
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	now := time.Now()
	ctx := context.Background()
	m := model.Sample{
		Metric: map[model.LabelName]model.LabelValue{
			"__name__": "sidecar_test_metric",
			"instance": model.LabelValue("test"),
			"region":   model.LabelValue("foo"),
		},
		Value:     model.SampleValue(2),
		Timestamp: model.TimeFromUnixNano(now.Add(time.Hour).UnixNano()),
	}
	testutil.Ok(t, synthesizeSamples(ctx, prom, []model.Sample{m}))

	retv, _ := instantQuery(t, context.Background(), querier.Endpoint("http"), func() string {
		return "sidecar_test_metric"
	}, func() time.Time { return now.Add(time.Hour) }, promclient.QueryOptions{}, 1)

	testutil.Equals(t, model.Vector{&model.Sample{
		Metric: model.Metric{
			"__name__": "sidecar_test_metric",
			"instance": "test",
			"region":   "test",
		},
		Value:     model.SampleValue(2),
		Timestamp: model.TimeFromUnixNano(now.Add(time.Hour).UnixNano()),
	}}, retv)
}
