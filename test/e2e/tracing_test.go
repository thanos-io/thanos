// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/tracing/client"
	"github.com/thanos-io/thanos/pkg/tracing/jaeger"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	e2eobs "github.com/efficientgo/e2e/observable"
	"gopkg.in/yaml.v2"
)

// Test to check if the trace provider works as expected.
func TestJaegerTracing(t *testing.T) {
	env, err := e2e.NewDockerEnvironment("e2e-tracing-test")
	testutil.Ok(t, err)
	t.Cleanup(env.Close)
	name := "testing"
	newJaegerRunnable := env.Runnable(fmt.Sprintf("jaeger-%s", name)).
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
	newJaeger := e2eobs.AsObservable(newJaegerRunnable, "http.admin")
	testutil.Ok(t, e2e.StartAndWaitReady(newJaeger))

	jaegerConfig, err := yaml.Marshal(client.TracingConfig{
		Type: client.Jaeger,
		Config: jaeger.Config{
			ServiceName:  "thanos-sidecar",
			SamplerType:  "const",
			SamplerParam: 1,
			Endpoint:     "http://" + newJaeger.InternalEndpoint("jaeger.thrift") + "/api/traces",
		},
	})
	testutil.Ok(t, err)

	prom1, sidecar1 := e2ethanos.NewPrometheusWithJaegerTracingSidecarCustomImage(env, "alone", e2ethanos.DefaultPromConfig("prom-alone", 0, "", "", e2ethanos.LocalPrometheusTarget), "",
		e2ethanos.DefaultPrometheusImage(), "", e2ethanos.DefaultImage(), string(jaegerConfig), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1))

	qb := e2ethanos.NewQuerierBuilder(env, "1", sidecar1.InternalEndpoint("grpc"))
	q := qb.WithTracingConfig(fmt.Sprintf(`type: JAEGER
config:
  sampler_type: const
  sampler_param: 1
  service_name: thanos-query
  endpoint: %s`, "http://"+newJaeger.InternalEndpoint("jaeger.thrift")+"/api/traces")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "prom-alone",
			"replica":    "0",
		},
	})

	url := "http://" + strings.TrimSpace(newJaeger.Endpoint("http")+"/api/traces?service=thanos-query&operation=proxy.series")
	request, err := http.NewRequest("GET", url, nil)
	testutil.Ok(t, err)
	client := &http.Client{}

	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() error {
		response, err := client.Do(request)
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
		testutil.Assert(t, strings.Contains(resp, `"serviceName":"thanos-sidecar"`))
		return nil
	}))
}
