// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.
package e2e_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/tracing/client"
	"github.com/thanos-io/thanos/pkg/tracing/jaeger"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"

	"github.com/efficientgo/e2e"
	"github.com/efficientgo/tools/core/pkg/backoff"
	"github.com/efficientgo/tools/core/pkg/testutil"
	"gopkg.in/yaml.v2"
)

// Test to check if the trace provider works as expected.
func TestJaegerTracing(t *testing.T) {
	env, err := e2e.NewDockerEnvironment("e2e-tracing-test")
	testutil.Ok(t, err)
	t.Cleanup(env.Close)
	name := "testing"
	newJaeger := e2e.NewInstrumentedRunnable(env, fmt.Sprintf("jaeger-%s", name)).
		WithPorts(
			map[string]int{
				"http":                      16686,
				"http.admin":                14269,
				"jaeger.thrift-model.proto": 14250,
				"jaeger.thrift":             14268,
			}, "http.admin").
		Init(e2e.StartOptions{
			Image:     "jaegertracing/all-in-one:1.33",
			Readiness: e2e.NewHTTPReadinessProbe("http.admin", "/", 200, 200),
		})
	testutil.Ok(t, e2e.StartAndWaitReady(newJaeger))

	jaegerConfig, err := yaml.Marshal(client.TracingConfig{
		Type: client.Jaeger,
		Config: jaeger.Config{
			ServiceName:  "thanos",
			SamplerType:  "const",
			SamplerParam: 1,
			Endpoint:     "http://" + newJaeger.InternalEndpoint("jaeger.thrift") + "/api/traces", // make this a var
		},
	})
	testutil.Ok(t, err)

	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecarCustomImage(env, "alone", e2ethanos.DefaultPromConfig("prom-alone", 0, "", "", e2ethanos.LocalPrometheusTarget), "",
		e2ethanos.DefaultPrometheusImage(), "", e2ethanos.DefaultImage(), string(jaegerConfig), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1))

	qb := e2ethanos.NewQuerierBuilder(env, "1", sidecar1.InternalEndpoint("grpc"))
	q := qb.WithTracingConfig(fmt.Sprintf(`type: JAEGER
config:
  sampler_type: const
  sampler_param: 1
  service_name: %s
  endpoint: %s`, qb.Name(), "http://"+newJaeger.InternalEndpoint("jaeger.thrift")+"/api/traces")).Init()
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

	url := "http://" + strings.TrimSpace(newJaeger.Endpoint("http")+"/api/traces?service=thanos") // pretty print
	// fmt.Printf("\n\n JAEGER URL: %s", url) // --> gives the correct URL displaying 'thanos' traces.
	request, err := http.NewRequest("GET", url, nil)
	testutil.Ok(t, err)
	client := &http.Client{}

	ctx = context.Background()
	b := backoff.New(ctx, backoff.Config{
		Min:        500 * time.Millisecond,
		Max:        5 * time.Second,
		MaxRetries: 10,
	})
	for b.Reset(); b.Ongoing(); {
		response, err := client.Do(request)
		// Retry if we have a connection problem (timeout, etc)
		if err != nil {
			b.Wait()
			continue
		}

		// Jaeger might give a 404 or 500 before the trace is there.  Retry.
		if response.StatusCode != http.StatusOK {
			t.Logf("\n\n WAITING")
			b.Wait()
			continue
		}

		// We got a 200 response.
		defer response.Body.Close()

		body, err := ioutil.ReadAll(response.Body)
		testutil.Ok(t, err)

		t.Logf("\n\n the resp is: %s", string(body))
	}

}
