// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	e2eobs "github.com/efficientgo/e2e/observable"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/tracing/client"
	jaegercfg "github.com/thanos-io/thanos/pkg/tracing/jaeger"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
	"gopkg.in/yaml.v2"
)

// jaegerResponse is a minimal representation of the Jaeger /api/traces response.
type jaegerResponse struct {
	Data []jaegerTrace `json:"data"`
}

type jaegerTrace struct {
	Spans []jaegerSpan `json:"spans"`
}

type jaegerSpan struct {
	OperationName string      `json:"operationName"`
	Tags          []jaegerTag `json:"tags"`
}

type jaegerTag struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

// findTag returns the tag with the given key, or nil.
func (s *jaegerSpan) findTag(key string) *jaegerTag {
	for i := range s.Tags {
		if s.Tags[i].Key == key {
			return &s.Tags[i]
		}
	}
	return nil
}

// startJaeger creates and starts a Jaeger all-in-one container.
func startJaeger(t *testing.T, env e2e.Environment, name string) *e2eobs.Observable {
	t.Helper()
	r := env.Runnable(fmt.Sprintf("jaeger-%s", name)).
		WithPorts(map[string]int{
			"http":                      16686,
			"http.admin":                14269,
			"jaeger.thrift-model.proto": 14250,
			"jaeger.thrift":             14268,
		}).
		Init(e2e.StartOptions{
			Image:     "jaegertracing/all-in-one:1.33",
			Readiness: e2e.NewHTTPReadinessProbe("http.admin", "/", 200, 200),
		})
	j := e2eobs.AsObservable(r, "http.admin")
	testutil.Ok(t, e2e.StartAndWaitReady(j))
	return j
}

// jaegerTracingConfig returns a YAML tracing config for the given Jaeger and service name.
func jaegerTracingConfig(jaeger *e2eobs.Observable, serviceName string) string {
	cfg, _ := yaml.Marshal(client.TracingConfig{
		Type: client.Jaeger,
		Config: jaegercfg.Config{
			ServiceName:  serviceName,
			SamplerType:  "const",
			SamplerParam: 1,
			Endpoint:     "http://" + jaeger.InternalEndpoint("jaeger.thrift") + "/api/traces",
		},
	})
	return string(cfg)
}

// queryJaegerSpans fetches spans from Jaeger for the given service and operation,
// retrying until at least one span is found or the context expires.
func queryJaegerSpans(t *testing.T, ctx context.Context, jaegerEndpoint, service, operation string) []jaegerSpan {
	t.Helper()

	u := fmt.Sprintf("http://%s/api/traces?service=%s&operation=%s&limit=20",
		strings.TrimSpace(jaegerEndpoint), service, operation)
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	testutil.Ok(t, err)

	var spans []jaegerSpan
	httpClient := &http.Client{}

	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() error {
		resp, err := httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("jaeger returned %d", resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		var jr jaegerResponse
		if err := json.Unmarshal(body, &jr); err != nil {
			return err
		}
		if len(jr.Data) == 0 {
			return errors.New("no traces returned yet")
		}
		spans = nil
		for _, trace := range jr.Data {
			for _, s := range trace.Spans {
				if s.OperationName == operation {
					spans = append(spans, s)
				}
			}
		}
		if len(spans) == 0 {
			return errors.Errorf("no spans with operation %q found yet", operation)
		}
		return nil
	}))
	return spans
}

// queryJaegerSpansWithTag fetches all traces from Jaeger for the given service
// and returns spans that contain a tag with the given key and a positive numeric
// value. This avoids needing to know the exact operation name (which can vary or
// contain special characters) and skips spans from early requests that returned
// zero results.
func queryJaegerSpansWithTag(t *testing.T, ctx context.Context, jaegerEndpoint, service, tagKey string) []jaegerSpan {
	t.Helper()

	u := fmt.Sprintf("http://%s/api/traces?service=%s&limit=50",
		strings.TrimSpace(jaegerEndpoint), service)

	var spans []jaegerSpan
	httpClient := &http.Client{}

	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() error {
		req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
		if err != nil {
			return err
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("jaeger returned %d", resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		var jr jaegerResponse
		if err := json.Unmarshal(body, &jr); err != nil {
			return err
		}
		if len(jr.Data) == 0 {
			return errors.New("no traces returned yet")
		}
		spans = nil
		for _, trace := range jr.Data {
			for _, s := range trace.Spans {
				if tag := s.findTag(tagKey); tag != nil && tagNumericValue(tag) > 0 {
					spans = append(spans, s)
				}
			}
		}
		if len(spans) == 0 {
			return errors.Errorf("no spans with tag %q > 0 found yet", tagKey)
		}
		return nil
	}))
	return spans
}

// Test to check if the trace provider works as expected.
func TestJaegerTracing(t *testing.T) {
	env, err := e2e.NewDockerEnvironment("e2e-tracing-test")
	testutil.Ok(t, err)
	t.Cleanup(env.Close)

	j := startJaeger(t, env, "testing")

	prom1, sidecar1 := e2ethanos.NewPrometheusWithJaegerTracingSidecarCustomImage(
		env, "alone",
		e2ethanos.DefaultPromConfig("prom-alone", 0, "", "", e2ethanos.LocalPrometheusTarget), "",
		e2ethanos.DefaultPrometheusImage(), "",
		e2ethanos.DefaultImage(), jaegerTracingConfig(j, "thanos-sidecar"), "",
	)
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1))

	q := e2ethanos.NewQuerierBuilder(env, "1", sidecar1.InternalEndpoint("grpc")).
		WithTracingConfig(fmt.Sprintf(`type: JAEGER
config:
  sampler_type: const
  sampler_param: 1
  service_name: thanos-query
  endpoint: %s`, "http://"+j.InternalEndpoint("jaeger.thrift")+"/api/traces")).
		Init()
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

	jaegerHTTP := j.Endpoint("http")

	// Original assertions: verify traces exist with the expected service names.
	spans := queryJaegerSpans(t, ctx, jaegerHTTP, "thanos-query", "proxy.series")
	testutil.Assert(t, len(spans) > 0, "expected proxy.series spans")
}

// TestTracingAttributes verifies that the span attributes added by the
// tracing-part-1 branch appear on traces collected in Jaeger.
//
// Attributes under test:
//   - series.selector, result.series, result.samples  (proxy.series)
//   - result.wire_bytes                                (HTTP server span)
func TestTracingAttributes(t *testing.T) {
	env, err := e2e.NewDockerEnvironment("e2e-trace-attrs")
	testutil.Ok(t, err)
	t.Cleanup(env.Close)

	j := startJaeger(t, env, "attrs")

	prom, sidecar := e2ethanos.NewPrometheusWithJaegerTracingSidecarCustomImage(
		env, "attr",
		e2ethanos.DefaultPromConfig("prom-attr", 0, "", "", e2ethanos.LocalPrometheusTarget), "",
		e2ethanos.DefaultPrometheusImage(), "",
		e2ethanos.DefaultImage(), jaegerTracingConfig(j, "thanos-sidecar"), "",
	)
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	q := e2ethanos.NewQuerierBuilder(env, "1", sidecar.InternalEndpoint("grpc")).
		WithTracingConfig(fmt.Sprintf(`type: JAEGER
config:
  sampler_type: const
  sampler_param: 1
  service_name: thanos-query
  endpoint: %s`, "http://"+j.InternalEndpoint("jaeger.thrift")+"/api/traces")).
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// Wait for the "up" metric to be queryable.
	queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "prom-attr",
			"replica":    "0",
		},
	})

	jaegerHTTP := j.Endpoint("http")

	t.Run("proxy_series_attributes", func(t *testing.T) {
		// Find spans with result.series > 0 (skips early requests before data was available).
		spans := queryJaegerSpansWithTag(t, ctx, jaegerHTTP, "thanos-query", "result.series")
		testutil.Assert(t, len(spans) > 0, "expected spans with result.series > 0")

		span := spans[0]

		tag := span.findTag("series.selector")
		testutil.Assert(t, tag != nil, "series.selector tag should be present")

		tag = span.findTag("result.series")
		testutil.Assert(t, tag != nil, "result.series tag should be present")
		testutil.Assert(t, tagNumericValue(tag) > 0, "result.series should be > 0")

		tag = span.findTag("result.samples")
		testutil.Assert(t, tag != nil, "result.samples tag should be present")
		testutil.Assert(t, tagNumericValue(tag) >= 0, "result.samples should be >= 0")
	})

	t.Run("http_wire_bytes", func(t *testing.T) {
		spans := queryJaegerSpansWithTag(t, ctx, jaegerHTTP, "thanos-query", "result.wire_bytes")
		testutil.Assert(t, len(spans) > 0, "expected spans with result.wire_bytes > 0")

		span := spans[0]

		tag := span.findTag("result.wire_bytes")
		testutil.Assert(t, tag != nil, "result.wire_bytes tag should be present")
		testutil.Assert(t, tagNumericValue(tag) > 0, "result.wire_bytes should be > 0")
	})
}

// tagNumericValue extracts a numeric value from a Jaeger tag.
// Jaeger encodes int64 tags with type "int64" and a string value,
// and JSON numbers may decode as float64 or json.Number.
func tagNumericValue(tag *jaegerTag) float64 {
	if tag == nil {
		return 0
	}
	switch v := tag.Value.(type) {
	case float64:
		return v
	case json.Number:
		f, _ := v.Float64()
		return f
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0
		}
		return f
	default:
		return 0
	}
}
