// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.
package e2e_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/thanos-io/thanos/pkg/tracing/jaeger"

	"github.com/efficientgo/e2e"
	"github.com/efficientgo/tools/core/pkg/testutil"
	"github.com/go-kit/log"
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
			}, "http.admin").
		Init(e2e.StartOptions{
			Image:     "jaegertracing/all-in-one:1.33",
			Readiness: e2e.NewHTTPReadinessProbe("http.admin", "/", 200, 200),
		})
	testutil.Ok(t, e2e.StartAndWaitReady(newJaeger))

	var logger log.Logger
	ctx := context.Background()
	config := &jaeger.Config{
		ServiceName: "test-service",
		Endpoint:    newJaeger.Endpoint("jaeger.thrift-model.proto"),
	}
	data, err := yaml.Marshal(config)
	testutil.Ok(t, err)

	_, err = jaeger.NewTracerProvider(ctx, logger, data)
	testutil.Ok(t, err)
}
