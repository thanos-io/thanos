// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// This file includes unit tests that test only tiny logic in this package, but are here mainly as a showcase on how tracing can
// be configured.

package google_cloud

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
	"github.com/thanos-io/thanos/pkg/tracing"
	"github.com/thanos-io/thanos/pkg/tracing/migration"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestMain(m *testing.M) {
	custom.TolerantVerifyLeakMain(m)
}

// This test shows that if sample factor will enable tracing on client process, even when it would be disabled on server
// it will be still enabled for all spans within this span.
func TestContextTracing_ClientEnablesTracing(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tracerOtel := newTracerProvider(
		context.Background(),
		log.NewNopLogger(),
		tracesdk.NewSimpleSpanProcessor(exp),
		1, // always sample
		"gcloud-test-client",
	)
	tracer, _ := migration.Bridge(tracerOtel, log.NewNopLogger())

	clientRoot, clientCtx := tracing.StartSpan(tracing.ContextWithTracer(context.Background(), tracer), "a")

	// Simulate Server process with different tracer, but with client span in context.
	srvTracerOtel := newTracerProvider(
		context.Background(),
		log.NewNopLogger(),
		tracesdk.NewSimpleSpanProcessor(exp),
		0, // never sample
		"gcloud-test-server",
	)
	srvTracer, _ := migration.Bridge(srvTracerOtel, log.NewNopLogger())

	srvRoot, srvCtx := tracing.StartSpan(tracing.ContextWithTracer(clientCtx, srvTracer), "b")
	srvChild, _ := tracing.StartSpan(srvCtx, "bb")

	tracing.CountSpans_ClientEnablesTracing(t, exp, clientRoot, srvRoot, srvChild)
}

// This test shows that if sample factor will disable tracing on client process,  when it would be enabled on server
// it will be still disabled for all spans within this span.
func TestContextTracing_ClientDisablesTracing(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tracerOtel := newTracerProvider(
		context.Background(),
		log.NewNopLogger(),
		tracesdk.NewSimpleSpanProcessor(exp),
		0, // never sample
		"gcloud-test-client",
	)
	tracer, _ := migration.Bridge(tracerOtel, log.NewNopLogger())

	clientRoot, clientCtx := tracing.StartSpan(tracing.ContextWithTracer(context.Background(), tracer), "a")

	// Simulate Server process with different tracer, but with client span in context.
	srvTracerOtel := newTracerProvider(
		context.Background(),
		log.NewNopLogger(),
		tracesdk.NewSimpleSpanProcessor(exp),
		0, // never sample
		"gcloud-test-server",
	)
	srvTracer, _ := migration.Bridge(srvTracerOtel, log.NewNopLogger())

	srvRoot, srvCtx := tracing.StartSpan(tracing.ContextWithTracer(clientCtx, srvTracer), "b")
	srvChild, _ := tracing.StartSpan(srvCtx, "bb")

	tracing.ContextTracing_ClientDisablesTracing(t, exp, clientRoot, srvRoot, srvChild)
}

// This test shows that if span will contain special baggage (for example from special HTTP header), even when sample
// factor will disable client & server tracing, it will be still enabled for all spans within this span.
func TestContextTracing_ForceTracing(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tracerOtel := newTracerProvider(
		context.Background(),
		log.NewNopLogger(),
		tracesdk.NewSimpleSpanProcessor(exp),
		0, // never sample
		"gcloud-test-client",
	)
	tracer, _ := migration.Bridge(tracerOtel, log.NewNopLogger())

	// Start the root span with the tag to force tracing.
	clientRoot, clientCtx := tracing.StartSpan(
		tracing.ContextWithTracer(context.Background(), tracer),
		"a",
		opentracing.Tag{Key: migration.ForceTracingAttributeKey, Value: "true"},
	)

	// Simulate Server process with different tracer, but with client span in context.
	srvTracerOtel := newTracerProvider(
		context.Background(),
		log.NewNopLogger(),
		tracesdk.NewSimpleSpanProcessor(exp),
		0, // never sample
		"gcloud-test-server",
	)
	srvTracer, _ := migration.Bridge(srvTracerOtel, log.NewNopLogger())

	srvRoot, srvCtx := tracing.StartSpan(tracing.ContextWithTracer(clientCtx, srvTracer), "b")
	srvChild, _ := tracing.StartSpan(srvCtx, "bb")

	tracing.ContextTracing_ForceTracing(t, exp, clientRoot, srvRoot, srvChild)
}
