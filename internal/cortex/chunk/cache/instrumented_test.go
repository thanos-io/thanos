// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	bridge "go.opentelemetry.io/otel/bridge/opentracing"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestInstrumentedCacheDoesNotEmitEmptySpanEvents(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := tracesdk.NewTracerProvider(tracesdk.WithSpanProcessor(recorder))
	tracer, _ := bridge.NewTracerPair(provider.Tracer(""))
	previousTracer := opentracing.GlobalTracer()
	opentracing.SetGlobalTracer(tracer)
	t.Cleanup(func() {
		opentracing.SetGlobalTracer(previousTracer)
		require.NoError(t, provider.Shutdown(context.Background()))
	})

	span := tracer.StartSpan("cache")
	ctx := opentracing.ContextWithSpan(context.Background(), span)
	c := Instrument("test", NewMockCache(), prometheus.NewRegistry())
	c.Store(ctx, []string{"key"}, [][]byte{[]byte("value")})
	c.Fetch(ctx, []string{"key", "missing"})
	span.Finish()

	recordedSpans := recorder.Ended()
	require.NotEmpty(t, recordedSpans)
	for _, recordedSpan := range recordedSpans {
		for _, event := range recordedSpan.Events() {
			require.NotEmpty(t, event.Name)
		}
	}
}
