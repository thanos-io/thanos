// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package migration

import (
	"context"
	"io"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/opentracing-go"
	"go.opentelemetry.io/otel"
	bridge "go.opentelemetry.io/otel/bridge/opentracing"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Bridge is a method to facilitate migration from OpenTracing (OT) to
// OpenTelemetry (OTEL). It pairs an OTEL tracer with a so-called bridge
// tracer, which satisfies the OT Tracer interface. This makes it possible
// for OT instrumentation to work with an OTEL tracer.
//
// NOTE: After instrumentation migration is finished, this bridge should be
// removed.
func Bridge(tp *tracesdk.TracerProvider, l log.Logger) (opentracing.Tracer, io.Closer) {
	otel.SetErrorHandler(otelErrHandler(func(err error) {
		level.Error(l).Log("msg", "OpenTelemetry ErrorHandler", "err", err)
	}))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(tp)

	bridgeTracer, _ := bridge.NewTracerPair(tp.Tracer(""))
	bridgeTracer.SetWarningHandler(func(warn string) {
		level.Warn(l).Log("msg", "OpenTelemetry BridgeWarningHandler", "warn", warn)
	})

	tpShutdownFunc := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		return tp.Shutdown(ctx)
	}

	return bridgeTracer, shutdownAsCloser(tpShutdownFunc)
}

func GetTraceIDFromBridgeSpan(span opentracing.Span) (string, bool) {
	ctx := bridge.NewBridgeTracer().ContextWithSpanHook(context.Background(), span)
	otelSpan := trace.SpanFromContext(ctx)
	if otelSpan.SpanContext().IsSampled() && otelSpan.SpanContext().IsValid() {
		return otelSpan.SpanContext().TraceID().String(), true
	}

	return "", false
}

type otelErrHandler func(err error)

func (o otelErrHandler) Handle(err error) {
	o(err)
}

// Workaround to satisfy io.Closer interface.
type shutdownAsCloser func() error

func (s shutdownAsCloser) Close() error {
	return s()
}
