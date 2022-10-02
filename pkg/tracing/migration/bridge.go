// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package migration

import (
	"context"
	"io"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go"
	"go.opentelemetry.io/contrib/propagators/autoprop"
	"go.opentelemetry.io/otel"
	bridge "go.opentelemetry.io/otel/bridge/opentracing"
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
	otel.SetTextMapPropagator(autoprop.NewTextMapPropagator())
	otel.SetTracerProvider(tp)

	bridgeTracer, _ := bridge.NewTracerPair(tp.Tracer(""))
	bridgeTracer.SetWarningHandler(func(warn string) {
		level.Warn(l).Log("msg", "OpenTelemetry BridgeWarningHandler", "warn", warn)
	})
	bridgeTracer.SetTextMapPropagator(autoprop.NewTextMapPropagator())

	tpShutdownFunc := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		return tp.Shutdown(ctx)
	}

	return &bridgeTracerWrapper{bt: bridgeTracer}, shutdownAsCloser(tpShutdownFunc)
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

// This wrapper is necessary to enable proper trace propagation for gRPC
// calls between components. The bridge.BridgeTracer currently supports injection /
// extraction of only single carrier type which is opentracing.HTTPHeadersCarrier.
// (see https://github.com/open-telemetry/opentelemetry-go/blob/main/bridge/opentracing/bridge.go#L626)
//
// To work around this, this wrapper extends Inject / Extract methods to "convert"
// other carrier types to opentracing.HTTPHeadersCarrier, in order to propagate
// data correctly. This is currently, at minimum, required for proper functioning
// of propagation in the gRPC middleware, which uses metadata.MD as a carrier.
// (see https://github.com/grpc-ecosystem/go-grpc-middleware/blob/v2.0.0-rc.2/interceptors/tracing/client.go#L95)
type bridgeTracerWrapper struct {
	bt *bridge.BridgeTracer
}

func (b *bridgeTracerWrapper) StartSpan(operationName string, opts ...opentracing.StartSpanOption) opentracing.Span {
	return b.bt.StartSpan(operationName, opts...)
}

func (b *bridgeTracerWrapper) Inject(sm opentracing.SpanContext, format interface{}, carrier interface{}) error {
	otCarrier := opentracing.HTTPHeadersCarrier{}
	err := b.bt.Inject(sm, format, otCarrier)
	if err != nil {
		return err
	}

	if tmw, ok := carrier.(opentracing.TextMapWriter); ok {
		err := otCarrier.ForeachKey(func(key, val string) error {
			tmw.Set(key, val)
			return nil
		})
		if err != nil {
			return err
		}
	}

	return b.bt.Inject(sm, format, carrier)
}

func (b *bridgeTracerWrapper) Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	if tmr, ok := carrier.(opentracing.TextMapReader); ok {
		otCarrier := opentracing.HTTPHeadersCarrier{}
		err := tmr.ForeachKey(func(key, val string) error {
			otCarrier.Set(key, val)
			return nil
		})
		if err != nil {
			return nil, err
		}

		return b.bt.Extract(format, otCarrier)
	}

	return b.bt.Extract(format, carrier)
}
