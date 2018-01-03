package tracing

import (
	"os"

	"context"

	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/version"
)

const ForceTracingBaggageKey = "X-Thanos-Force-Tracing"

type contextKey struct{}

var tracerKey = contextKey{}

// ContextWithTracer returns a new `context.Context` that holds a reference to given opentracing.Tracer.
func ContextWithTracer(ctx context.Context, tracer opentracing.Tracer) context.Context {
	return context.WithValue(ctx, tracerKey, tracer)
}

func tracerFromContext(ctx context.Context) opentracing.Tracer {
	val := ctx.Value(tracerKey)
	if sp, ok := val.(opentracing.Tracer); ok {
		return sp
	}
	return nil
}

// StartSpan starts and returns span with `operationName` using any Span found within given context.
// It uses traces propagated in context.
func StartSpan(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	tracer := tracerFromContext(ctx)
	if tracer == nil {
		// No tracing found, use noop one.
		tracer = &opentracing.NoopTracer{}
	}

	var span opentracing.Span
	if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
		opts = append(opts, opentracing.ChildOf(parentSpan.Context()))
	}
	span = tracer.StartSpan(operationName, opts...)
	return span, opentracing.ContextWithSpan(ctx, span)
}

type tracer struct {
	debugName string
	wrapped   opentracing.Tracer
}

func (t *tracer) StartSpan(operationName string, opts ...opentracing.StartSpanOption) opentracing.Span {
	span := t.wrapped.StartSpan(operationName, opts...)

	if t.debugName != "" {
		span.SetTag("service_name", t.debugName)
	}

	// Set common tags.
	if hostname := os.Getenv("HOSTNAME"); hostname != "" {
		span.SetTag("hostname", hostname)
	}

	span.SetTag("binary_revision", version.Revision)
	if len(os.Args) > 1 {
		span.SetTag("binary_cmd", os.Args[1])
	}

	return span
}

func (t *tracer) Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	return t.wrapped.Extract(format, carrier)
}

func (t *tracer) Inject(sm opentracing.SpanContext, format interface{}, carrier interface{}) error {
	return t.wrapped.Inject(sm, format, carrier)
}

type forceRecorder struct {
	wrapped basictracer.SpanRecorder
}

// RecordSpan invokes wrapper SpanRecorder only if Sampled field is true or ForceTracingBaggageKey item is set in span's context.
// NOTE(bplotka): Currently only HTTP supports ForceTracingBaggageKey injection on ForceTracingBaggageKey header existence.
func (r *forceRecorder) RecordSpan(sp basictracer.RawSpan) {
	if force := sp.Context.Baggage[ForceTracingBaggageKey]; force != "" {
		sp.Context.Sampled = true
	}

	// All recorder implementation should support handling sp.Context.Sampled.
	r.wrapped.RecordSpan(sp)
}
