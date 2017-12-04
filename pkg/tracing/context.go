package tracing

import (
	"context"

	"github.com/opentracing/opentracing-go"
)

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

// StartSpanFromContext starts and returns span with `operationName` using any Span found within given context.
// It uses traces propagated in context.
func StartSpanFromContext(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
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
