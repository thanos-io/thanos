package tracing

import (
	"os"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/version"
)

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
	if len(os.Args) > 0 {
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
