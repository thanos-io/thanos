package noop

import "github.com/opentracing/opentracing-go"

type Tracer struct {
	opentracing.NoopTracer
}

func (t *Tracer) Close() error {
	return nil
}
