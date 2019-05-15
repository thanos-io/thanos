package provider

import "github.com/opentracing/opentracing-go"

var (
	noopTracer = opentracing.NoopTracer{}
	noopCloser = &noopTracerCloser{}
)

type noopTracerCloser struct {
}

func (t *noopTracerCloser) Close() error {
	return nil
}
