package dapperish

import (
	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
)

// NewTracer returns a new dapperish Tracer instance.
func NewTracer(processName string) opentracing.Tracer {
	return basictracer.New(NewTrivialRecorder(processName))
}
