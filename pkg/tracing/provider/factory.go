package provider

import (
	"context"
	"github.com/go-kit/kit/log/level"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/improbable-eng/thanos/pkg/tracing/provider/jaeger"
	"github.com/improbable-eng/thanos/pkg/tracing/provider/stackdriver"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	jaegerTracerType      = "jaeger"
	stackdriverTracerType = "stackdriver"

	envVarTracerType = "THANOS_TRACER_TYPE"
)

// Factory implements tracing.Factory interface as a meta-factory for tracers.
type Factory struct {
	tracingType *string
	factories   map[string]tracing.Factory
}

// NewFactory creates the meta-factory.
func NewFactory() *Factory {
	return &Factory{
		factories: map[string]tracing.Factory{
			jaegerTracerType:      jaeger.NewFactory(),
			stackdriverTracerType: stackdriver.NewFactory(),
		},
	}
}

// Create implements tracing.Factory
func (f *Factory) Create(ctx context.Context, logger log.Logger, serviceName string) (opentracing.Tracer, io.Closer, error) {
	var tracer opentracing.Tracer
	var closer io.Closer
	var err error
	factory, ok := f.factories[*f.tracingType];
	if !ok {
		level.Info(logger).Log("msg", "Invalid tracer type. Tracing will be disabled.", "err", err)
		return noopTracer, noopCloser, nil
	}
	tracer, closer, err = factory.Create(ctx, logger, serviceName)
	if err != nil {
		level.Info(logger).Log("msg", "Tracer create error. Tracing will be disabled.", "err", err)
		return noopTracer, noopCloser, nil
	}
	return tracer, closer, nil
}

// RegisterKingpinFlags implements tracing.Factory.
func (f *Factory) RegisterKingpinFlags(app *kingpin.Application) {
	f.tracingType = app.Flag("tracing.type", "gcloud/jaeger.").Default("").Envar(envVarTracerType).String()
	for _, t := range f.factories {
		t.RegisterKingpinFlags(app)
	}
}
