package provider

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/improbable-eng/thanos/pkg/tracing/provider/jaeger"
	"github.com/improbable-eng/thanos/pkg/tracing/provider/noop"
	"github.com/improbable-eng/thanos/pkg/tracing/provider/stackdriver"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	jaegerTracerType      = "jaeger"
	stackdriverTracerType = "stackdriver"
	noopTracerType        = "noop"

	envVarTracerType = "THANOS_TRACER_TYPE"
)

// Factory - tracer factory.
type Factory struct {
	tracingType *string
	factories   map[string]tracing.Factory
}

// NewFactory return new tracer factory.
func NewFactory() *Factory {
	return &Factory{
		factories: map[string]tracing.Factory{
			jaegerTracerType:      jaeger.NewFactory(),
			stackdriverTracerType: stackdriver.NewFactory(),
			noopTracerType:        noop.NewFactory(),
		},
	}
}

// Create implement factoty.Factory
func (f *Factory) Create(ctx context.Context, logger log.Logger, serviceName string) (opentracing.Tracer, io.Closer) {
	return f.factories[*f.tracingType].Create(ctx, logger, serviceName)
}

func (f *Factory) RegisterKingpinFlags(app *kingpin.Application) {
	f.tracingType = app.Flag("tracing.type", "gcloud/jaeger.").Default(noopTracerType).Envar(envVarTracerType).String()
	for _, t := range f.factories {
		t.RegisterKingpinFlags(app)
	}
}
