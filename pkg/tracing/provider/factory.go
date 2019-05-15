package provider

import (
	"context"
	"github.com/go-kit/kit/log/level"
	"io"
	"io/ioutil"

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

// Factory is a meta-factory for tracers.
type Factory struct {
	tracingType *string
	factories   map[string]tracing.Factory
}

// NewFactory creates the meta-factory.
func NewFactory(app *kingpin.Application) *Factory {
	f :=  &Factory{
		factories: map[string]tracing.Factory{
			jaegerTracerType:      jaeger.NewFactory(),
			stackdriverTracerType: stackdriver.NewFactory(),
		},
	}
	f.registerKingpinFlags(app)
	return f
}

// Create creates the tracer in appliance with a tracerType
func (f *Factory) Create(ctx context.Context, logger log.Logger, serviceName string) (opentracing.Tracer, io.Closer) {
	var tracer opentracing.Tracer
	var closer io.Closer
	var err error
	factory, ok := f.factories[*f.tracingType];
	if !ok {
		level.Info(logger).Log("msg", "Invalid tracer type. Tracing will be disabled.", "err", err)
		return &opentracing.NoopTracer{}, ioutil.NopCloser(nil)
	}
	tracer, closer, err = factory.Create(ctx, logger, serviceName)
	if err != nil {
		level.Info(logger).Log("msg", "Tracer create error. Tracing will be disabled.", "err", err)
		return &opentracing.NoopTracer{}, ioutil.NopCloser(nil)
	}
	return tracer, closer
}

// registerKingpinFlags registers kingpinFlags for every tracer type.
func (f *Factory) registerKingpinFlags(app *kingpin.Application) {
	f.tracingType = app.Flag("tracing.type", "gcloud/jaeger.").Default("").Envar(envVarTracerType).String()
	for _, t := range f.factories {
		t.RegisterKingpinFlags(app)
	}
}
