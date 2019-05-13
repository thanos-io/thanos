package provider

import (
	"context"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/improbable-eng/thanos/pkg/tracing/provider/gcloud"
	"github.com/improbable-eng/thanos/pkg/tracing/provider/jaeger"
	"gopkg.in/alecthomas/kingpin.v2"
	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
)

const (
	jaegerTracingType = "jaeger"
	gcloudTracingType = "gcloud"
)

// Factory - tracer factory.
type Factory struct {
	tracingType *string
	factories map[string]tracing.Factory
}

// NewFactory return new tracer factory.
func NewFactory() (*Factory) {
	f := &Factory{}
	f.factories = make(map[string]tracing.Factory)

	f.factories[jaegerTracingType] = jaeger.NewFactory()
	f.factories[gcloudTracingType] = gcloud.NewFactory()

	return f
}

// Create implement factoty.Factory
func (f *Factory) Create(ctx context.Context, logger log.Logger, debugName string) (opentracing.Tracer, func() error) {
	return f.factories[*f.tracingType].Create(ctx, logger, debugName)
}

func (f *Factory) RegisterKingpinFlags(app *kingpin.Application) {
	f.tracingType = app.Flag("tracing.type", "gcloud/jaeger.").Default("jaeger").String()
	for _, t := range f.factories {
		t.RegisterKingpinFlags(app)
	}
}