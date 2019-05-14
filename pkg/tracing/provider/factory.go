package provider

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/improbable-eng/thanos/pkg/tracing/provider/gcloud"
	"github.com/improbable-eng/thanos/pkg/tracing/provider/jaeger"
	"github.com/improbable-eng/thanos/pkg/tracing/provider/noop"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	jaegerTraceType = "jaeger"
	gcloudTraceType = "gcloud"
	noopTraceType     = "noop"
)

// Factory - tracer factory.
type Factory struct {
	tracingType *string
	factories   map[string]tracing.Factory
}

// NewFactory return new tracer factory.
func NewFactory() *Factory {
	f := &Factory{}
	f.factories = make(map[string]tracing.Factory)

	f.factories[jaegerTraceType] = jaeger.NewFactory()
	f.factories[gcloudTraceType] = gcloud.NewFactory()
	f.factories[noopTraceType] = noop.NewFactory()

	return f
}

// Create implement factoty.Factory
func (f *Factory) Create(ctx context.Context, logger log.Logger, serviceName string) (opentracing.Tracer, io.Closer) {
	return f.factories[*f.tracingType].Create(ctx, logger, serviceName)
}

func (f *Factory) RegisterKingpinFlags(app *kingpin.Application) {
	f.tracingType = app.Flag("tracing.type", "gcloud/jaeger.").Default("noop").String()
	for _, t := range f.factories {
		t.RegisterKingpinFlags(app)
	}
}
