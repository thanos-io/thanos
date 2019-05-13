package gcloud

import (
	"context"
	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type Factory struct {
	gcloudTraceProjectID *string
	sampleFactor *uint64
}

func NewFactory() *Factory {
	return &Factory{}
}

func (f *Factory) Create(ctx context.Context, logger log.Logger, debugName string) (opentracing.Tracer, func() error) {
	return NewOptionalGCloudTracer(ctx, logger, *f.gcloudTraceProjectID, *f.sampleFactor, debugName)
}

func (f *Factory) RegisterKingpinFlags(app *kingpin.Application) {
	f.gcloudTraceProjectID = app.Flag("gcloudtrace.project", "GCP project to send Google Cloud Trace tracings to. If empty, tracing will be disabled.").Default("").String()
	f.sampleFactor = app.Flag("gcloudtrace.sample-factor", "How often we send traces (1/<sample-factor>). If 0 no trace will be sent periodically, unless forced by baggage item. See `pkg/tracing/tracing.go` for details.").Default("1").Uint64()
}