package gcloud

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/tracing/provider/noop"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type Factory struct {
	gcloudTraceProjectID *string
	sampleFactor         *uint64
}

func NewFactory() *Factory {
	return &Factory{}
}

func (f *Factory) Create(ctx context.Context, logger log.Logger, serviceName string) (opentracing.Tracer, io.Closer) {
	if *f.gcloudTraceProjectID == "" {
		level.Warn(logger).Log("msg", "gcloudtrace.project is empty. Google Cloud Tracer. Tracing will be disabled")
		t := &noop.Tracer{}
		return t, t
	}

	tracer, closer, err := newGCloudTracer(ctx, logger, *f.gcloudTraceProjectID, *f.sampleFactor, serviceName)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to init Google Cloud Tracer. Tracing will be disabled", "err", err)
		t := &noop.Tracer{}
		return t, t
	}

	level.Info(logger).Log("msg", "initiated Google Cloud Tracer. Tracing will be enabled", "err", err)
	return tracer, closer
}

func (f *Factory) RegisterKingpinFlags(app *kingpin.Application) {
	f.gcloudTraceProjectID = app.Flag("gcloudtrace.project", "GCP project to send Google Cloud Trace tracings to. If empty, tracing will be disabled.").Default("").String()
	f.sampleFactor = app.Flag("gcloudtrace.sample-factor", "How often we send traces (1/<sample-factor>). If 0 no trace will be sent periodically, unless forced by baggage item. See `pkg/tracing/tracing.go` for details.").Default("1").Uint64()
}
