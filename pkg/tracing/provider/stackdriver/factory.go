package stackdriver

import (
	"context"
	"errors"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/alecthomas/kingpin.v2"
)

// Factory implements tracing.Factory for Stackdriver Tracer.
type Factory struct {
	gcloudTraceProjectID *string
	sampleFactor         *uint64
}

// NewFactory creates a new Factory
func NewFactory() *Factory {
	return &Factory{}
}

// Create implements tracing.Factory
func (f *Factory) Create(ctx context.Context, logger log.Logger, serviceName string) (opentracing.Tracer, io.Closer, error) {
	if *f.gcloudTraceProjectID == "" {
		return nil, nil, errors.New("stackdriver.project is empty.")
	}
	tracer, closer, err := newGCloudTracer(ctx, logger, *f.gcloudTraceProjectID, *f.sampleFactor, serviceName)
	if err != nil {
		return nil, nil, err
	}
	return tracer, closer, nil
}

// RegisterKingpinFlags implements tracing.Factory
func (f *Factory) RegisterKingpinFlags(app *kingpin.Application) {
	f.gcloudTraceProjectID = app.Flag("stackdriver.project", "GCP project to send Google Cloud Trace tracings to. If empty, tracing will be disabled.").Default("").String()
	f.sampleFactor = app.Flag("stackdriver.sample-factor", "How often we send traces (1/<sample-factor>). If 0 no trace will be sent periodically, unless forced by baggage item. See `pkg/tracing/tracing.go` for details.").Default("1").Uint64()
}
