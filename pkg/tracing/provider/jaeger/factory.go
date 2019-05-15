package jaeger

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-lib/metrics/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

// Factory implements tracing.Factory for Jaeger Tracer.
type Factory struct {
}


// NewFactory creates a new Factory
func NewFactory() *Factory {
	return &Factory{}
}

// Create implements tracing.Factory
func (f *Factory) Create(ctx context.Context, logger log.Logger, serviceName string) (opentracing.Tracer, io.Closer, error) {
	cfg, err := config.FromEnv()
	if err != nil {
		return nil, nil, err
	}
	cfg.Headers = &jaeger.HeadersConfig{
		JaegerDebugHeader: tracing.ForceTracingBaggageKey,
	}
	cfg.Headers.ApplyDefaults()
	if serviceName != "" {
		cfg.ServiceName = serviceName
	}

	jLogger := &jaegerLogger{
		logger: logger,
	}
	jMetricsFactory := prometheus.New()
	tracer, closer, err := cfg.NewTracer(
		config.Metrics(jMetricsFactory),
		config.Logger(jLogger),
	)
	if err != nil {
		return nil, nil, err
	}
	return tracer, closer, nil
}

// RegisterKingpinFlags implements tracing.Factory
func (f *Factory) RegisterKingpinFlags(app *kingpin.Application) {
}
