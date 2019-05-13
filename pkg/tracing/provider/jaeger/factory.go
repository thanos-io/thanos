package jaeger

import (
	"context"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-lib/metrics/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

type Factory struct {
}

func NewFactory() *Factory {
	return &Factory{}
}

func (f *Factory) Create(ctx context.Context, logger log.Logger, debugName string) (opentracing.Tracer, func() error) {
	cfg, err := config.FromEnv()
	cfg.Headers = &jaeger.HeadersConfig{
		JaegerDebugHeader: tracing.ForceTracingBaggageKey,
	}
	cfg.Headers.ApplyDefaults()
	if debugName != "" {
		cfg.ServiceName = debugName
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
		level.Warn(logger).Log("msg", "failed to init Jaeger Tracer. Tracing will be disabled", "err", err)
		return &opentracing.NoopTracer{}, func() error { return nil }
	}
	level.Info(logger).Log("msg", "initiated Jaeger Tracer. Tracing will be enabled", "err", err)
	return tracer, closer.Close
}

func (f *Factory) RegisterKingpinFlags(app *kingpin.Application) {
}