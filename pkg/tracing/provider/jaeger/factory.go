package jaeger

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/improbable-eng/thanos/pkg/tracing/provider/noop"
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

func (f *Factory) Create(ctx context.Context, logger log.Logger, serviceName string) (opentracing.Tracer, io.Closer) {
	cfg, err := config.FromEnv()
	if err != nil {
		level.Warn(logger).Log("msg", "failed to init Jaeger Tracer from Environment variables. Tracing will be disabled", "err", err)
		t := &noop.Tracer{}
		return t, t
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
		level.Warn(logger).Log("msg", "failed to init Jaeger Tracer. Tracing will be disabled", "err", err)
		t := &noop.Tracer{}
		return t, t
	}
	level.Info(logger).Log("msg", "initiated Jaeger Tracer. Tracing will be enabled", "err", err)
	return tracer, closer
}

func (f *Factory) RegisterKingpinFlags(app *kingpin.Application) {
}
