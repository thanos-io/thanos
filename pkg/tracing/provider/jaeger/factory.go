package jaeger

import (
	"context"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-lib/metrics/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
	jaeger_log "github.com/uber/jaeger-client-go/log"
)

type Factory struct {
	serviceName *string
}

func NewFactory() *Factory {
	return &Factory{}
}

func (f *Factory) Create(ctx context.Context, logger log.Logger) (opentracing.Tracer, func() error) {
	cfg, err := config.FromEnv()
	cfg.Sampler.Type = "const"
	cfg.Sampler.Param = 1
	cfg.Reporter.LogSpans = true
	if *f.serviceName != "" {
		cfg.ServiceName = *f.serviceName
	}

	jLogger := jaeger_log.StdLogger
	jMetricsFactory := prometheus.New()
	tracer, closer, err := cfg.NewTracer(
		config.Metrics(jMetricsFactory),
		config.Logger(jLogger),
	)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to init Jaeger Tracer. Tracing will be disabled", "err", err)
		return &opentracing.NoopTracer{}, func() error { return nil }
	}
	return tracer, closer.Close
}

func (f *Factory) RegisterKingpinFlags(app *kingpin.Application) {
	f.serviceName = app.Flag("jaeger.service-name", "Jaeger service_name. If empty, tracing will be disabled.").Default("").String()
}