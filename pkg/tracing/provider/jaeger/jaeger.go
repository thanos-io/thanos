package jaeger

import (
	"context"
	"github.com/go-kit/kit/log/level"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-lib/metrics/prometheus"
)

func NewTracer(ctx context.Context, logger log.Logger, conf []byte) (opentracing.Tracer, io.Closer, error) {
	var (
		cfg *config.Configuration
		err error
	)
	if conf != nil {
		level.Info(logger).Log("msg", "loading Jaeger tracing configuration from YAML")
		cfg, err = FromYaml(conf)
	} else {
		level.Info(logger).Log("msg", "loading Jaeger tracing configuration from ENV")
		cfg, err = config.FromEnv()
	}
	if err != nil {
		return nil, nil, err
	}

	cfg.Headers = &jaeger.HeadersConfig{
		JaegerDebugHeader: tracing.ForceTracingBaggageKey,
	}
	cfg.Headers.ApplyDefaults()

	return cfg.NewTracer(
		config.Metrics(prometheus.New()),
		config.Logger(&jaegerLogger{
			logger: logger,
		}),
	)
}
