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
	"gopkg.in/yaml.v2"
)

type Config struct {
	ServiceName string `yaml:"service_name"`
}

func parseConfig(conf []byte) (Config, error) {
	config := Config{}
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return Config{}, err
	}
	return config, nil
}

func NewTracer(ctx context.Context, logger log.Logger, conf []byte) (opentracing.Tracer, io.Closer, error) {
	config, err := parseConfig(conf)
	if err != nil {
		return nil, nil, err
	}
	return NewTracerWithConfig(ctx, logger, config)
}

func NewTracerWithConfig(ctx context.Context, logger log.Logger, conf Config) (opentracing.Tracer, io.Closer, error) {
	cfg, err := config.FromEnv()
	if err != nil {
		return nil, nil, err
	}
	cfg.Headers = &jaeger.HeadersConfig{
		JaegerDebugHeader: tracing.ForceTracingBaggageKey,
	}
	cfg.Headers.ApplyDefaults()
	cfg.ServiceName = conf.ServiceName

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
