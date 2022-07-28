// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package jaeger

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/thanos-io/thanos/pkg/tracing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-client-go/zipkin"
	jaeger_prometheus "github.com/uber/jaeger-lib/metrics/prometheus"
)

// Tracer extends opentracing.Tracer.
type Tracer struct {
	opentracing.Tracer
}

// GetTraceIDFromSpanContext return TraceID from span.Context.
func (t *Tracer) GetTraceIDFromSpanContext(ctx opentracing.SpanContext) (string, bool) {
	if c, ok := ctx.(jaeger.SpanContext); ok {
		return fmt.Sprintf("%016x", c.TraceID().Low), true
	}
	return "", false
}

// NewTracer create tracer from YAML.
func NewTracer(ctx context.Context, logger log.Logger, metrics *prometheus.Registry, conf []byte) (opentracing.Tracer, io.Closer, error) {
	var (
		cfg          *config.Configuration
		yamlCfg      *Config
		err          error
		jaegerTracer opentracing.Tracer
		closer       io.Closer
		jaegerOpts   []config.Option
	)
	if conf != nil {
		level.Info(logger).Log("msg", "loading Jaeger tracing configuration from YAML")
		cfg, yamlCfg, err = ParseConfigFromYaml(conf)
	} else {
		level.Info(logger).Log("msg", "loading Jaeger tracing configuration from ENV")
		cfg, err = config.FromEnv()
	}
	if err != nil {
		return nil, nil, err
	}

	cfg.Headers = &jaeger.HeadersConfig{
		JaegerDebugHeader: strings.ToLower(tracing.ForceTracingBaggageKey),
	}
	cfg.Headers.ApplyDefaults()

	jaegerOpts = []config.Option{
		config.Metrics(jaeger_prometheus.New(jaeger_prometheus.WithRegisterer(metrics))),
		config.Logger(&jaegerLogger{
			logger: logger,
		}),
	}

	if yamlCfg != nil && yamlCfg.UseB3Headers {
		zipkinPropagator := zipkin.NewZipkinB3HTTPHeaderPropagator()

		jaegerOpts = append(
			jaegerOpts,
			config.Injector(opentracing.HTTPHeaders, zipkinPropagator),
			config.Extractor(opentracing.HTTPHeaders, zipkinPropagator),
		)
	}

	jaegerTracer, closer, err = cfg.NewTracer(jaegerOpts...)
	t := &Tracer{
		jaegerTracer,
	}
	return t, closer, err
}
