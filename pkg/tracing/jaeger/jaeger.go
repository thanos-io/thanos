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
	jaeger_prometheus "github.com/uber/jaeger-lib/metrics/prometheus"
	otel_jaeger "go.opentelemetry.io/otel/exporters/jaeger"
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
		err          error
		jaegerTracer opentracing.Tracer
		closer       io.Closer
	)
	if conf != nil {
		level.Info(logger).Log("msg", "loading Jaeger tracing configuration from YAML")
		cfg, err = ParseConfigFromYaml(conf)
	} else {
		level.Info(logger).Log("msg", "loading Jaeger tracing configuration from ENV")
		cfg, err = config.FromEnv()
	}
	if err != nil {
		return nil, nil, err
	}

	level.Info(logger).Log("msg", "getting tracing config", cfg)
	cfg.Headers = &jaeger.HeadersConfig{
		JaegerDebugHeader: strings.ToLower(tracing.ForceTracingBaggageKey),
	}
	cfg.Headers.ApplyDefaults()
	jaegerTracer, closer, err = cfg.NewTracer(
		config.Metrics(jaeger_prometheus.New(jaeger_prometheus.WithRegisterer(metrics))),
		config.Logger(&jaegerLogger{
			logger: logger,
		}),
	)
	// return OTEL Jaeger exporter here instead
	// exp, err := otel_jaeger.New(otel_jaeger.WithCollectorEndpoint(otel_jaeger.WithEndpoint(url)))
	exp, err := otel_jaeger.New(otel_jaeger.WithCollectorEndpoint())
	if err != nil {
		return nil, nil, err
	}
	_ = exp

	t := &Tracer{
		jaegerTracer,
	}
	return t, closer, err
}
