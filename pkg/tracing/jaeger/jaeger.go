// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package jaeger

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/thanos-io/thanos/pkg/tracing"
	"github.com/thanos-io/thanos/pkg/tracing/migration"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/version"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	jaeger_prometheus "github.com/uber/jaeger-lib/metrics/prometheus"
	"go.opentelemetry.io/otel/attribute"
	otel_jaeger "go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"gopkg.in/yaml.v2"
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

// NewTracerProvider returns a new instance of an OpenTelemetry tracer provider.
func NewTracerProvider(ctx context.Context, logger log.Logger, conf []byte) (*tracesdk.TracerProvider, error) {
	config := Config{}
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, err
	}

	var exporter *otel_jaeger.Exporter
	var err error

	if config.Endpoint != "" {
		var jaegerCollectorEndpointOptions []otel_jaeger.CollectorEndpointOption
		if config.User != "" {
			jaegerCollectorEndpointOptions = append(jaegerCollectorEndpointOptions, otel_jaeger.WithUsername(config.User))
		}
		if config.Password != "" {
			jaegerCollectorEndpointOptions = append(jaegerCollectorEndpointOptions, otel_jaeger.WithPassword(config.Password))
		}
		jaegerCollectorEndpointOptions = append(jaegerCollectorEndpointOptions, otel_jaeger.WithEndpoint(config.Endpoint))

		exporter, err = otel_jaeger.New(otel_jaeger.WithCollectorEndpoint(jaegerCollectorEndpointOptions...))
		if err != nil {
			return nil, err
		}
	} else if config.AgentHost != "" && config.AgentPort != 0 {
		var jaegerAgentEndpointOptions []otel_jaeger.AgentEndpointOption
		jaegerAgentEndpointOptions = append(jaegerAgentEndpointOptions, otel_jaeger.WithAgentHost(config.AgentHost))
		jaegerAgentEndpointOptions = append(jaegerAgentEndpointOptions, otel_jaeger.WithAgentPort(strconv.Itoa(config.AgentPort)))

		exporter, err = otel_jaeger.New(otel_jaeger.WithAgentEndpoint(jaegerAgentEndpointOptions...))
		if err != nil {
			return nil, err
		}
	} else {
		exporter, err = otel_jaeger.New(nil)
		if err != nil {
			return nil, err
		}
	}

	processor := tracesdk.NewBatchSpanProcessor(exporter)
	tp := newTraceProvider(ctx, logger, processor, config.SamplerParam, config.ServiceName)

	return tp, nil
}

func newTraceProvider(ctx context.Context, logger log.Logger, processor tracesdk.SpanProcessor,
	samplingFactor float64, serviceName string) *tracesdk.TracerProvider {

	var fraction float64
	if samplingFactor == 0 {
		fraction = 0
	} else {
		fraction = 1 / float64(samplingFactor)
	}

	resource, err := resource.New(ctx, resource.WithAttributes(collectAttributes(serviceName)...))
	if err != nil {
		level.Warn(logger).Log("msg", "jaeger: detecting resources for tracing provider failed", "err", err)
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithSpanProcessor(processor),
		tracesdk.WithSampler(
			migration.SamplerWithOverride(
				tracesdk.ParentBased(tracesdk.TraceIDRatioBased(fraction)),
				migration.ForceTracingAttributeKey,
			),
		),
		tracesdk.WithResource(resource),
	)

	return tp
}

func collectAttributes(serviceName string) []attribute.KeyValue {
	attr := []attribute.KeyValue{
		semconv.ServiceNameKey.String(serviceName),
		attribute.String("binary_revision", version.Revision),
	}

	if len(os.Args) > 1 {
		attr = append(attr, attribute.String("binary_cmd", os.Args[1]))
	}

	return attr
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
	if err != nil {
		return nil, nil, err
	}

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
