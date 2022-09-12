// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package jaeger

import (
	"context"

	"github.com/thanos-io/thanos/pkg/tracing/migration"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.opentelemetry.io/otel/attribute"
	otel_jaeger "go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"gopkg.in/yaml.v2"
)

// NewTracerProvider returns a new instance of an OpenTelemetry tracer provider.
func NewTracerProvider(ctx context.Context, logger log.Logger, conf []byte) (*tracesdk.TracerProvider, error) {
	config := Config{}
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, err
	}

	printDeprecationWarnings(config, logger)

	var exporter *otel_jaeger.Exporter
	var err error

	if config.Endpoint != "" {
		collectorOptions := getCollectorEndpoints(config)

		exporter, err = otel_jaeger.New(otel_jaeger.WithCollectorEndpoint(collectorOptions...))
		if err != nil {
			return nil, err
		}
	} else if config.AgentHost != "" && config.AgentPort != 0 {
		jaegerAgentEndpointOptions := getAgentEndpointOptions(config)

		exporter, err = otel_jaeger.New(otel_jaeger.WithAgentEndpoint(jaegerAgentEndpointOptions...))
		if err != nil {
			return nil, err
		}
	} else {
		exporter, err = otel_jaeger.New(otel_jaeger.WithAgentEndpoint())
		if err != nil {
			return nil, err
		}
	}

	var tags []attribute.KeyValue
	if config.Tags != "" {
		tags = getAttributesFromTags(config)
	}

	sampler := getSampler(config)
	var processorOptions []tracesdk.BatchSpanProcessorOption
	var processor tracesdk.SpanProcessor
	if config.ReporterMaxQueueSize != 0 {
		processorOptions = append(processorOptions, tracesdk.WithMaxQueueSize(config.ReporterMaxQueueSize))
	}

	//Ref: https://epsagon.com/observability/opentelemetry-best-practices-overview-part-2-2/ .
	if config.ReporterFlushInterval != 0 {
		processorOptions = append(processorOptions, tracesdk.WithBatchTimeout(config.ReporterFlushInterval))
	}

	processor = tracesdk.NewBatchSpanProcessor(exporter, processorOptions...)

	tp := newTraceProvider(ctx, config.ServiceName, logger, processor, sampler, tags)

	return tp, nil
}

// getAttributesFromTags returns tags as OTel attributes.
func getAttributesFromTags(config Config) []attribute.KeyValue {
	return parseTags(config.Tags)
}

func newTraceProvider(ctx context.Context, serviceName string, logger log.Logger, processor tracesdk.SpanProcessor,
	sampler tracesdk.Sampler, tags []attribute.KeyValue) *tracesdk.TracerProvider {

	resource, err := resource.New(
		ctx,
		resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)),
		resource.WithAttributes(tags...),
	)
	if err != nil {
		level.Warn(logger).Log("msg", "jaeger: detecting resources for tracing provider failed", "err", err)
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithSpanProcessor(processor),
		tracesdk.WithSampler(
			migration.SamplerWithOverride(
				sampler, migration.ForceTracingAttributeKey,
			),
		),
		tracesdk.WithResource(resource),
	)

	return tp
}
