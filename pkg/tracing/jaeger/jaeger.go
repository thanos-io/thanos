// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package jaeger

import (
	"context"
	"fmt"

	"github.com/thanos-io/thanos/pkg/tracing"
	"github.com/thanos-io/thanos/pkg/tracing/migration"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"go.opentelemetry.io/otel/attribute"
	otel_jaeger "go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
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
		exporter, err = otel_jaeger.New(nil)
		if err != nil {
			return nil, err
		}
	}

	var tags []attribute.KeyValue
	if config.Tags != "" {
		tags = getAttributesFromTags(config)
	}
	samplingFraction := getSamplingFraction(config.SamplerType, config.SamplerParam)
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

	tp := newTraceProvider(ctx, logger, processor, samplingFraction, tags, config.ServiceName)

	return tp, nil
}

// getAttributesFromTags returns tags as OTel attributes.
func getAttributesFromTags(config Config) []attribute.KeyValue {
	return parseTags(config.Tags)
}

func newTraceProvider(ctx context.Context, logger log.Logger, processor tracesdk.SpanProcessor,
	samplingFraction float64, tags []attribute.KeyValue, serviceName string) *tracesdk.TracerProvider {

	attributes := tracing.CollectAttributes(serviceName)
	attributes = append(attributes, tags...)
	resource, err := resource.New(ctx, resource.WithAttributes(attributes...))
	if err != nil {
		level.Warn(logger).Log("msg", "jaeger: detecting resources for tracing provider failed", "err", err)
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithSpanProcessor(processor),
		tracesdk.WithSampler(
			migration.SamplerWithOverride(
				tracesdk.ParentBased(tracesdk.TraceIDRatioBased(samplingFraction)),
				migration.ForceTracingAttributeKey,
			),
		),
		tracesdk.WithResource(resource),
	)

	return tp
}
