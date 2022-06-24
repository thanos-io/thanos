// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package jaeger

import (
	"context"
	"fmt"
	"math"
	"strconv"

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
	var queueSize tracesdk.BatchSpanProcessorOption
	var processor tracesdk.SpanProcessor
	if config.ReporterMaxQueueSize != 0 {
		queueSize = tracesdk.WithMaxQueueSize(config.ReporterMaxQueueSize)
		processor = tracesdk.NewBatchSpanProcessor(exporter, queueSize)
	} else {
		processor = tracesdk.NewBatchSpanProcessor(exporter)
	}
	tp := newTraceProvider(ctx, logger, processor, samplingFraction, tags, config.ServiceName)

	return tp, nil
}

// getSamplingFraction returns the sampling fraction based on the sampler type.
// Ref: https://www.jaegertracing.io/docs/1.35/sampling/#client-sampling-configuration
func getSamplingFraction(samplerType string, samplingFactor float64) float64 {
	if samplerType == "const" {
		if samplingFactor > 1 {
			return 1.0
		} else if samplingFactor < 0 {
			return 0.0
		}
		return math.Round(samplingFactor) // Returns either 0 or 1 for values [0,1].
	} else if samplerType == "probabilistic" {
		return samplingFactor
	} else if samplerType == "ratelimiting" {
		return math.Round(samplingFactor) // Needs to be an integer.
	}
	return samplingFactor
}

// getAttributesFromTags returns tags as OTel attributes.
func getAttributesFromTags(config Config) []attribute.KeyValue {
	return parseTags(config.Tags)
}

// getCollectorEndpoints returns Jaeger options populated with collector related options.
func getCollectorEndpoints(config Config) []otel_jaeger.CollectorEndpointOption {
	var collectorOptions []otel_jaeger.CollectorEndpointOption
	if config.User != "" {
		collectorOptions = append(collectorOptions, otel_jaeger.WithUsername(config.User))
	}
	if config.Password != "" {
		collectorOptions = append(collectorOptions, otel_jaeger.WithPassword(config.Password))
	}
	collectorOptions = append(collectorOptions, otel_jaeger.WithEndpoint(config.Endpoint))

	return collectorOptions
}

// getAgentEndpointOptions returns Jaeger options populated with agent related options.
func getAgentEndpointOptions(config Config) []otel_jaeger.AgentEndpointOption {
	var jaegerAgentEndpointOptions []otel_jaeger.AgentEndpointOption
	jaegerAgentEndpointOptions = append(jaegerAgentEndpointOptions, otel_jaeger.WithAgentHost(config.AgentHost))
	jaegerAgentEndpointOptions = append(jaegerAgentEndpointOptions, otel_jaeger.WithAgentPort(strconv.Itoa(config.AgentPort)))

	return jaegerAgentEndpointOptions
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
