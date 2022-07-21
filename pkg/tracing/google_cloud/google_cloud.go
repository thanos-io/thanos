// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package google_cloud

import (
	"context"
	"os"

	"github.com/prometheus/common/version"
	"github.com/thanos-io/thanos/pkg/tracing/migration"

	cloudtrace "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"gopkg.in/yaml.v2"
)

// Config - YAML configuration.
type Config struct {
	ServiceName  string `yaml:"service_name"`
	ProjectId    string `yaml:"project_id"`
	SampleFactor uint64 `yaml:"sample_factor"`
}

// NewTracerProvider create tracer provider from YAML.
func NewTracerProvider(ctx context.Context, logger log.Logger, conf []byte) (*tracesdk.TracerProvider, error) {
	config := Config{}
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, err
	}

	exporter, err := cloudtrace.New(
		cloudtrace.WithContext(ctx),
		cloudtrace.WithProjectID(config.ProjectId),
	)
	if err != nil {
		return nil, err
	}

	return newTracerProvider(ctx, logger, tracesdk.NewBatchSpanProcessor(exporter),
		config.SampleFactor, config.ServiceName), nil
}

func newTracerProvider(ctx context.Context, logger log.Logger, processor tracesdk.SpanProcessor, sampleFactor uint64, serviceName string) *tracesdk.TracerProvider {
	// Even if resource.New returns error, the resource will be valid - log the error and continue.
	resource, err := resource.New(ctx, resource.WithAttributes(collectAttributes(serviceName)...))
	if err != nil {
		level.Warn(logger).Log("msg", "detecting resources for tracing provider failed", "err", err)
	}

	var fraction float64
	if sampleFactor == 0 {
		fraction = 0
	} else {
		fraction = 1 / float64(sampleFactor)
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
