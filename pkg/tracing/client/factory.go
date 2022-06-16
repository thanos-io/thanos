// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package client

import (
	"context"
	"io"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/tracing/elasticapm"
	"github.com/thanos-io/thanos/pkg/tracing/google_cloud"
	"github.com/thanos-io/thanos/pkg/tracing/jaeger"
	"github.com/thanos-io/thanos/pkg/tracing/lightstep"
	"github.com/thanos-io/thanos/pkg/tracing/migration"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

type TracingProvider string

const (
	Stackdriver           TracingProvider = "STACKDRIVER"
	GoogleCloud           TracingProvider = "GOOGLE_CLOUD"
	Jaeger                TracingProvider = "JAEGER"
	ElasticAPM            TracingProvider = "ELASTIC_APM"
	Lightstep             TracingProvider = "LIGHTSTEP"
	OpenTelemetryProtocol TracingProvider = "OTLP"
)

type TracingConfig struct {
	Type   TracingProvider `yaml:"type"`
	Config interface{}     `yaml:"config"`
}

// NewOTELTracer returns an OTLP exporter based tracer.
func NewOTELTracer(ctx context.Context, logger log.Logger) trace.Tracer {
	client := otlptracehttp.NewClient()
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		level.Error(logger).Log("err with new client", err.Error())
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exporter),
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
		)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tracer := otel.GetTracerProvider().Tracer(
		"thanos-tracer",
		trace.WithSchemaURL(semconv.SchemaURL),
	)
	return tracer
}

func NewTracer(ctx context.Context, logger log.Logger, metrics *prometheus.Registry, confContentYaml []byte) (opentracing.Tracer, io.Closer, error) {
	level.Info(logger).Log("msg", "loading tracing configuration")
	tracingConf := &TracingConfig{}

	if err := yaml.UnmarshalStrict(confContentYaml, tracingConf); err != nil {
		return nil, nil, errors.Wrap(err, "parsing config tracing YAML")
	}

	var config []byte
	var err error
	if tracingConf.Config != nil {
		config, err = yaml.Marshal(tracingConf.Config)
		if err != nil {
			return nil, nil, errors.Wrap(err, "marshal content of tracing configuration")
		}
	}

	switch strings.ToUpper(string(tracingConf.Type)) {
	case string(Stackdriver), string(GoogleCloud):
		tracerProvider, err := google_cloud.NewTracerProvider(ctx, logger, config)
		if err != nil {
			return nil, nil, err
		}
		tracer, closerFunc := migration.Bridge(tracerProvider, logger)
		return tracer, closerFunc, nil
	case string(Jaeger):
		tracerProvider, err := jaeger.NewTracerProvider(ctx, logger, config)
		if err != nil {
			return nil, nil, errors.Wrap(err, "new tracer provider err")
		}
		tracer, closerFunc := migration.Bridge(tracerProvider, logger)
		return tracer, closerFunc, nil
	case string(ElasticAPM):
		return elasticapm.NewTracer(config)
	case string(Lightstep):
		return lightstep.NewTracer(ctx, config)
	default:
		return nil, nil, errors.Errorf("tracing with type %s is not supported", tracingConf.Type)
	}
}

func NoopTracer() opentracing.Tracer {
	return &opentracing.NoopTracer{}
}
