// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package otlp

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"gopkg.in/yaml.v2"
)

type Config struct {
	ReconnectionPeriod time.Duration `yaml:"reconnection_period"`
	Compression        string        `yaml:"compression"`
	Insecure           bool          `yaml:"insecure"`
	Endpoint           string        `yaml:"endpoint"`
	URLPath            string        `yaml:"url_path"`
	Timeout            time.Duration `yaml:"timeout"`
}

// NewOTELTracer returns an OTLP exporter based tracer.
func NewTracerProvider(ctx context.Context, logger log.Logger, conf []byte) (*tracesdk.TracerProvider, error) {
	config := Config{}
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, err
	}

	options := traceOptions(config)

	client := otlptracehttp.NewClient(options...)
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		level.Error(logger).Log("err with new client", err.Error())
		return nil, err
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exporter),
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
		)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}

func traceOptions(config Config) []otlptracehttp.Option {
	var options []otlptracehttp.Option
	if config.Endpoint != "" {
		options = append(options, otlptracehttp.WithEndpoint(config.Endpoint))
	}

	if config.Insecure {
		options = append(options, otlptracehttp.WithInsecure())
	}

	if config.URLPath != "" {
		options = append(options, otlptracehttp.WithURLPath(config.URLPath))
	}

	if config.Compression != "" {
		if config.Compression == "GzipCompression" {
			// Todo: how to access otlpconfig.Compression here?
			// Specifying 1 is just a workaround.
			options = append(options, otlptracehttp.WithCompression(1))
		}
	}

	if config.Timeout != 0 {
		options = append(options, otlptracehttp.WithTimeout(config.Timeout))
	}

	return options
}
