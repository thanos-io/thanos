// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package otlp

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	_ "google.golang.org/grpc/encoding/gzip"
	"gopkg.in/yaml.v2"
)

type Config struct {
	ClientType         string        `yaml:"client)type"`
	ReconnectionPeriod time.Duration `yaml:"reconnection_period"`
	Compression        string        `yaml:"compression"`
	Insecure           bool          `yaml:"insecure"`
	Endpoint           string        `yaml:"endpoint"`
	URLPath            string        `yaml:"url_path"`
	Timeout            time.Duration `yaml:"timeout"`
}

// add TLS config and HTTP headers

var (
	TracingClientGRPC string = "grpc"
	TracingClientHTTP string = "http"
)

// NewOTELTracer returns an OTLP exporter based tracer.
func NewTracerProvider(ctx context.Context, logger log.Logger, conf []byte) (*tracesdk.TracerProvider, error) {
	config := Config{}
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, err
	}

	var exporter *otlptrace.Exporter
	var err error
	switch config.ClientType {
	case TracingClientHTTP:
		options := traceHTTPOptions(config)

		client := otlptracehttp.NewClient(options...)
		exporter, err = otlptrace.New(ctx, client)
		if err != nil {
			return nil, err
		}

	case TracingClientGRPC:
		options := traceGRPCOptions(config)
		client := otlptracegrpc.NewClient(options...)
		exporter, err = otlptrace.New(ctx, client)
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.New("otlp: invalid client type. Only 'http' and 'grpc' are accepted. ")
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

func traceGRPCOptions(config Config) []otlptracegrpc.Option {
	var options []otlptracegrpc.Option
	if config.Endpoint != "" {
		options = append(options, otlptracegrpc.WithEndpoint(config.Endpoint))
	}

	if config.Insecure {
		options = append(options, otlptracegrpc.WithInsecure())
	}

	if config.ReconnectionPeriod != 0 {
		options = append(options, otlptracegrpc.WithReconnectionPeriod(config.ReconnectionPeriod))
	}

	if config.Timeout != 0 {
		options = append(options, otlptracegrpc.WithTimeout(config.Timeout))
	}

	if config.Compression != "" {
		if config.Compression == "gzip" {
			options = append(options, otlptracegrpc.WithCompressor(config.Compression))
		}
	}

	return options
}

func traceHTTPOptions(config Config) []otlptracehttp.Option {
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
		if config.Compression == "gzip" {
			options = append(options, otlptracehttp.WithCompression(otlptracehttp.GzipCompression))
		}
	}

	if config.Timeout != 0 {
		options = append(options, otlptracehttp.WithTimeout(config.Timeout))
	}

	return options
}
