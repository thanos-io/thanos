// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package otlp

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	_ "google.golang.org/grpc/encoding/gzip"
	"gopkg.in/yaml.v2"
)

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

	tp := newTraceProvider(ctx, exporter, logger)

	return tp, nil
}

func newTraceProvider(ctx context.Context, exporter *otlptrace.Exporter, logger log.Logger) *tracesdk.TracerProvider {
	resource, err := resource.New(ctx)
	if err != nil {
		level.Warn(logger).Log("msg", "jaeger: detecting resources for tracing provider failed", "err", err)
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exporter),
		tracesdk.WithResource(resource),
	)
	return tp
}
