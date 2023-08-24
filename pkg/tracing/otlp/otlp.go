// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package otlp

import (
	"context"
	"strconv"
	"strings"

	"github.com/thanos-io/thanos/pkg/tracing/migration"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	_ "google.golang.org/grpc/encoding/gzip"
	"gopkg.in/yaml.v2"
)

const (
	TracingClientGRPC string = "grpc"
	TracingClientHTTP string = "http"
	AlwaysSample      string = "alwayssample"
	NeverSample       string = "neversample"
	RatioBasedSample  string = "traceidratiobased"
)

// NewOTELTracer returns an OTLP exporter based tracer.
func NewTracerProvider(ctx context.Context, logger log.Logger, conf []byte) (*tracesdk.TracerProvider, error) {
	config := Config{}
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, err
	}

	var exporter *otlptrace.Exporter
	var err error
	switch strings.ToLower(config.ClientType) {
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

	processor := tracesdk.NewBatchSpanProcessor(exporter)
	sampler, err := getSampler(config)
	if err != nil {
		logger.Log(err)
	}
	tp := newTraceProvider(ctx, processor, logger, config.ServiceName, sampler)

	return tp, nil
}

func newTraceProvider(ctx context.Context, processor tracesdk.SpanProcessor, logger log.Logger, serviceName string, sampler tracesdk.Sampler) *tracesdk.TracerProvider {
	var (
		r   *resource.Resource
		err error
	)
	if serviceName != "" {
		r, err = resource.New(
			ctx,
			resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)),
		)
	} else {
		r, err = resource.New(
			ctx,
		)
	}
	if err != nil {
		level.Warn(logger).Log("msg", "jaeger: detecting resources for tracing provider failed", "err", err)
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithSpanProcessor(processor),
		tracesdk.WithResource(r),
		tracesdk.WithSampler(
			migration.SamplerWithOverride(
				sampler, migration.ForceTracingAttributeKey,
			),
		),
	)
	return tp
}

func getSampler(config Config) (tracesdk.Sampler, error) {
	switch strings.ToLower(config.SamplerType) {
	case AlwaysSample:
		return tracesdk.ParentBased(tracesdk.AlwaysSample()), nil
	case NeverSample:
		return tracesdk.ParentBased(tracesdk.NeverSample()), nil
	case RatioBasedSample:
		arg, err := strconv.ParseFloat(config.SamplerParam, 64)
		if err != nil {
			return tracesdk.ParentBased(tracesdk.TraceIDRatioBased(1.0)), err
		}
		return tracesdk.ParentBased(tracesdk.TraceIDRatioBased(arg)), nil
	}

	return tracesdk.ParentBased(tracesdk.TraceIDRatioBased(1.0)), nil
}
