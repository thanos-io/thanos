// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package stackdriver

import (
	"context"
	"fmt"
	"io"
	"os"

	trace "cloud.google.com/go/trace/apiv1"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/googleapis/gax-go"
	gcloudtracer "github.com/lovoo/gcloud-opentracing"
	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/version"
	pb "google.golang.org/genproto/googleapis/devtools/cloudtrace/v1"

	"github.com/thanos-io/thanos/pkg/tracing"
)

type tracer struct {
	serviceName string
	wrapped     opentracing.Tracer
}

// GetTraceIDFromSpanContext return TraceID from span.Context.
func (t *tracer) GetTraceIDFromSpanContext(ctx opentracing.SpanContext) (string, bool) {
	if c, ok := ctx.(basictracer.SpanContext); ok {
		// "%016x%016x" - ugly hack for gcloud find traces by ID https://console.cloud.google.com/traces/traces?project=<project_id>&tid=<62119f61b7c2663962119f61b7c26639>.
		return fmt.Sprintf("%016x%016x", c.TraceID, c.TraceID), true
	}
	return "", false
}

func (t *tracer) StartSpan(operationName string, opts ...opentracing.StartSpanOption) opentracing.Span {
	span := t.wrapped.StartSpan(operationName, opts...)

	if t.serviceName != "" {
		span.SetTag("service_name", t.serviceName)
	}

	// Set common tags.
	if hostname := os.Getenv("HOSTNAME"); hostname != "" {
		span.SetTag("hostname", hostname)
	}

	span.SetTag("binary_revision", version.Revision)
	if len(os.Args) > 1 {
		span.SetTag("binary_cmd", os.Args[1])
	}

	return span
}

func (t *tracer) Extract(format, carrier interface{}) (opentracing.SpanContext, error) {
	return t.wrapped.Extract(format, carrier)
}

func (t *tracer) Inject(sm opentracing.SpanContext, format, carrier interface{}) error {
	return t.wrapped.Inject(sm, format, carrier)
}

type forceRecorder struct {
	wrapped basictracer.SpanRecorder
}

// RecordSpan invokes wrapper SpanRecorder only if Sampled field is true or ForceTracingBaggageKey item is set in span's context.
// NOTE(bplotka): Currently only HTTP supports ForceTracingBaggageKey injection on ForceTracingBaggageKey header existence.
func (r *forceRecorder) RecordSpan(sp basictracer.RawSpan) {
	if force := sp.Context.Baggage[tracing.ForceTracingBaggageKey]; force != "" {
		sp.Context.Sampled = true
	}

	// All recorder implementation should support handling sp.Context.Sampled.
	r.wrapped.RecordSpan(sp)
}

type gcloudRecorderLogger struct {
	logger log.Logger
}

func (l *gcloudRecorderLogger) Infof(format string, args ...interface{}) {
	level.Info(l.logger).Log("msg", fmt.Sprintf(format, args...))
}

func (l *gcloudRecorderLogger) Errorf(format string, args ...interface{}) {
	level.Error(l.logger).Log("msg", fmt.Sprintf(format, args...))
}

// TODO(bwplotka): gcloudtracer is archived. Find replacement. For now wrap traceClient for compatibility.
type compTraceWrapper struct {
	cl *trace.Client
}

func (w *compTraceWrapper) PatchTraces(ctx context.Context, r *pb.PatchTracesRequest, _ ...gax.CallOption) error {
	// Opts are never used in `gcloudtracer.NewRecorder`.
	return w.cl.PatchTraces(ctx, r)
}

func (w *compTraceWrapper) Close() error {
	return w.cl.Close()
}

func newGCloudTracer(ctx context.Context, logger log.Logger, gcloudTraceProjectID string, sampleFactor uint64, serviceName string) (opentracing.Tracer, io.Closer, error) {
	traceClient, err := trace.NewClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	// TODO(bwplotka): gcloudtracer is archived. Find replacement. For now wrap traceClient for compatibility.
	r, err := gcloudtracer.NewRecorder(
		ctx,
		gcloudTraceProjectID,
		&compTraceWrapper{cl: traceClient},
		gcloudtracer.WithLogger(&gcloudRecorderLogger{logger: logger}))
	if err != nil {
		return nil, nil, err
	}

	shouldSample := func(traceID uint64) bool {
		// Set the sampling rate.
		return traceID%sampleFactor == 0
	}
	if sampleFactor < 1 {
		level.Debug(logger).Log("msg", "Tracing is enabled, but sampling is 0 which means only spans with 'force tracing' baggage will enable tracing.")
		shouldSample = func(_ uint64) bool {
			return false
		}
	}
	return &tracer{
		serviceName: serviceName,
		wrapped: basictracer.NewWithOptions(basictracer.Options{
			ShouldSample:   shouldSample,
			Recorder:       &forceRecorder{wrapped: r},
			MaxLogsPerSpan: 100,
		}),
	}, r, nil
}
