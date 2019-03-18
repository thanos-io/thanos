// Package gct contains initialization for Google Cloud Trace opentracing.Tracer.
package tracing

import (
	"context"

	"fmt"

	trace "cloud.google.com/go/trace/apiv1"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	gcloudtracer "github.com/lovoo/gcloud-opentracing"
	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
)

type gcloudRecorderLogger struct {
	logger log.Logger
}

func (l *gcloudRecorderLogger) Infof(format string, args ...interface{}) {
	level.Info(l.logger).Log("msg", fmt.Sprintf(format, args...))
}

func (l *gcloudRecorderLogger) Errorf(format string, args ...interface{}) {
	level.Error(l.logger).Log("msg", fmt.Sprintf(format, args...))
}

// NewOptionalGCloudTracer returns GoogleCloudTracer Tracer. In case of error it log warning and returns noop tracer.
func NewOptionalGCloudTracer(ctx context.Context, logger log.Logger, gcloudTraceProjectID string, sampleFactor uint64, debugName string) (opentracing.Tracer, func() error) {
	if gcloudTraceProjectID == "" {
		return &opentracing.NoopTracer{}, func() error { return nil }
	}

	tracer, closeFn, err := newGCloudTracer(ctx, logger, gcloudTraceProjectID, sampleFactor, debugName)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to init Google Cloud Tracer. Tracing will be disabled", "err", err)
		return &opentracing.NoopTracer{}, func() error { return nil }
	}

	level.Info(logger).Log("msg", "initiated Google Cloud Tracer. Tracing will be enabled", "err", err)
	return tracer, closeFn
}

func newGCloudTracer(ctx context.Context, logger log.Logger, gcloudTraceProjectID string, sampleFactor uint64, debugName string) (opentracing.Tracer, func() error, error) {
	traceClient, err := trace.NewClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	r, err := gcloudtracer.NewRecorder(
		ctx,
		gcloudTraceProjectID,
		traceClient,
		gcloudtracer.WithLogger(&gcloudRecorderLogger{logger: logger}))
	if err != nil {
		return nil, traceClient.Close, err
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
		debugName: debugName,
		wrapped: basictracer.NewWithOptions(basictracer.Options{
			ShouldSample:   shouldSample,
			Recorder:       &forceRecorder{wrapped: r},
			MaxLogsPerSpan: 100,
		}),
	}, r.Close, nil
}
