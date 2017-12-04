// Package gct contains initialization for Google Cloud Trace opentracing.Tracer.

package tracing

import (
	"context"

	"fmt"

	"cloud.google.com/go/trace/apiv1"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/lovoo/gcloud-opentracing"
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
func NewOptionalGCloudTracer(ctx context.Context, logger log.Logger, gcloudTraceProjectID string, sampleFactor uint64) (opentracing.Tracer, func() error) {
	if gcloudTraceProjectID == "" {
		return &opentracing.NoopTracer{}, nil
	}

	tracer, closeFn, err := newGCloudTracer(ctx, logger, gcloudTraceProjectID, sampleFactor)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to init Google Cloud Tracer. Tracing will be disabled", "err", err)
		return &opentracing.NoopTracer{}, nil
	}

	return tracer, closeFn
}

func newGCloudTracer(ctx context.Context, logger log.Logger, gcloudTraceProjectID string, sampleFactor uint64) (opentracing.Tracer, func() error, error) {
	if sampleFactor < 1 {
		return nil, nil, errors.Errorf("invalid opentracing sample factor: %v, should be > 0", sampleFactor)
	}

	traceClient, err := trace.NewClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	recorder, err := gcloudtracer.NewRecorder(
		ctx,
		gcloudTraceProjectID,
		traceClient,
		gcloudtracer.WithLogger(&gcloudRecorderLogger{logger: logger}))
	if err != nil {
		return nil, traceClient.Close, err
	}

	// Set the sampling rate.
	opts := basictracer.Options{
		ShouldSample: func(traceID uint64) bool {
			return traceID%sampleFactor == 0
		},
		Recorder:       recorder,
		MaxLogsPerSpan: 100,
	}

	return &tracer{
		wrapped: basictracer.NewWithOptions(opts),
	}, recorder.Close, nil
}
