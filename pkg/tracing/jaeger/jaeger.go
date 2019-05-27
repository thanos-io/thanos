package jaeger

import (
	"context"
	"fmt"
	"github.com/go-kit/kit/log/level"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-lib/metrics/prometheus"
)

type Tracer struct {
	opentracing.Tracer
}

func (t *Tracer) GetTraceIdFromSpanContext(ctx opentracing.SpanContext) (string, bool) {
	if c, ok := ctx.(jaeger.SpanContext); ok {
		return fmt.Sprintf("%016x", c.TraceID().Low), true
	}
	return "", false
}

func NewTracer(ctx context.Context, logger log.Logger, conf []byte) (opentracing.Tracer, io.Closer, error) {
	var (
		cfg *config.Configuration
		err error
		jaegerTracer opentracing.Tracer
		closer io.Closer
	)
	if conf != nil {
		level.Info(logger).Log("msg", "loading Jaeger tracing configuration from YAML")
		cfg, err = FromYaml(conf)
	} else {
		level.Info(logger).Log("msg", "loading Jaeger tracing configuration from ENV")
		cfg, err = config.FromEnv()
	}
	if err != nil {
		return nil, nil, err
	}

	cfg.Headers = &jaeger.HeadersConfig{
		JaegerDebugHeader: tracing.ForceTracingBaggageKey,
	}
	cfg.Headers.ApplyDefaults()
	jaegerTracer, closer, err = cfg.NewTracer(
		config.Metrics(prometheus.New()),
		config.Logger(&jaegerLogger{
			logger: logger,
		}),
	)
	t := &Tracer{
		jaegerTracer,
	}
	return t, closer, err
}
