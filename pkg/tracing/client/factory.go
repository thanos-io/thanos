// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package client

import (
	"context"
	"io"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/tracing/elasticapm"
	"github.com/thanos-io/thanos/pkg/tracing/jaeger"
	"github.com/thanos-io/thanos/pkg/tracing/lightstep"
	"github.com/thanos-io/thanos/pkg/tracing/stackdriver"
	"gopkg.in/yaml.v2"
)

type TracingProvider string

const (
	STACKDRIVER TracingProvider = "STACKDRIVER"
	JAEGER      TracingProvider = "JAEGER"
	ELASTIC_APM TracingProvider = "ELASTIC_APM"
	LIGHTSTEP   TracingProvider = "LIGHTSTEP"
)

type TracingConfig struct {
	Type   TracingProvider `yaml:"type"`
	Config interface{}     `yaml:"config"`
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
	case string(STACKDRIVER):
		return stackdriver.NewTracer(ctx, logger, config)
	case string(JAEGER):
		return jaeger.NewTracer(ctx, logger, metrics, config)
	case string(ELASTIC_APM):
		return elasticapm.NewTracer(config)
	case string(LIGHTSTEP):
		return lightstep.NewTracer(ctx, config)
	default:
		return nil, nil, errors.Errorf("tracing with type %s is not supported", tracingConf.Type)
	}
}

func NoopTracer() opentracing.Tracer {
	return &opentracing.NoopTracer{}
}
