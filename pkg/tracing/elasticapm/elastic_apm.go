// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package elasticapm

import (
	"io"

	"github.com/opentracing/opentracing-go"
	"go.elastic.co/apm"
	"go.elastic.co/apm/module/apmot"
	"gopkg.in/yaml.v2"
)

func init() {
	apm.DefaultTracer.Close()
}

type Config struct {
	ServiceName        string  `yaml:"service_name"`
	ServiceVersion     string  `yaml:"service_version"`
	ServiceEnvironment string  `yaml:"service_environment"`
	SampleRate         float64 `yaml:"sample_rate"`
}

func NewTracer(conf []byte) (opentracing.Tracer, io.Closer, error) {
	config := Config{}
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, nil, err
	}
	tracer, err := apm.NewTracerOptions(apm.TracerOptions{
		ServiceName:        config.ServiceName,
		ServiceVersion:     config.ServiceVersion,
		ServiceEnvironment: config.ServiceEnvironment,
	})
	if err != nil {
		return nil, nil, err
	}
	tracer.SetSampler(apm.NewRatioSampler(config.SampleRate))
	return apmot.New(apmot.WithTracer(tracer)), tracerCloser{tracer}, nil
}

type tracerCloser struct {
	tracer *apm.Tracer
}

func (t tracerCloser) Close() error {
	if t.tracer != nil {
		t.tracer.Close()
	}
	return nil
}
