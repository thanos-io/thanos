// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package lightstep

import (
	"context"
	"io"

	"github.com/lightstep/lightstep-tracer-go"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/yaml.v2"
)

// Config - YAML configuration.
type Config struct {
	// AccessToken is the unique API key for your LightStep project. It is
	// available on your account page at https://app.lightstep.com/account.
	AccessToken string `yaml:"access_token"`

	// Collector is the host, port, and plaintext option to use
	// for the collector.
	Collector lightstep.Endpoint `yaml:"collector"`
}

// Tracer wraps the Lightstep tracer and the context.
type Tracer struct {
	lightstep.Tracer
	ctx context.Context
}

// Close synchronously flushes the Lightstep tracer, then terminates it.
func (t *Tracer) Close() error {
	t.Tracer.Close(t.ctx)

	return nil
}

// NewTracer creates a Tracer with the options present in the YAML config.
func NewTracer(ctx context.Context, yamlConfig []byte) (opentracing.Tracer, io.Closer, error) {
	config := Config{}
	if err := yaml.Unmarshal(yamlConfig, &config); err != nil {
		return nil, nil, err
	}

	options := lightstep.Options{
		AccessToken: config.AccessToken,
		Collector:   config.Collector,
	}
	lighstepTracer, err := lightstep.CreateTracer(options)
	if err != nil {
		return nil, nil, err
	}

	t := &Tracer{
		lighstepTracer,
		ctx,
	}
	return t, t, nil
}
