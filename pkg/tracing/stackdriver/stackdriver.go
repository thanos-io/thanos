// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package stackdriver

import (
	"context"
	"io"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/yaml.v2"
)

// Config - YAML configuration.
type Config struct {
	ServiceName  string `yaml:"service_name"`
	ProjectId    string `yaml:"project_id"`
	SampleFactor uint64 `yaml:"sample_factor"`
}

// NewTracer create tracer from YAML.
func NewTracer(ctx context.Context, logger log.Logger, conf []byte) (opentracing.Tracer, io.Closer, error) {
	config := Config{}
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, nil, err
	}
	return newGCloudTracer(ctx, logger, config.ProjectId, config.SampleFactor, config.ServiceName)
}
