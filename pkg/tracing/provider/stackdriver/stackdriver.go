package stackdriver

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/yaml.v2"
)

type Config struct {
	ServiceName  string `yaml:"service_name"`
	ProjectId    string `yaml:"project_id"`
	SampleFactor uint64 `yaml:"sample_factor"`
}

func parseConfig(conf []byte) (Config, error) {
	config := Config{}
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return Config{}, err
	}
	return config, nil
}

func NewTracer(ctx context.Context, logger log.Logger, conf []byte) (opentracing.Tracer, io.Closer, error) {
	config, err := parseConfig(conf)
	if err != nil {
		return nil, nil, err
	}
	println(config.ProjectId)
	return newGCloudTracer(ctx, logger, config.ProjectId, config.SampleFactor, config.ServiceName)
}
