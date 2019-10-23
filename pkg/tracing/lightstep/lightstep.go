package lightstep

import (
	"context"
	"errors"
	"io"

	"github.com/lightstep/lightstep-tracer-go"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/yaml.v2"
)

type Config struct {
	// AccessToken is the unique API key for your LightStep project.  It is
	// available on your account page at https://app.lightstep.com/account
	AccessToken string `yaml:"access_token"`

	// Collector is the host, port, and plaintext option to use
	// for the collector.
	Collector lightstep.Endpoint `yaml:"collector"`
}

type Tracer struct {
	lightstep.Tracer
	ctx context.Context
}

func (t *Tracer) Close() error {
	lightstepTracer := t.Tracer
	lightstepTracer.Close(t.ctx)

	return nil
}

func NewTracer(ctx context.Context, yamlConfig []byte) (opentracing.Tracer, io.Closer, error) {
	config := Config{}
	if err := yaml.Unmarshal(yamlConfig, &config); err != nil {
		return nil, nil, err
	}

	options := lightstep.Options{
		AccessToken: config.AccessToken,
		Collector:   config.Collector,
	}
	lighstepTracer := lightstep.NewTracer(options)
	if lighstepTracer == nil { // lightstep.NewTracer returns nil when there is an error
		return nil, nil, errors.New("error creating Lightstep tracer")
	}

	t := &Tracer{
		lighstepTracer,
		ctx,
	}
	return t, t, nil
}
