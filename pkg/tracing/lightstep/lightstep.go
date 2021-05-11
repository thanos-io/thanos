// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package lightstep

import (
	"context"
	"io"
	"os"
	"strings"

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

	// Tags is a string comma-delimited of key value pairs that holds metadata that will be sent to lightstep
	Tags string `yaml:"tags"`
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

	var tags opentracing.Tags
	if config.Tags != "" {
		tags = parseTags(config.Tags)
	}

	options := lightstep.Options{
		AccessToken: config.AccessToken,
		Collector:   config.Collector,
		Tags:        tags,
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

// parseTags parses the given string into a map of strings to empty interface.
// Spec for this value:
// - comma separated list of key=value
// - value can be specified using the notation ${envVar:defaultValue}, where `envVar`
// is an environment variable and `defaultValue` is the value to use in case the env var is not set.
func parseTags(sTags string) opentracing.Tags {
	pairs := strings.Split(sTags, ",")
	tags := make(opentracing.Tags)
	for _, p := range pairs {
		kv := strings.SplitN(p, "=", 2)
		k, v := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])

		if strings.HasPrefix(v, "${") && strings.HasSuffix(v, "}") {
			ed := strings.SplitN(v[2:len(v)-1], ":", 2)
			e, d := ed[0], ed[1]
			v = os.Getenv(e)
			if v == "" && d != "" {
				v = d
			}
		}

		tags[k] = v
	}

	return tags
}
