// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package jaeger

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"gopkg.in/yaml.v2"
)

// Config - YAML configuration. For details see to https://github.com/jaegertracing/jaeger-client-go#environment-variables.
type Config struct {
	ServiceName            string        `yaml:"service_name"`
	Disabled               bool          `yaml:"disabled"`
	RPCMetrics             bool          `yaml:"rpc_metrics"`
	Tags                   string        `yaml:"tags"`
	SamplerType            string        `yaml:"sampler_type"`
	SamplerParam           float64       `yaml:"sampler_param"`
	SamplerManagerHostPort string        `yaml:"sampler_manager_host_port"`
	SamplerMaxOperations   int           `yaml:"sampler_max_operations"`
	SamplerRefreshInterval time.Duration `yaml:"sampler_refresh_interval"`
	ReporterMaxQueueSize   int           `yaml:"reporter_max_queue_size"`
	ReporterFlushInterval  time.Duration `yaml:"reporter_flush_interval"`
	ReporterLogSpans       bool          `yaml:"reporter_log_spans"`
	Endpoint               string        `yaml:"endpoint"`
	User                   string        `yaml:"user"`
	Password               string        `yaml:"password"`
	AgentHost              string        `yaml:"agent_host"`
	AgentPort              int           `yaml:"agent_port"`
}

// ParseConfigFromYaml uses config YAML to set the tracer's Configuration.
func ParseConfigFromYaml(cfg []byte) (*config.Configuration, error) {
	conf := &Config{}

	if err := yaml.Unmarshal(cfg, &conf); err != nil {
		return nil, err
	}

	c := &config.Configuration{}

	if conf.ServiceName != "" {
		c.ServiceName = conf.ServiceName
	}

	if conf.RPCMetrics {
		c.RPCMetrics = conf.RPCMetrics
	}

	if conf.Disabled {
		c.Disabled = conf.Disabled
	}

	if conf.Tags != "" {
		c.Tags = parseTags(conf.Tags)
	}

	c.Sampler = samplerConfigFromConfig(*conf)

	if r, err := reporterConfigFromConfig(*conf); err == nil {
		c.Reporter = r
	} else {
		return nil, errors.Wrap(err, "cannot obtain reporter config from YAML")
	}

	return c, nil
}

// samplerConfigFromConfig creates a new SamplerConfig based on the YAML Config.
func samplerConfigFromConfig(cfg Config) *config.SamplerConfig {
	sc := &config.SamplerConfig{}

	if cfg.SamplerType != "" {
		sc.Type = cfg.SamplerType
	}

	if cfg.SamplerParam != 0 {
		sc.Param = cfg.SamplerParam
	}

	if cfg.SamplerManagerHostPort != "" {
		sc.SamplingServerURL = cfg.SamplerManagerHostPort
	}

	if cfg.SamplerMaxOperations != 0 {
		sc.MaxOperations = cfg.SamplerMaxOperations
	}

	if cfg.SamplerRefreshInterval != 0 {
		sc.SamplingRefreshInterval = cfg.SamplerRefreshInterval
	}

	return sc
}

// reporterConfigFromConfig creates a new ReporterConfig based on the YAML Config.
func reporterConfigFromConfig(cfg Config) (*config.ReporterConfig, error) {
	rc := &config.ReporterConfig{}

	if cfg.ReporterMaxQueueSize != 0 {
		rc.QueueSize = cfg.ReporterMaxQueueSize
	}

	if cfg.ReporterFlushInterval != 0 {
		rc.BufferFlushInterval = cfg.ReporterFlushInterval
	}

	if cfg.ReporterLogSpans {
		rc.LogSpans = cfg.ReporterLogSpans
	}

	if cfg.Endpoint != "" {
		u, err := url.ParseRequestURI(cfg.Endpoint)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot parse endpoint=%s", cfg.Endpoint)
		}
		rc.CollectorEndpoint = u.String()
		user := cfg.User
		pswd := cfg.Password
		if user != "" && pswd == "" || user == "" && pswd != "" {
			return nil, errors.Errorf("you must set %s and %s parameters together", cfg.User, cfg.Password)
		}
		rc.User = user
		rc.Password = pswd
	} else {
		host := jaeger.DefaultUDPSpanServerHost
		if cfg.AgentHost != "" {
			host = cfg.AgentHost
		}

		port := jaeger.DefaultUDPSpanServerPort
		if cfg.AgentPort != 0 {
			port = cfg.AgentPort
		}
		rc.LocalAgentHostPort = fmt.Sprintf("%s:%d", host, port)
	}

	return rc, nil
}

// parseTags parses the given string into a collection of Tags.
// Spec for this value:
// - comma separated list of key=value
// - value can be specified using the notation ${envVar:defaultValue}, where `envVar`
// is an environment variable and `defaultValue` is the value to use in case the env var is not set.
func parseTags(sTags string) []opentracing.Tag {
	pairs := strings.Split(sTags, ",")
	tags := make([]opentracing.Tag, 0)
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

		tag := opentracing.Tag{Key: k, Value: v}
		tags = append(tags, tag)
	}

	return tags
}
