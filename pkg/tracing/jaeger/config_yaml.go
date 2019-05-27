package jaeger

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"gopkg.in/yaml.v2"
)

type Config struct {
	ServiceName            string `yaml:"service_name"`
	Disabled               string `yaml:"disabled"`
	RPCMetrics             string `yaml:"rpc_metrics"`
	Tags                   string `yaml:"tags"`
	SamplerType            string `yaml:"sampler_type"`
	SamplerParam           string `yaml:"sampler_param"`
	SamplerManagerHostPort string `yaml:"sampler_manager_host_port"`
	SamplerMaxOperations   string `yaml:"sampler_max_operations"`
	SamplerRefreshInterval string `yaml:"sampler_refresh_interval"`
	ReporterMaxQueueSize   string `yaml:"reporter_max_queue_size"`
	ReporterFlushInterval  string `yaml:"reporter_flush_interval"`
	ReporterLogSpans       string `yaml:"reporter_log_spans"`
	Endpoint               string `yaml:"endpoint"`
	User                   string `yaml:"user"`
	Password               string `yaml:"password"`
	AgentHost              string `yaml:"agent_host"`
	AgentPort              string `yaml:"agent_port"`
}

// FromYaml uses config YAML to set the tracer's Configuration.
func FromYaml(cfg []byte) (*config.Configuration, error) {
	conf := &Config{}

	if err := yaml.Unmarshal(cfg, &conf); err != nil {
		return nil, err
	}

	c := &config.Configuration{}

	if e := conf.ServiceName; e != "" {
		c.ServiceName = e
	}

	if e := conf.RPCMetrics; e != "" {
		if value, err := strconv.ParseBool(e); err == nil {
			c.RPCMetrics = value
		} else {
			return nil, errors.Wrapf(err, "cannot parse env var %s=%s", conf.RPCMetrics, e)
		}
	}

	if e := os.Getenv(conf.Disabled); e != "" {
		if value, err := strconv.ParseBool(e); err == nil {
			c.Disabled = value
		} else {
			return nil, errors.Wrapf(err, "cannot parse env var %s=%s", conf.Disabled, e)
		}
	}

	if e := conf.Tags; e != "" {
		c.Tags = parseTags(e)
	}

	if s, err := samplerConfigFromConfig(*conf); err == nil {
		c.Sampler = s
	} else {
		return nil, errors.Wrap(err, "cannot obtain sampler config from env")
	}

	if r, err := reporterConfigFromEnv(*conf); err == nil {
		c.Reporter = r
	} else {
		return nil, errors.Wrap(err, "cannot obtain reporter config from env")
	}

	return c, nil
}

// samplerConfigFromConfig creates a new SamplerConfig based on the YAML Config.
func samplerConfigFromConfig(cfg Config) (*config.SamplerConfig, error) {
	sc := &config.SamplerConfig{}

	if e := cfg.SamplerType; e != "" {
		sc.Type = e
	}

	if e := cfg.SamplerParam; e != "" {
		if value, err := strconv.ParseFloat(e, 64); err == nil {
			sc.Param = value
		} else {
			return nil, errors.Wrapf(err, "cannot parse env var %s=%s", cfg.SamplerParam, e)
		}
	}

	if e := cfg.SamplerManagerHostPort; e != "" {
		sc.SamplingServerURL = e
	}

	if e := cfg.SamplerMaxOperations; e != "" {
		if value, err := strconv.ParseInt(e, 10, 0); err == nil {
			sc.MaxOperations = int(value)
		} else {
			return nil, errors.Wrapf(err, "cannot parse env var %s=%s", cfg.SamplerMaxOperations, e)
		}
	}

	if e := cfg.SamplerRefreshInterval; e != "" {
		if value, err := time.ParseDuration(e); err == nil {
			sc.SamplingRefreshInterval = value
		} else {
			return nil, errors.Wrapf(err, "cannot parse env var %s=%s", cfg.SamplerRefreshInterval, e)
		}
	}

	return sc, nil
}

// reporterConfigFromConfig creates a new ReporterConfig based on the YAML Config.
func reporterConfigFromEnv(cfg Config) (*config.ReporterConfig, error) {
	rc := &config.ReporterConfig{}

	if e := cfg.ReporterMaxQueueSize; e != "" {
		if value, err := strconv.ParseInt(e, 10, 0); err == nil {
			rc.QueueSize = int(value)
		} else {
			return nil, errors.Wrapf(err, "cannot parse env var %s=%s", cfg.ReporterMaxQueueSize, e)
		}
	}

	if e := cfg.ReporterFlushInterval; e != "" {
		if value, err := time.ParseDuration(e); err == nil {
			rc.BufferFlushInterval = value
		} else {
			return nil, errors.Wrapf(err, "cannot parse env var %s=%s", cfg.ReporterFlushInterval, e)
		}
	}

	if e := cfg.ReporterLogSpans; e != "" {
		if value, err := strconv.ParseBool(e); err == nil {
			rc.LogSpans = value
		} else {
			return nil, errors.Wrapf(err, "cannot parse env var %s=%s", cfg.ReporterLogSpans, e)
		}
	}

	if e := cfg.Endpoint; e != "" {
		u, err := url.ParseRequestURI(e)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot parse env var %s=%s", cfg.Endpoint, e)
		}
		rc.CollectorEndpoint = u.String()
		user := cfg.User
		pswd := cfg.Password
		if user != "" && pswd == "" || user == "" && pswd != "" {
			return nil, errors.Errorf("you must set %s and %s env vars together", cfg.User, cfg.Password)
		}
		rc.User = user
		rc.Password = pswd
	} else {
		host := jaeger.DefaultUDPSpanServerHost
		if e := cfg.AgentHost; e != "" {
			host = e
		}

		port := jaeger.DefaultUDPSpanServerPort
		if e := cfg.AgentPort; e != "" {
			if value, err := strconv.ParseInt(e, 10, 0); err == nil {
				port = int(value)
			} else {
				return nil, errors.Wrapf(err, "cannot parse env var %s=%s", cfg.AgentPort, e)
			}
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
