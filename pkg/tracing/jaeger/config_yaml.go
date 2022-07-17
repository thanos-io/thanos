// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package jaeger

import (
	"log"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/samplers/jaegerremote"
	"go.opentelemetry.io/otel/attribute"
	otel_jaeger "go.opentelemetry.io/otel/exporters/jaeger"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

type ParentBasedSamplerConfig struct {
	LocalParentSampled  bool `yaml:"local_parent_sampled"`
	RemoteParentSampled bool `yaml:"remote_parent_sampled"`
}

// Config - YAML configuration. For details see to https://github.com/jaegertracing/jaeger-client-go#environment-variables.
type Config struct {
	ServiceName                        string                   `yaml:"service_name"`
	Disabled                           bool                     `yaml:"disabled"`
	RPCMetrics                         bool                     `yaml:"rpc_metrics"`
	Tags                               string                   `yaml:"tags"`
	SamplerType                        string                   `yaml:"sampler_type"`
	SamplerParam                       float64                  `yaml:"sampler_param"`
	SamplerManagerHostPort             string                   `yaml:"sampler_manager_host_port"`
	SamplerMaxOperations               int                      `yaml:"sampler_max_operations"`
	SamplerRefreshInterval             time.Duration            `yaml:"sampler_refresh_interval"`
	SamplerParentConfig                ParentBasedSamplerConfig `yaml:"sampler_parent_config"`
	ReporterMaxQueueSize               int                      `yaml:"reporter_max_queue_size"`
	ReporterFlushInterval              time.Duration            `yaml:"reporter_flush_interval"`
	ReporterLogSpans                   bool                     `yaml:"reporter_log_spans"`
	ReporterDisableAttemptReconnecting bool                     `yaml:"reporter_disable_attempt_reconnecting"`
	ReporterAttemptReconnectInterval   time.Duration            `yaml:"reporter_attempt_reconnect_interval"`
	Endpoint                           string                   `yaml:"endpoint"`
	User                               string                   `yaml:"user"`
	Password                           string                   `yaml:"password"`
	AgentHost                          string                   `yaml:"agent_host"`
	AgentPort                          int                      `yaml:"agent_port"`
	SamplingServerURL                  bool                     `yaml:"sampling_server_url"`
	OperationNameLateBinding           bool                     `yaml:"operation_name_late_binding"`
	Gen128Bit                          bool                     `yaml:"traceid_128bit"`
	// Remove the above field. Ref: https://github.com/open-telemetry/opentelemetry-specification/issues/525#issuecomment-605519217
	// Ref: https://opentelemetry.io/docs/reference/specification/trace/api/#spancontext
}

// getCollectorEndpoints returns Jaeger options populated with collector related options.
func getCollectorEndpoints(config Config) []otel_jaeger.CollectorEndpointOption {
	var collectorOptions []otel_jaeger.CollectorEndpointOption
	if config.User != "" {
		collectorOptions = append(collectorOptions, otel_jaeger.WithUsername(config.User))
	}
	if config.Password != "" {
		collectorOptions = append(collectorOptions, otel_jaeger.WithPassword(config.Password))
	}
	collectorOptions = append(collectorOptions, otel_jaeger.WithEndpoint(config.Endpoint))

	return collectorOptions
}

// getAgentEndpointOptions returns Jaeger options populated with agent related options.
func getAgentEndpointOptions(config Config) []otel_jaeger.AgentEndpointOption {
	var endpointOptions []otel_jaeger.AgentEndpointOption
	endpointOptions = append(endpointOptions, otel_jaeger.WithAgentHost(config.AgentHost))
	endpointOptions = append(endpointOptions, otel_jaeger.WithAgentPort(strconv.Itoa(config.AgentPort)))

	// This option was part of the Jaeger config was JAEGER_REPORTER_ATTEMPT_RECONNECTING_DISABLED.
	if config.ReporterDisableAttemptReconnecting {
		endpointOptions = append(endpointOptions, otel_jaeger.WithDisableAttemptReconnecting())
		if config.ReporterAttemptReconnectInterval != 0 {
			endpointOptions = append(endpointOptions, otel_jaeger.WithAttemptReconnectingInterval(config.ReporterAttemptReconnectInterval))
		}
	}

	if config.ReporterLogSpans {
		var logger *log.Logger
		endpointOptions = append(endpointOptions, otel_jaeger.WithLogger(logger))
	}

	return endpointOptions
}

// getSamplingFraction returns the sampling fraction based on the sampler type.
// Ref: https://www.jaegertracing.io/docs/1.35/sampling/#client-sampling-configuration
func getSamplingFraction(samplerType string, samplingFactor float64) float64 {
	if samplerType == "const" {
		if samplingFactor > 1 {
			return 1.0
		} else if samplingFactor < 0 {
			return 0.0
		}
		return math.Round(samplingFactor) // Returns either 0 or 1 for values [0,1].
	} else if samplerType == "probabilistic" {
		return samplingFactor
	} else if samplerType == "ratelimiting" {
		return math.Round(samplingFactor) // Needs to be an integer.
	}
	return samplingFactor
}

func getSampler(config Config) tracesdk.Sampler {
	samplerType := config.SamplerType
	samplingFraction := getSamplingFraction(samplerType, config.SamplerParam)

	var sampler tracesdk.Sampler
	if samplerType == "probabilistic" {
		sampler = tracesdk.ParentBased(tracesdk.TraceIDRatioBased(samplingFraction))
	} else if samplerType == "const" {
		if samplingFraction == 1.0 {
			sampler = tracesdk.AlwaysSample()
		} else {
			sampler = tracesdk.NeverSample()
		}
	} else if samplerType == "remote" {
		var remoteOptions []jaegerremote.Option
		if config.SamplerRefreshInterval != 0 {
			remoteOptions = append(remoteOptions, jaegerremote.WithSamplingRefreshInterval(config.SamplerRefreshInterval))
		}
		if config.SamplingServerURL && config.AgentHost != "" && config.AgentPort != 0 {
			localAgentURL := &(url.URL{
				Host: config.AgentHost + ":" + strconv.Itoa(config.AgentPort),
			})
			remoteOptions = append(remoteOptions, jaegerremote.WithSamplingServerURL(localAgentURL.String()))
		}
		if config.SamplerMaxOperations != 0 {
			remoteOptions = append(remoteOptions, jaegerremote.WithMaxOperations(config.SamplerMaxOperations))
		}
		if config.OperationNameLateBinding {
			remoteOptions = append(remoteOptions, jaegerremote.WithOperationNameLateBinding(true))
		}
		sampler = jaegerremote.New(config.ServiceName, remoteOptions...)
	} else { // Default sampler type is parent based.
		var root tracesdk.Sampler
		var parentOptions []tracesdk.ParentBasedSamplerOption
		if config.SamplerParentConfig.LocalParentSampled {
			parentOptions = append(parentOptions, tracesdk.WithLocalParentSampled(root))
		}
		if config.SamplerParentConfig.RemoteParentSampled {
			parentOptions = append(parentOptions, tracesdk.WithRemoteParentSampled(root))
		}
		sampler = tracesdk.ParentBased(root, parentOptions...)
	}
	return sampler
}

// parseTags parses the given string into a collection of attributes.
// Spec for this value:
// - comma separated list of key=value
// - value can be specified using the notation ${envVar:defaultValue}, where `envVar`
// is an environment variable and `defaultValue` is the value to use in case the env var is not set.
// TODO(aditi): when Lighstep and Elastic APM have been migrated, move 'parseTags()' to the common 'tracing' package.
func parseTags(sTags string) []attribute.KeyValue {
	pairs := strings.Split(sTags, ",")
	tags := make([]attribute.KeyValue, 0)
	for _, p := range pairs {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) < 2 {
			continue // to avoid panic
		}
		k, v := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])

		if strings.HasPrefix(v, "${") && strings.HasSuffix(v, "}") {
			ed := strings.SplitN(v[2:len(v)-1], ":", 2)
			e, d := ed[0], ed[1]
			v = os.Getenv(e)
			if v == "" && d != "" {
				v = d
			}
		}

		tag := attribute.String(k, v)
		tags = append(tags, tag)
	}

	return tags
}
