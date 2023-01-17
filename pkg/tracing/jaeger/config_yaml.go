// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package jaeger

import (
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	glog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.opentelemetry.io/contrib/samplers/jaegerremote"
	"go.opentelemetry.io/otel/attribute"
	otel_jaeger "go.opentelemetry.io/otel/exporters/jaeger"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

const (
	SamplerTypeRemote        = "remote"
	SamplerTypeProbabilistic = "probabilistic"
	SamplerTypeConstant      = "const"
	SamplerTypeRateLimiting  = "ratelimiting"
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
	SamplingServerURL                  string                   `yaml:"sampling_server_url"`
	OperationNameLateBinding           bool                     `yaml:"operation_name_late_binding"`
	InitialSamplingRate                float64                  `yaml:"initial_sampler_rate"`
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

	// This option, as part of the Jaeger config, was JAEGER_REPORTER_ATTEMPT_RECONNECTING_DISABLED.
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
	switch samplerType {
	case "const":
		if samplingFactor > 1 {
			return 1.0
		} else if samplingFactor < 0 {
			return 0.0
		}
		return math.Round(samplingFactor)

	case "probabilistic":
		return samplingFactor

	case "ratelimiting":
		return math.Round(samplingFactor) // Needs to be an integer
	}

	return samplingFactor
}

func getSampler(config Config) tracesdk.Sampler {
	samplerType := config.SamplerType
	if samplerType == "" {
		samplerType = SamplerTypeRateLimiting
	}
	samplingFraction := getSamplingFraction(samplerType, config.SamplerParam)

	var sampler tracesdk.Sampler
	switch samplerType {
	case SamplerTypeProbabilistic:
		sampler = tracesdk.TraceIDRatioBased(samplingFraction)
	case SamplerTypeConstant:
		if samplingFraction == 1.0 {
			sampler = tracesdk.AlwaysSample()
		} else {
			sampler = tracesdk.NeverSample()
		}
	case SamplerTypeRemote:
		remoteOptions := getRemoteOptions(config)
		sampler = jaegerremote.New(config.ServiceName, remoteOptions...)
	// Fallback always to default (rate limiting).
	case SamplerTypeRateLimiting:
	default:
		// The same config options are applicable to both remote and rate-limiting samplers.
		remoteOptions := getRemoteOptions(config)
		sampler = jaegerremote.New(config.ServiceName, remoteOptions...)
		sampler, ok := sampler.(*rateLimitingSampler)
		if ok {
			sampler.Update(config.SamplerParam)
		}
	}

	// Use parent-based to make sure we respect the span parent, if
	// it is sampled. Optionally, allow user to specify the
	// parent-based options.
	var parentOptions []tracesdk.ParentBasedSamplerOption
	if config.SamplerParentConfig.LocalParentSampled {
		parentOptions = append(parentOptions, tracesdk.WithLocalParentSampled(sampler))
	}
	if config.SamplerParentConfig.RemoteParentSampled {
		parentOptions = append(parentOptions, tracesdk.WithRemoteParentSampled(sampler))
	}
	sampler = tracesdk.ParentBased(sampler, parentOptions...)

	return sampler
}

func getRemoteOptions(config Config) []jaegerremote.Option {
	var remoteOptions []jaegerremote.Option
	if config.SamplerRefreshInterval != 0 {
		remoteOptions = append(remoteOptions, jaegerremote.WithSamplingRefreshInterval(config.SamplerRefreshInterval))
	}
	if config.SamplingServerURL != "" {
		remoteOptions = append(remoteOptions, jaegerremote.WithSamplingServerURL(config.SamplingServerURL))
	}
	if config.SamplerMaxOperations != 0 {
		remoteOptions = append(remoteOptions, jaegerremote.WithMaxOperations(config.SamplerMaxOperations))
	}
	if config.OperationNameLateBinding {
		remoteOptions = append(remoteOptions, jaegerremote.WithOperationNameLateBinding(true))
	}
	// SamplerRefreshInterval is the interval for polling the backend for sampling strategies.
	// Ref: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/sdk-environment-variables.md#general-sdk-configuration.
	if config.SamplerRefreshInterval != 0 {
		remoteOptions = append(remoteOptions, jaegerremote.WithSamplingRefreshInterval(config.SamplerRefreshInterval))
	}
	// InitialSamplingRate is the sampling probability when the backend is unreachable.
	if config.InitialSamplingRate != 0.0 {
		remoteOptions = append(remoteOptions, jaegerremote.WithInitialSampler(tracesdk.TraceIDRatioBased(config.InitialSamplingRate)))
	}

	return remoteOptions
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

// printDeprecationWarnings logs deprecation warnings for config options that are no
// longer supported.
func printDeprecationWarnings(config Config, l glog.Logger) {
	commonDeprecationMessage := " has been deprecated as a config option."
	if config.RPCMetrics {
		level.Info(l).Log("msg", "RPC Metrics"+commonDeprecationMessage)
	}
	if config.Gen128Bit {
		level.Info(l).Log("msg", "Gen128Bit"+commonDeprecationMessage)
	}
	if config.Disabled {
		level.Info(l).Log("msg", "Disabled"+commonDeprecationMessage)
	}
}
