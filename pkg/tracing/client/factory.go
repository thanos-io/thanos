package client

import (
	"context"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"io"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/tracing/elasticapm"
	"github.com/thanos-io/thanos/pkg/tracing/jaeger"
	"github.com/thanos-io/thanos/pkg/tracing/stackdriver"
	"gopkg.in/yaml.v2"
)

type TracingProvider string

const (
	STACKDRIVER TracingProvider = "STACKDRIVER"
	JAEGER      TracingProvider = "JAEGER"
	ELASTIC_APM TracingProvider = "ELASTIC_APM"
)

type TracingConfig struct {
	Type            TracingProvider `yaml:"type"`
	// Methods that are filtered out from tracing. E.g /thanos.Store/Info
	MethodBlacklist []string        `yaml:"method_blacklist"`
	Config          interface{}     `yaml:"config"`
}

// NewTracer returns new tracer from config or error i unknown.
func NewTracer(ctx context.Context, logger log.Logger, metrics *prometheus.Registry, confContentYaml []byte) (opentracing.Tracer, io.Closer, error) {
	level.Info(logger).Log("msg", "loading tracing configuration")
	tracingConf := &TracingConfig{}

	if err := yaml.UnmarshalStrict(confContentYaml, tracingConf); err != nil {
		return nil, nil, errors.Wrap(err, "parsing config tracing YAML")
	}

	var config []byte
	var err error
	if tracingConf.Config != nil {
		config, err = yaml.Marshal(tracingConf.Config)
		if err != nil {
			return nil, nil, errors.Wrap(err, "marshal content of tracing configuration")
		}
	}

	switch strings.ToUpper(string(tracingConf.Type)) {
	case string(STACKDRIVER):
		return stackdriver.NewTracer(ctx, logger, config)
	case string(JAEGER):
		return jaeger.NewTracer(ctx, logger, metrics, config)
	case string(ELASTIC_APM):
		return elasticapm.NewTracer(config)
	default:
		return nil, nil, errors.Errorf("tracing with type %s is not supported", tracingConf.Type)
	}
}

// NoopTracer returns noop tracer.
func NoopTracer() opentracing.Tracer {
	return &opentracing.NoopTracer{}
}

// Blacklist filter returns gRPC opentracing filter which disables tracing for given full method names.
func BlacklistFilter(blacklistMethod []string) grpc_opentracing.FilterFunc {
	blacklist := make(map[string]struct{}, len(blacklistMethod))
	for _, m := range blacklistMethod {
		blacklist[m] = struct{}{}
	}
	return func(ctx context.Context, fullMethodName string) bool {
		if _, ok := blacklist[fullMethodName]; ok {
			return false
		}
		return true
	}
}