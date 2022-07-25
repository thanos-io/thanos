// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extgrpc

import (
	"math"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/httpconfig"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// EndpointsConfig configures a cluster of gRPC endpoints from static addresses and
// file service discovery. Similar to exthttp.EndpointConfig but for gRPC.
type EndpointsConfig struct {
	// List of addresses with DNS prefixes.
	Addresses []string `yaml:"addresses"`
	// List of file configurations (our FileSD supports different DNS lookups).
	FileSDConfigs []httpconfig.FileSDConfig `yaml:"file_sd_configs"`
}

// NewDiscoverer returns a new exthttp.Discoverer.
func NewDiscoverer(logger log.Logger, cfg EndpointsConfig, provider httpconfig.AddressProvider) (*httpconfig.Discoverer, error) {
	return httpconfig.NewDiscoverer(logger, httpconfig.EndpointsConfig{
		Addresses:     cfg.Addresses,
		FileSDConfigs: cfg.FileSDConfigs,
	}, provider)
}

// TODO: Description
func ClientGRPCMetrics(reg *prometheus.Registry, clientName string) *grpc_prometheus.ClientMetrics {
	if clientName == "" {
		clientName = "default"
	}

	grpcMets := grpc_prometheus.NewClientMetrics(grpc_prometheus.WithConstLabels(map[string]string{"client": clientName}))
	grpcMets.EnableClientHandlingTimeHistogram(
		grpc_prometheus.WithHistogramConstLabels(map[string]string{"client": clientName}),
		grpc_prometheus.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120, 240, 360, 720}),
	)
	if reg != nil {
		reg.MustRegister(grpcMets)
	}

	return grpcMets
}

// ClientGRPCOpts creates gRPC dial options from config..
func ClientGRPCOpts(logger log.Logger, tracer opentracing.Tracer, metrics *grpc_prometheus.ClientMetrics, config httpconfig.ClientConfig) ([]grpc.DialOption, error) {
	dialOpts := []grpc.DialOption{
		// We want to make sure that we can receive huge gRPC messages from storeAPI.
		// On TCP level we can be fine, but the gRPC overhead for huge messages could be significant.
		// Current limit is ~2GB.
		// TODO(bplotka): Split sent chunks on store node per max 4MB chunks if needed.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
		grpc.WithUnaryInterceptor(
			grpc_middleware.ChainUnaryClient(
				metrics.UnaryClientInterceptor(),
				tracing.UnaryClientInterceptor(tracer),
			),
		),
		grpc.WithStreamInterceptor(
			grpc_middleware.ChainStreamClient(
				metrics.StreamClientInterceptor(),
				tracing.StreamClientInterceptor(tracer),
			),
		),
	}

	// TODO(bwplotka): Add here support for non TLS exthttp.ClientConfig options, for not block them.
	if (config.BasicAuth != httpconfig.BasicAuth{} || config.BearerToken != "" || config.BearerTokenFile != "" || config.ProxyURL != "") {
		return nil, errors.New("basic auth, bearer token and proxy URL options are currently not implemented")

	}

	if (config.TLSConfig == httpconfig.TLSConfig{}) {
		return append(dialOpts, grpc.WithInsecure()), nil
	}

	level.Info(logger).Log("msg", "enabling client to server TLS")

	tlsCfg, err := tls.NewClientConfig(logger, config.TLSConfig.CertFile, config.TLSConfig.KeyFile, config.TLSConfig.CAFile, config.TLSConfig.ServerName, config.TLSConfig.InsecureSkipVerify)
	if err != nil {
		return nil, err
	}
	return append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))), nil
}
