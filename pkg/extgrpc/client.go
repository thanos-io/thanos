// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extgrpc

import (
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/health"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/mem"

	"github.com/thanos-io/thanos/pkg/extgrpc/lb/p2c"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type nonPoolingCodec struct {
	encoding.CodecV2
}

func init() {
	encoding.RegisterCodecV2(&nonPoolingCodec{
		CodecV2: encoding.GetCodecV2("proto"),
	})
}

func (n *nonPoolingCodec) Name() string {
	return "proto"
}

var nopPool = mem.NopBufferPool{}

func (n *nonPoolingCodec) Unmarshal(data mem.BufferSlice, v any) error {
	gmsg, ok := v.(gogoMsg)
	if !ok {
		return n.CodecV2.Unmarshal(data, v)
	}

	// TODO(GiedriusS): we use unsafe code around labels so we cannot use pooling here.
	buf := data.MaterializeToBuffer(nopPool)

	return gmsg.Unmarshal(buf.ReadOnlyData())
}

type gogoMsg interface {
	Size() int
	MarshalToSizedBuffer([]byte) (int, error)
	Unmarshal([]byte) error
}

func (c *nonPoolingCodec) Marshal(v any) (mem.BufferSlice, error) {
	gmsg, ok := v.(gogoMsg)
	if !ok {
		return c.CodecV2.Marshal(v)
	}
	size := gmsg.Size()
	pool := mem.DefaultBufferPool()
	buf := pool.Get(size)

	n, err := gmsg.MarshalToSizedBuffer((*buf)[:size])
	if err != nil {
		pool.Put(buf)
		return mem.BufferSlice{}, err
	}

	bufExact := (*buf)[:n]

	return mem.BufferSlice{mem.NewBuffer(&bufExact, pool)}, nil
}

// DefaultServiceConfig is the default gRPC service config applied to all client connections.
// It enables round-robin load balancing and retries on transient UNAVAILABLE errors.
// For details on retry capabilities, see https://github.com/grpc/proposal/blob/master/A6-client-retries.md#retry-policy-capabilities
const DefaultServiceConfig = `{
  "loadBalancingPolicy": "round_robin",
  "healthCheckConfig": {
    "serviceName": ""
  },
  "methodConfig": [{
    "name": [{}],
    "retryPolicy": {
      "maxAttempts": 3,
      "initialBackoff": "0.1s",
      "maxBackoff": "1s",
      "backoffMultiplier": 2,
      "retryableStatusCodes": ["UNAVAILABLE"]
    }
  }],
  "retryThrottling": {
    "maxTokens": 100,
    "tokenRatio": 1.0
  }
}`

// StoreClientGRPCOption is a functional option for StoreClientGRPCOpts.
type StoreClientGRPCOption func(*storeClientGRPCOpts)

type storeClientGRPCOpts struct {
	serviceConfig                string
	secure                       bool
	skipVerify                   bool
	cert                         string
	key                          string
	caCert                       string
	serverName                   string
	compression                  string
	keepaliveTime                time.Duration
	keepaliveTimeout             time.Duration
	keepalivePermitWithoutStream bool
}

// WithTLS enables TLS for the gRPC client connection.
// Presence of this option implies secure=true.
// Empty cert/key/caCert/serverName values are valid and result in TLS with system CA pool.
func WithTLS(cert, key, caCert, serverName string, skipVerify bool) StoreClientGRPCOption {
	return func(o *storeClientGRPCOpts) {
		o.secure = true
		o.cert = cert
		o.key = key
		o.caCert = caCert
		o.serverName = serverName
		o.skipVerify = skipVerify
	}
}

// WithServiceConfig overrides the default gRPC service config. No-op if empty.
func WithServiceConfig(serviceConfig string) StoreClientGRPCOption {
	return func(o *storeClientGRPCOpts) {
		if serviceConfig != "" {
			o.serviceConfig = serviceConfig
		}
	}
}

// WithCompression enables gRPC compression with the given algorithm. No-op if empty or "none".
func WithCompression(compression string) StoreClientGRPCOption {
	return func(o *storeClientGRPCOpts) {
		if compression != "" && compression != "none" {
			o.compression = compression
		}
	}
}

// WithKeepaliveParams overrides the client keepalive parameters.
// A zero keepaliveTime or keepaliveTimeout falls back to the package default
// (10s / 5s). Note: grpc-go silently clamps keepaliveTime below 10s up to 10s.
func WithKeepaliveParams(keepaliveTime, keepaliveTimeout time.Duration, permitWithoutStream bool) StoreClientGRPCOption {
	return func(o *storeClientGRPCOpts) {
		o.keepaliveTime = keepaliveTime
		o.keepaliveTimeout = keepaliveTimeout
		o.keepalivePermitWithoutStream = permitWithoutStream
	}
}

// StoreClientGRPCOpts creates gRPC dial options for connecting to a store client.
// By default, DefaultServiceConfig is applied and connections are insecure.
// Use WithTLS, WithServiceConfig, and WithCompression to customize.
func StoreClientGRPCOpts(logger log.Logger, reg prometheus.Registerer, tracer opentracing.Tracer, options ...StoreClientGRPCOption) ([]grpc.DialOption, error) {
	o := &storeClientGRPCOpts{}
	for _, opt := range options {
		opt(o)
	}

	// Register the p2c balancer with the provided logger so diagnostics
	// flow through the same logger as the rest of the component.
	p2c.RegisterBalancer(logger)

	serviceConfig := o.serviceConfig
	if serviceConfig == "" {
		serviceConfig = DefaultServiceConfig
	}

	keepaliveTime := o.keepaliveTime
	if keepaliveTime == 0 {
		keepaliveTime = 10 * time.Second
	}
	keepaliveTimeout := o.keepaliveTimeout
	if keepaliveTimeout == 0 {
		keepaliveTimeout = 5 * time.Second
	}

	grpcMets := grpc_prometheus.NewClientMetrics(
		grpc_prometheus.WithClientHandlingTimeHistogram(grpc_prometheus.WithHistogramOpts(
			&prometheus.HistogramOpts{
				Buckets:                        []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120, 240, 360, 720},
				NativeHistogramMaxBucketNumber: 256,
				NativeHistogramBucketFactor:    1.1,
			},
		)),
	)
	dialOpts := []grpc.DialOption{
		// We want to make sure that we can receive huge gRPC messages from storeAPI.
		// On TCP level we can be fine, but the gRPC overhead for huge messages could be significant.
		// Current limit is ~2GB.
		// TODO(bplotka): Split sent chunks on store node per max 4MB chunks if needed.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
		grpc.WithChainUnaryInterceptor(
			grpcserver.NewUnaryClientRequestIDInterceptor(),
			grpcMets.UnaryClientInterceptor(),
			tracing.UnaryClientInterceptor(tracer),
		),
		grpc.WithChainStreamInterceptor(
			grpcserver.NewStreamClientRequestIDInterceptor(),
			grpcMets.StreamClientInterceptor(),
			tracing.StreamClientInterceptor(tracer),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveTime,
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: o.keepalivePermitWithoutStream,
		}),
		grpc.WithDefaultServiceConfig(serviceConfig),
		grpc.WithDisableServiceConfig(),
	}
	if o.compression != "" {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(o.compression)))
	}
	if reg != nil {
		reg.MustRegister(grpcMets)
	}

	if !o.secure {
		return append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials())), nil
	}

	level.Info(logger).Log("msg", "enabling client to server TLS")

	tlsCfg, err := tls.NewClientConfig(logger, o.cert, o.key, o.caCert, o.serverName, o.skipVerify)
	if err != nil {
		return nil, err
	}
	return append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))), nil
}
