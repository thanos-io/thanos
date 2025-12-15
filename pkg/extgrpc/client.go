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
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/mem"

	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// vtprotoCodec is a gRPC codec that uses vtprotobuf for optimized marshaling/unmarshaling.
// It supports memory pooling for WriteRequest to reduce allocations in high-throughput paths.
type vtprotoCodec struct {
	encoding.CodecV2
}

func init() {
	encoding.RegisterCodecV2(&vtprotoCodec{
		CodecV2: encoding.GetCodecV2("proto"),
	})
}

func (c *vtprotoCodec) Name() string {
	return "proto"
}

var nopPool = mem.NopBufferPool{}

// vtprotoMsg is the interface for vtprotobuf generated messages.
type vtprotoMsg interface {
	SizeVT() int
	MarshalToSizedBufferVT([]byte) (int, error)
	UnmarshalVT([]byte) error
}

func (c *vtprotoCodec) Unmarshal(data mem.BufferSlice, v any) error {
	// For other vtprotobuf messages, use the optimized UnmarshalVT method.
	if vtmsg, ok := v.(vtprotoMsg); ok {
		buf := data.MaterializeToBuffer(nopPool)
		return vtmsg.UnmarshalVT(buf.ReadOnlyData())
	}

	// Fallback to default codec for non-vtprotobuf messages.
	return c.CodecV2.Unmarshal(data, v)
}

func (c *vtprotoCodec) Marshal(v any) (mem.BufferSlice, error) {
	// Use vtprotobuf optimized marshaling for vtproto messages.
	if vtmsg, ok := v.(vtprotoMsg); ok {
		size := vtmsg.SizeVT()
		pool := mem.DefaultBufferPool()
		buf := pool.Get(size)

		n, err := vtmsg.MarshalToSizedBufferVT((*buf)[:size])
		if err != nil {
			pool.Put(buf)
			return mem.BufferSlice{}, err
		}

		bufExact := (*buf)[:n]
		return mem.BufferSlice{mem.NewBuffer(&bufExact, pool)}, nil
	}

	// Fallback to default codec for non-vtprotobuf messages.
	return c.CodecV2.Marshal(v)
}

// EndpointGroupGRPCOpts creates gRPC dial options for connecting to endpoint groups.
// For details on retry capabilities, see https://github.com/grpc/proposal/blob/master/A6-client-retries.md#retry-policy-capabilities
func EndpointGroupGRPCOpts(serviceConfig string) []grpc.DialOption {
	if serviceConfig == "" {
		serviceConfig = `
{
  "loadBalancingPolicy":"round_robin",
  "retryPolicy": {
    "maxAttempts": 3,
    "initialBackoff": "0.1s",
    "backoffMultiplier": 2,
    "retryableStatusCodes": [
  	  "UNAVAILABLE"
    ]
  }
}`
	}

	return []grpc.DialOption{
		grpc.WithDefaultServiceConfig(serviceConfig),
		grpc.WithDisableServiceConfig(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 5 * time.Second}),
	}
}

// StoreClientGRPCOpts creates gRPC dial options for connecting to a store client.
func StoreClientGRPCOpts(logger log.Logger, reg prometheus.Registerer, tracer opentracing.Tracer, secure, skipVerify bool, cert, key, caCert, serverName string) ([]grpc.DialOption, error) {
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
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 5 * time.Second}),
	}
	if reg != nil {
		reg.MustRegister(grpcMets)
	}

	if !secure {
		return append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials())), nil
	}

	level.Info(logger).Log("msg", "enabling client to server TLS")

	tlsCfg, err := tls.NewClientConfig(logger, cert, key, caCert, serverName, skipVerify)
	if err != nil {
		return nil, err
	}
	return append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))), nil
}
