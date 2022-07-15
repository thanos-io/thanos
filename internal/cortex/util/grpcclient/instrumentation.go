// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package grpcclient

import (
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"

	cortexmiddleware "github.com/thanos-io/thanos/internal/cortex/util/middleware"
)

func Instrument(requestDuration *prometheus.HistogramVec) ([]grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor) {
	return []grpc.UnaryClientInterceptor{
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
			cortexmiddleware.PrometheusGRPCUnaryInstrumentation(requestDuration),
		}, []grpc.StreamClientInterceptor{
			otgrpc.OpenTracingStreamClientInterceptor(opentracing.GlobalTracer()),
			middleware.StreamClientUserHeaderInterceptor,
			cortexmiddleware.PrometheusGRPCStreamInstrumentation(requestDuration),
		}
}
