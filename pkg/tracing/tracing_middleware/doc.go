// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

/*
This was copied over from https://github.com/grpc-ecosystem/go-grpc-middleware/tree/v2.0.0-rc.3
and modified to support tracing in Thanos till migration to Otel is supported.
*/

// Package tracing_middleware
/*
tracing is a "parent" package for gRPC logging middlewares.

This middleware relies on OpenTracing as our tracing interface.

OpenTracing Interceptors

These are both client-side and server-side interceptors for OpenTracing. They are a provider-agnostic, with backends
such as Zipkin, or Google Stackdriver Trace.

For a service that sends out requests and receives requests, you *need* to use both, otherwise downstream requests will
not have the appropriate requests propagated.

All server-side spans are tagged with grpc_ctxtags information.

For more information see:
http://opentracing.io/documentation/
https://github.com/opentracing/specification/blob/master/semantic_conventions.md
*/
package tracing_middleware
