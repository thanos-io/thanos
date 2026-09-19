// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

/*
This was copied over from https://github.com/grpc-ecosystem/go-grpc-middleware/tree/v2.0.0-rc.3
and modified to support tracing in Thanos till migration to Otel is supported.
*/

/*
Package `metautils` provides convenience functions for dealing with gRPC metadata.MD objects inside
Context handlers.

While the upstream grpc-go package contains decent functionality (see https://github.com/grpc/grpc-go/blob/master/Documentation/grpc-metadata.md)
they are hard to use.

The majority of functions center around the NiceMD, which is a convenience wrapper around metadata.MD. For example
the following code allows you to easily extract incoming metadata (server handler) and put it into a new client context
metadata.

  nmd := metautils.ExtractIncoming(serverCtx).Clone(":authorization", ":custom")
  clientCtx := nmd.Set("x-client-header", "2").Set("x-another", "3").ToOutgoing(ctx)
*/

package metautils
