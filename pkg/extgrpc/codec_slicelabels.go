// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

//go:build slicelabels

package extgrpc

import "google.golang.org/grpc/mem"

var materializePool mem.BufferPool = mem.NopBufferPool{}

func materializeForUnmarshal(data mem.BufferSlice) ([]byte, func()) {
	buf := data.MaterializeToBuffer(materializePool)
	return buf.ReadOnlyData(), nil
}
