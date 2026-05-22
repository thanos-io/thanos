// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

//go:build slicelabels

package extgrpc

import "google.golang.org/grpc/mem"

var nopPool = mem.NopBufferPool{}

func materializeForUnmarshal(data mem.BufferSlice) ([]byte, func()) {
	buf := data.MaterializeToBuffer(nopPool)
	return buf.ReadOnlyData(), nil
}
