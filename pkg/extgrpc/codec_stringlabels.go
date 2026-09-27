// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

//go:build !slicelabels

package extgrpc

import "google.golang.org/grpc/mem"

func materializeForUnmarshal(data mem.BufferSlice) ([]byte, func()) {
	buf := data.MaterializeToBuffer(mem.DefaultBufferPool())
	return buf.ReadOnlyData(), buf.Free
}
