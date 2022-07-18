// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package tsdb

import (
	"github.com/oklog/ulid"

	"github.com/thanos-io/thanos/internal/cortex/ingester/client"
)

// HashBlockID returns a 32-bit hash of the block ID useful for
// ring-based sharding.
func HashBlockID(id ulid.ULID) uint32 {
	h := client.HashNew32()
	for _, b := range id {
		h = client.HashAddByte32(h, b)
	}
	return h
}
