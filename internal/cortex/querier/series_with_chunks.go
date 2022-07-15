// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package querier

import (
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/thanos/internal/cortex/chunk"
)

// SeriesWithChunks extends storage.Series interface with direct access to Cortex chunks.
type SeriesWithChunks interface {
	storage.Series

	// Returns all chunks with series data.
	Chunks() []chunk.Chunk
}
