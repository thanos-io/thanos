// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package chunk

import (
	"context"
	"time"
)

// BucketClient is used to enforce retention on chunk buckets.
type BucketClient interface {
	DeleteChunksBefore(ctx context.Context, ts time.Time) error
}
