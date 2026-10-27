// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// ApplyRetentionPolicyByResolution removes blocks depending on the specified retentionByResolution based on blocks MaxTime.
// A value of 0 disables the retention for its resolution.
func ApplyRetentionPolicyByResolution(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.Bucket,
	metas map[ulid.ULID]*metadata.Meta,
	retentionByResolution map[ResolutionLevel]time.Duration,
	blocksMarkedForDeletion prometheus.Counter,
) error {
	level.Info(logger).Log("msg", "start optional retention")
	for id, m := range metas {
		if IsRetentionExpired(m, retentionByResolution, time.Now()) {
			retentionDuration := retentionByResolution[ResolutionLevel(m.Thanos.Downsample.Resolution)]
			level.Info(logger).Log("msg", "applying retention: marking block for deletion", "id", id, "maxTime", time.Unix(m.MaxTime/1000, 0).String())
			if err := block.MarkForDeletion(ctx, logger, bkt, id, fmt.Sprintf("block exceeding retention of %v", retentionDuration), blocksMarkedForDeletion); err != nil {
				return errors.Wrap(err, "delete block")
			}
		}
	}
	level.Info(logger).Log("msg", "optional retention apply done")
	return nil
}

// IsRetentionExpired reports whether the block is past the retention configured for its resolution at the given time.
// A retention of 0 disables the retention for its resolution.
func IsRetentionExpired(m *metadata.Meta, retentionByResolution map[ResolutionLevel]time.Duration, now time.Time) bool {
	retentionDuration := retentionByResolution[ResolutionLevel(m.Thanos.Downsample.Resolution)]
	if retentionDuration.Seconds() == 0 {
		return false
	}
	return now.After(time.Unix(m.MaxTime/1000, 0).Add(retentionDuration))
}
