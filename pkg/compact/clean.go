// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
)

// BlocksCleaner is a struct that deletes blocks from bucket which are marked for deletion.
type BlocksCleaner struct {
	logger               log.Logger
	bkt                  objstore.Bucket
	deleteDelay          time.Duration
	blocksCleaned        prometheus.Counter
	blockCleanupFailures prometheus.Counter
}

// NewBlocksCleaner creates a new BlocksCleaner.
func NewBlocksCleaner(logger log.Logger, bkt objstore.Bucket, reg prometheus.Registerer, deleteDelay time.Duration) *BlocksCleaner {
	blocksCleaned := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compactor_blocks_cleaned_total",
		Help: "Total number of blocks deleted in compactor.",
	})
	blockCleanupFailures := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compactor_block_cleanup_failures_total",
		Help: "Failures encountered while deleting blocks in compactor.",
	})

	return &BlocksCleaner{
		logger:               logger,
		bkt:                  bkt,
		deleteDelay:          deleteDelay,
		blocksCleaned:        blocksCleaned,
		blockCleanupFailures: blockCleanupFailures,
	}
}

// DeleteMarkedBlocks uses ignoreDeletionMarkFilter to gather the blocks that are marked for deletion and deletes those
// if older than given deleteDelay.
// TODO(bwplotka): Differentiate deletion marks between backup and delete?
func (s *BlocksCleaner) DeleteMarkedBlocks(ctx context.Context, deletionMarkMap map[ulid.ULID]*metadata.DeletionMark) error {
	level.Info(s.logger).Log("msg", "started cleaning of blocks marked for deletion")

	for id, deletionMark := range deletionMarkMap {
		if time.Since(time.Unix(deletionMark.DeletionTime, 0)).Seconds() > s.deleteDelay.Seconds() {
			if err := block.Delete(ctx, s.logger, s.bkt, id); err != nil {
				s.blockCleanupFailures.Inc()
				return errors.Wrapf(err, "delete block %s", id)
			}
			s.blocksCleaned.Inc()
			level.Info(s.logger).Log("msg", "deleted block marked for deletion", "block", id)
		}
	}

	level.Info(s.logger).Log("msg", "cleaning of blocks marked for deletion done")
	return nil
}
