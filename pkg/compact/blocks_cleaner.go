// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
)

type deleteBlockFn func(ctx context.Context, logger log.Logger, bkt objstore.Bucket, id ulid.ULID)
type deleteBlockAndErrorFn func(ctx context.Context, logger log.Logger, bkt objstore.Bucket, id ulid.ULID) error

// BlocksCleaner is a struct that deletes blocks from bucket which are marked for deletion.
type BlocksCleaner struct {
	logger                   log.Logger
	ignoreDeletionMarkFilter *block.IgnoreDeletionMarkFilter
	bkt                      objstore.Bucket
	deleteDelay              time.Duration
	blocksCleaned            prometheus.Counter
	blockCleanupFailures     prometheus.Counter
	deleteBlockFn            deleteBlockAndErrorFn
	// cleanByIDs is a list of block IDs to be cleaned. If empty, all blocks marked for deletion will be cleaned.
	cleanByIDs map[ulid.ULID]struct{}
}

// NewBlocksCleaner creates a new BlocksCleaner.
func NewBlocksCleaner(
	logger log.Logger,
	bkt objstore.Bucket,
	ignoreDeletionMarkFilter *block.IgnoreDeletionMarkFilter,
	deleteDelay time.Duration,
	blocksCleaned, blockCleanupFailures prometheus.Counter,
	cleanByID ...ulid.ULID) *BlocksCleaner {

	var idFilter = map[ulid.ULID]struct{}{}
	if len(cleanByID) > 0 {
		for _, id := range cleanByID {
			idFilter[id] = struct{}{}
		}
	}

	deleteBlock := func(ctx context.Context, logger log.Logger, bkt objstore.Bucket, id ulid.ULID) error {
		if err := block.Delete(ctx, logger, bkt, id); err != nil {
			blockCleanupFailures.Inc()
			return errors.Wrap(err, "delete block")
		}
		blocksCleaned.Inc()
		level.Info(logger).Log("msg", "deleted block marked for deletion", "block", id)
		return nil
	}

	return &BlocksCleaner{
		logger:                   logger,
		ignoreDeletionMarkFilter: ignoreDeletionMarkFilter,
		bkt:                      bkt,
		deleteDelay:              deleteDelay,
		blocksCleaned:            blocksCleaned,
		blockCleanupFailures:     blockCleanupFailures,
		deleteBlockFn:            deleteBlock,
		cleanByIDs:               idFilter,
	}
}

func NewDryRunBlocksCleaner(logger log.Logger, bkt objstore.Bucket, ignoreDeletionMarkFilter *block.IgnoreDeletionMarkFilter, deleteDelay time.Duration, cleanByID ...ulid.ULID) *BlocksCleaner {
	cleaner := NewBlocksCleaner(logger, bkt, ignoreDeletionMarkFilter, deleteDelay, nil, nil, cleanByID...)
	level.Info(logger).Log("msg", "dry-run block cleaning enabled")
	cleaner.deleteBlockFn = func(ctx context.Context, logger log.Logger, bkt objstore.Bucket, id ulid.ULID) error {
		level.Info(logger).Log("msg", "would delete block marked for deletion", "block", id)
		return nil
	}
	return cleaner
}

// DeleteMarkedBlocks uses ignoreDeletionMarkFilter to gather the blocks that are marked for deletion and deletes those
// if older than given deleteDelay.
func (s *BlocksCleaner) DeleteMarkedBlocks(ctx context.Context) error {
	level.Info(s.logger).Log("msg", "started cleaning of blocks marked for deletion")

	filterByID := len(s.cleanByIDs) > 0
	deletionMarkMap := s.ignoreDeletionMarkFilter.DeletionMarkBlocks()
	for _, deletionMark := range deletionMarkMap {
		if filterByID {
			if _, ok := s.cleanByIDs[deletionMark.ID]; !ok {
				continue
			}
		}
		if time.Since(time.Unix(deletionMark.DeletionTime, 0)).Seconds() > s.deleteDelay.Seconds() {
			if err := s.deleteBlockFn(ctx, s.logger, s.bkt, deletionMark.ID); err != nil {
				return errors.Wrap(err, "delete block")
			}
		}
	}

	level.Info(s.logger).Log("msg", "cleaning of blocks marked for deletion done")
	return nil
}
