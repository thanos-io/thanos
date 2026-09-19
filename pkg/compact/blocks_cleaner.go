// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/errutil"
)

// BlocksCleaner is a struct that deletes blocks from bucket which are marked for deletion.
type BlocksCleaner struct {
	logger                   log.Logger
	ignoreDeletionMarkFilter *block.IgnoreDeletionMarkFilter
	bkt                      objstore.Bucket
	deleteDelay              time.Duration
	blocksCleaned            prometheus.Counter
	blockCleanupFailures     prometheus.Counter
}

// NewBlocksCleaner creates a new BlocksCleaner.
func NewBlocksCleaner(logger log.Logger, bkt objstore.Bucket, ignoreDeletionMarkFilter *block.IgnoreDeletionMarkFilter, deleteDelay time.Duration, blocksCleaned, blockCleanupFailures prometheus.Counter) *BlocksCleaner {
	return &BlocksCleaner{
		logger:                   logger,
		ignoreDeletionMarkFilter: ignoreDeletionMarkFilter,
		bkt:                      bkt,
		deleteDelay:              deleteDelay,
		blocksCleaned:            blocksCleaned,
		blockCleanupFailures:     blockCleanupFailures,
	}
}

// DeleteMarkedBlocks uses ignoreDeletionMarkFilter to gather the blocks that are marked for deletion and deletes those
// if older than given deleteDelay.
func (s *BlocksCleaner) DeleteMarkedBlocks(ctx context.Context) (map[ulid.ULID]struct{}, error) {
	const conc = 32

	level.Info(s.logger).Log("msg", "started cleaning of blocks marked for deletion")

	var (
		merr             errutil.SyncMultiError
		deletedBlocksMtx sync.Mutex
		deletedBlocks    = make(map[ulid.ULID]struct{}, 0)
		deletionMarkMap  = s.ignoreDeletionMarkFilter.DeletionMarkBlocks()
		wg               sync.WaitGroup
		dm               = make(chan *metadata.DeletionMark, conc)
	)

	for range conc {
		wg.Go(func() {
			for deletionMark := range dm {
				if ctx.Err() != nil {
					return
				}
				if time.Since(time.Unix(deletionMark.DeletionTime, 0)).Seconds() > s.deleteDelay.Seconds() {
					if err := block.Delete(ctx, s.logger, s.bkt, deletionMark.ID); err != nil {
						s.blockCleanupFailures.Inc()
						merr.Add(errors.Wrap(err, "delete block"))
						continue
					}

					s.blocksCleaned.Inc()
					level.Info(s.logger).Log("msg", "deleted block marked for deletion", "block", deletionMark.ID)

					deletedBlocksMtx.Lock()
					deletedBlocks[deletionMark.ID] = struct{}{}
					deletedBlocksMtx.Unlock()
				}
			}
		})
	}

	for _, deletionMark := range deletionMarkMap {
		dm <- deletionMark
	}
	close(dm)
	wg.Wait()

	if ctx.Err() != nil {
		return deletedBlocks, ctx.Err()
	}

	level.Info(s.logger).Log("msg", "cleaning of blocks marked for deletion done")
	return deletedBlocks, merr.Err()
}
