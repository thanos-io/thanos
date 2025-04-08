// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
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
func (s *BlocksCleaner) DeleteMarkedBlocks(ctx context.Context) error {
	level.Info(s.logger).Log("msg", "started cleaning of blocks marked for deletion")

	deletionMarkMap := s.ignoreDeletionMarkFilter.DeletionMarkBlocks()
	wg := &sync.WaitGroup{}
	sem := make(chan struct{}, runtime.NumCPU())
	for _, deletionMark := range deletionMarkMap {
		if time.Since(time.Unix(deletionMark.DeletionTime, 0)).Seconds() > s.deleteDelay.Seconds() {
			sem <- struct{}{} // acquire BEFORE spawning goroutine
			wg.Add(1)
			go func(wg *sync.WaitGroup, sem chan struct{}, id ulid.ULID) {
				defer wg.Done()
				defer func() { <-sem }() // release
				if err := block.Delete(ctx, s.logger, s.bkt, id); err != nil {
					s.blockCleanupFailures.Inc()
					level.Error(s.logger).Log("msg", "failed to delete block marked for deletion", "block", deletionMark.ID, "err", err)
					return
				}
				s.blocksCleaned.Inc()
			}(wg, sem, deletionMark.ID)
			level.Info(s.logger).Log("msg", "deleted block marked for deletion", "block", deletionMark.ID)
		}
	}
	wg.Wait()
	level.Info(s.logger).Log("msg", "cleaning of blocks marked for deletion done")
	return nil
}
