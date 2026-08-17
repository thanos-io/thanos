// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"context"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// GatherMarkedBlocks reads one marker type for each provided block meta and
// returns markers keyed by block ID. Missing and partially uploaded markers are
// ignored, matching the behavior expected by metadata filters.
func GatherMarkedBlocks[M metadata.Marker](
	ctx context.Context,
	logger log.Logger,
	bkt objstore.InstrumentedBucketReader,
	metas map[ulid.ULID]*metadata.Meta,
	concurrency int,
	newMarker func() M,
	partialMarkerFilename string,
	synced GaugeVec,
	syncedLabel string,
) (map[ulid.ULID]M, error) {
	if concurrency < 1 {
		concurrency = 1
	}

	markedBlocks := make(map[ulid.ULID]M)
	var markedBlocksMtx sync.Mutex

	// Make a copy of block IDs to check, in order to avoid concurrency issues
	// between the scheduler and workers.
	blockIDs := make([]ulid.ULID, 0, len(metas))
	for id := range metas {
		blockIDs = append(blockIDs, id)
	}

	var (
		eg errgroup.Group
		ch = make(chan ulid.ULID, concurrency)
	)

	for i := 0; i < concurrency; i++ {
		eg.Go(func() error {
			var lastErr error
			for id := range ch {
				marker := newMarker()
				if err := metadata.ReadMarker(ctx, logger, bkt, id.String(), marker); err != nil {
					if errors.Cause(err) == metadata.ErrorMarkerNotFound {
						continue
					}
					if errors.Cause(err) == metadata.ErrorUnmarshalMarker {
						level.Warn(logger).Log("msg", "found partial "+partialMarkerFilename+"; if we will see it happening often for the same block, consider manually deleting "+partialMarkerFilename+" from the object storage", "block", id, "err", err)
						continue
					}
					// Remember the last error and continue draining the channel.
					lastErr = err
					continue
				}

				markedBlocksMtx.Lock()
				markedBlocks[id] = marker
				markedBlocksMtx.Unlock()
				if synced != nil {
					synced.WithLabelValues(syncedLabel).Inc()
				}
			}

			return lastErr
		})
	}

	// Workers scheduled, distribute blocks.
	eg.Go(func() error {
		defer close(ch)

		for _, id := range blockIDs {
			select {
			case ch <- id:
				// Nothing to do.
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return markedBlocks, nil
}
