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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
)

const (
	// PartialUploadThresholdAge is a time after partial block is assumed aborted and ready to be cleaned.
	// Keep it long as it is based on block creation time not upload start time.
	PartialUploadThresholdAge = 2 * 24 * time.Hour
)

// getOldestModifiedTime returns the oldest modified time of a block in the bucket.
// If it is not possible to get the last modified timestamp then it falls back to the time
// encoded in the block's ULID.
func getOldestModifiedTime(ctx context.Context, blockID ulid.ULID, bkt objstore.Bucket) (time.Time, error) {
	var lastModifiedTime time.Time

	err := bkt.IterWithAttributes(ctx, blockID.String(), func(attrs objstore.IterObjectAttributes) error {
		lm, ok := attrs.LastModified()
		if !ok {
			return nil
		}
		if lm.After(lastModifiedTime) {
			lastModifiedTime = lm
		}
		return nil
	}, objstore.WithUpdatedAt(), objstore.WithRecursiveIter())

	if err != nil {
		return timestamp.Time(int64(blockID.Time())), err
	}

	if lastModifiedTime.IsZero() {
		return timestamp.Time(int64(blockID.Time())), fmt.Errorf("no last modified time found for block %s, using block creation time instead", blockID.String())
	}

	return lastModifiedTime, nil
}

func BestEffortCleanAbortedPartialUploads(
	ctx context.Context,
	logger log.Logger,
	partial map[ulid.ULID]error,
	bkt objstore.Bucket,
	deleteAttempts prometheus.Counter,
	blockCleanups prometheus.Counter,
	blockCleanupFailures prometheus.Counter,
) {
	level.Info(logger).Log("msg", "started cleaning of aborted partial uploads")

	for id := range partial {
		lastModifiedTime, err := getOldestModifiedTime(ctx, id, bkt)
		if err != nil {
			level.Warn(logger).Log("msg", "failed to get last modified time for block; falling back to block creation time", "block", id, "err", err)
		}
		if time.Since(lastModifiedTime) <= PartialUploadThresholdAge {
			continue
		}

		deleteAttempts.Inc()
		level.Info(logger).Log("msg", "found partially uploaded block; deleting", "block", id)
		if err := block.Delete(ctx, logger, bkt, id); err != nil {
			blockCleanupFailures.Inc()
			level.Warn(logger).Log("msg", "failed to delete aborted partial upload; will retry in next iteration", "block", id, "thresholdAge", PartialUploadThresholdAge, "err", err)
			continue
		}
		blockCleanups.Inc()
		level.Info(logger).Log("msg", "deleted aborted partial upload", "block", id, "thresholdAge", PartialUploadThresholdAge)
	}
	level.Info(logger).Log("msg", "cleaning of aborted partial uploads done")
}
