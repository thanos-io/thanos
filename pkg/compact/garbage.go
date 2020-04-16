package compact

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

const (
	// PartialUploadThresholdAge is a time after partial block is assumed aborted and ready to be cleaned.
	// Keep it long as it is based on block creation time not upload start time.
	PartialUploadThresholdAge = 2 * 24 * time.Hour
)

type garbageMetrics struct {
	garbageCollectedBlocks    prometheus.Counter
	garbageCollections        prometheus.Counter
	garbageCollectionFailures prometheus.Counter
	garbageCollectionDuration prometheus.Histogram

	partialUploadDeleteAttempts prometheus.Counter
}

func newGarbageMetrics(reg prometheus.Registerer) *garbageMetrics {
	var g garbageMetrics

	g.garbageCollectedBlocks = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collected_blocks_total",
		Help: "Total number of deleted blocks by compactor.",
	})
	g.garbageCollections = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collection_total",
		Help: "Total number of garbage collection operations.",
	})
	g.garbageCollectionFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collection_failures_total",
		Help: "Total number of failed garbage collection operations.",
	})
	g.garbageCollectionDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_compact_garbage_collection_duration_seconds",
		Help:    "Time it took to perform garbage collection iteration.",
		Buckets: []float64{0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120, 240, 360, 720},
	})
	g.partialUploadDeleteAttempts = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compactor_aborted_partial_uploads_deletion_attempts_total",
		Help: "Total number of started deletions of blocks that are assumed aborted and only partially uploaded.",
	})

	return &g
}

// Garbage is a struct responsible for all actions related to block (delayed) removal.
type Garbage struct {
	logger  log.Logger
	marker  *metadata.DeletionMarker
	metrics *garbageMetrics
}

// NewGarbage returns Garbage.
func NewGarbage(logger log.Logger, reg prometheus.Registerer, marker *metadata.DeletionMarker) *Garbage {
	return &Garbage{
		logger:  logger,
		marker:  marker,
		metrics: newGarbageMetrics(reg),
	}
}

// Collect schedules removal of given duplicated blocks if not already removed.
func (g *Garbage) Collect(ctx context.Context, duplicateID []ulid.ULID, alreadyMarkedForDelete map[ulid.ULID]*metadata.DeletionMark) error {
	begin := time.Now()

	toRemove := make([]ulid.ULID, 0, len(duplicateID))
	for _, id := range duplicateID {
		if _, exists := alreadyMarkedForDelete[id]; exists {
			continue
		}
		toRemove = append(toRemove, id)
	}

	for _, id := range toRemove {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		level.Info(g.logger).Log("msg", "marking outdated block for deletion before compaction; this should happen rarely, after compactor restarts", "block", id, "reason", metadata.BetweenCompactDuplicateReason)
		err := g.marker.MarkForDeletion(ctx, id, metadata.BetweenCompactDuplicateReason)
		if err != nil {
			g.metrics.garbageCollectionFailures.Inc()
			return retry(errors.Wrapf(err, "mark block %s for deletion; reason: %v", id, metadata.BetweenCompactDuplicateReason))
		}
		g.metrics.garbageCollectedBlocks.Inc()
	}
	g.metrics.garbageCollections.Inc()
	// TODO(bwplotka): Remove, no point of tracking time here.
	g.metrics.garbageCollectionDuration.Observe(time.Since(begin).Seconds())
	return nil
}

func (g *Garbage) PostCompactCollect(ctx context.Context, ids ...ulid.ULID) error {
	for _, id := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		level.Info(g.logger).Log("msg", "marking outdated block for deletion", "block", id, "reason", metadata.PostCompactDuplicateDeletion)
		err := g.marker.MarkForDeletion(ctx, id, metadata.PostCompactDuplicateDeletion)
		if err != nil {
			g.metrics.garbageCollectionFailures.Inc()
			return retry(errors.Wrapf(err, "mark block %s for deletion; reason: %v", id, metadata.PostCompactDuplicateDeletion))
		}
		g.metrics.garbageCollectedBlocks.Inc()
	}
	return nil
}

// ApplyRetention collects blocks depending on the specified retentionByResolution based on blocks MaxTime.
// A value of 0 disables the retention for its resolution.
func (g *Garbage) ApplyRetention(ctx context.Context, retentionByResolution map[ResolutionLevel]time.Duration, metas map[ulid.ULID]*metadata.Meta) error {
	retentionEnabled := false
	for _, d := range retentionByResolution {
		if d.Seconds() != 0 {
			retentionEnabled = true
		}
	}

	if !retentionEnabled {
		level.Info(g.logger).Log("msg", "retention disabled; skipping")
		return nil
	}

	level.Info(g.logger).Log("msg", "start retention apply", "retention", fmt.Sprintf("%v", retentionByResolution))
	for id, m := range metas {
		retentionDuration := retentionByResolution[ResolutionLevel(m.Thanos.Downsample.Resolution)]
		if retentionDuration.Seconds() == 0 {
			continue
		}

		maxTime := time.Unix(m.MaxTime/1000, 0)
		if time.Now().After(maxTime.Add(retentionDuration)) {

			level.Info(g.logger).Log("msg", "applying retention: marking block for deletion", "id", id, "maxTime", maxTime.String())
			if err := g.marker.MarkForDeletion(ctx, id, metadata.RetentionDeletion); err != nil {
				return errors.Wrapf(err, "mark for delete block %s; reason %s", id.String(), metadata.RetentionDeletion)
			}
		}
	}
	level.Info(g.logger).Log("msg", "retention apply done")
	return nil
}

func (g *Garbage) CleanTooLongPartialBlocks(ctx context.Context, partial map[ulid.ULID]error) {
	level.Info(g.logger).Log("msg", "started cleaning of obsolete partial uploads")

	// Delete partial blocks that are older than partialUploadThresholdAge.
	// TODO(bwplotka): This is can cause data loss if blocks are:
	// * being uploaded longer than partialUploadThresholdAge
	// * being uploaded and started after their partialUploadThresholdAge
	// can be assumed in this case. Keep partialUploadThresholdAge long for now.
	// Mitigate this by adding ModifiedTime to bkt and check that instead of ULID (block creation time).
	for id := range partial {
		if ulid.Now()-id.Time() <= uint64(PartialUploadThresholdAge/time.Millisecond) {
			// Minimum delay has not expired, ignore for now.
			continue
		}

		g.metrics.partialUploadDeleteAttempts.Inc()
		level.Info(g.logger).Log("msg", "found partially uploaded block; marking for deletion", "block", id, "reason", metadata.PartialForTooLongDeletion)
		if err := g.marker.MarkForDeletion(ctx, id, metadata.PartialForTooLongDeletion); err != nil {
			level.Warn(g.logger).Log(
				"msg", "failed to obsolete aborted partial upload; skipping",
				"block", id,
				"thresholdAge", PartialUploadThresholdAge,
				"err", err,
				"reason", metadata.PartialForTooLongDeletion,
			)
			return
		}
	}
	level.Info(g.logger).Log("msg", "cleaning of obsolete partial uploads done")
}
