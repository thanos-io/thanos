// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type OverlappingCompactionLifecycleCallback struct {
	overlappingBlocks prometheus.Counter
}

func NewOverlappingCompactionLifecycleCallback(reg *prometheus.Registry, enabled bool) CompactionLifecycleCallback {
	if enabled {
		return OverlappingCompactionLifecycleCallback{
			overlappingBlocks: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "thanos_compact_group_overlapping_blocks_total",
				Help: "Total number of blocks that are overlapping and to be deleted.",
			}),
		}
	}
	return DefaultCompactionLifecycleCallback{}
}

// PreCompactionCallback given the assumption that toCompact is sorted by MinTime in ascending order from Planner
// (not guaranteed on MaxTime order), we will detect overlapping blocks and delete them while retaining all others.
func (o OverlappingCompactionLifecycleCallback) PreCompactionCallback(ctx context.Context, logger log.Logger, cg *Group, toCompact []*metadata.Meta) error {
	if len(toCompact) == 0 {
		return nil
	}
	prev := 0
	for curr, currB := range toCompact {
		prevB := toCompact[prev]
		if curr == 0 || currB.Thanos.Source == metadata.ReceiveSource || prevB.MaxTime <= currB.MinTime {
			// no overlapping with previous blocks, skip it
			prev = curr
			continue
		} else if currB.MinTime < prevB.MinTime {
			// halt when the assumption is broken, the input toCompact isn't sorted by minTime, need manual investigation
			return halt(errors.Errorf("later blocks has smaller minTime than previous block: %s -- %s", prevB.String(), currB.String()))
		} else if prevB.MaxTime < currB.MaxTime && prevB.MinTime != currB.MinTime {
			err := errors.Errorf("found partially overlapping block: %s -- %s", prevB.String(), currB.String())
			if cg.enableVerticalCompaction {
				level.Error(logger).Log("msg", "best effort to vertical compact", "err", err)
				prev = curr
				continue
			} else {
				return halt(err)
			}
		} else if prevB.MinTime == currB.MinTime && prevB.MaxTime == currB.MaxTime {
			if prevB.Stats.NumSeries != currB.Stats.NumSeries || prevB.Stats.NumSamples != currB.Stats.NumSamples {
				level.Warn(logger).Log("msg", "found same time range but different stats, keep both blocks",
					"prev", prevB.String(), "prevSeries", prevB.Stats.NumSeries, "prevSamples", prevB.Stats.NumSamples,
					"curr", currB.String(), "currSeries", currB.Stats.NumSeries, "currSamples", currB.Stats.NumSamples,
				)
				prev = curr
				continue
			}
		}
		// prev min <= curr min < prev max
		toDelete := -1
		if prevB.MaxTime >= currB.MaxTime {
			toDelete = curr
			level.Warn(logger).Log("msg", "found overlapping block in plan, keep previous block",
				"toKeep", prevB.String(), "toDelete", currB.String())
		} else if prevB.MaxTime < currB.MaxTime {
			toDelete = prev
			prev = curr
			level.Warn(logger).Log("msg", "found overlapping block in plan, keep current block",
				"toKeep", currB.String(), "toDelete", prevB.String())
		}
		o.overlappingBlocks.Inc()
		if err := DeleteBlockNow(ctx, logger, cg.bkt, toCompact[toDelete]); err != nil {
			return retry(err)
		}
		toCompact[toDelete] = nil
	}
	return nil
}

func (o OverlappingCompactionLifecycleCallback) PostCompactionCallback(_ context.Context, _ log.Logger, _ *Group, _ ulid.ULID) error {
	return nil
}

func (o OverlappingCompactionLifecycleCallback) GetBlockPopulator(_ context.Context, _ log.Logger, _ *Group) (tsdb.BlockPopulator, error) {
	return tsdb.DefaultBlockPopulator{}, nil
}

func FilterRemovedBlocks(blocks []*metadata.Meta) (res []*metadata.Meta) {
	for _, b := range blocks {
		if b != nil {
			res = append(res, b)
		}
	}
	return res
}

func DeleteBlockNow(ctx context.Context, logger log.Logger, bkt objstore.Bucket, m *metadata.Meta) error {
	level.Warn(logger).Log("msg", "delete polluted block immediately", "block", m.String(),
		"level", m.Compaction.Level, "parents", fmt.Sprintf("%v", m.Compaction.Parents),
		"resolution", m.Thanos.Downsample.Resolution, "source", m.Thanos.Source, "labels", m.Thanos.GetLabels(),
		"series", m.Stats.NumSeries, "samples", m.Stats.NumSamples, "chunks", m.Stats.NumChunks)
	if err := block.Delete(ctx, logger, bkt, m.ULID); err != nil {
		return errors.Wrapf(err, "delete overlapping block %s", m.String())
	}
	return nil
}
