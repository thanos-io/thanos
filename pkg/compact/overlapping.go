// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

const (
	overlappingReason = "blocks-overlapping"

	symbolTableSizeExceedsError = "symbol table size exceeds"
	symbolTableSizeLimit        = 1024 * 1024
)

type OverlappingCompactionLifecycleCallback struct {
	overlappingBlocks prometheus.Counter
	noCompaction      prometheus.Counter
	noDownsampling    prometheus.Counter
}

func NewOverlappingCompactionLifecycleCallback(reg *prometheus.Registry, logger log.Logger, enabled bool) CompactionLifecycleCallback {
	if enabled {
		level.Info(logger).Log("msg", "enabled overlapping blocks compaction lifecycle callback")
		return OverlappingCompactionLifecycleCallback{
			overlappingBlocks: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "thanos_compact_group_overlapping_blocks_total",
				Help: "Total number of blocks that are overlapping.",
			}),
			noCompaction: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "thanos_compact_group_overlapping_blocks_no_compaction_total",
				Help: "Total number of blocks that are overlapping and mark no compaction.",
			}),
			noDownsampling: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "thanos_compact_group_overlapping_blocks_no_downsampling_total",
				Help: "Total number of blocks that are overlapping and mark no downsampling.",
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
	var reason error
	for curr, currB := range toCompact {
		prevB := toCompact[prev]
		if curr == 0 || currB.Thanos.Source == metadata.ReceiveSource || prevB.MaxTime <= currB.MinTime {
			// no overlapping with previous blocks, skip it
			prev = curr
			continue
		} else if currB.MinTime < prevB.MinTime {
			// halt when the assumption is broken, the input toCompact isn't sorted by minTime, need manual investigation
			return halt(errors.Errorf("later blocks has smaller minTime than previous block: %s -- %s", prevB.String(), currB.String()))
		}
		// prev min <= curr min < prev max
		o.overlappingBlocks.Inc()
		if prevB.MaxTime < currB.MaxTime && prevB.MinTime != currB.MinTime {
			o.noCompaction.Inc()
			reason = fmt.Errorf("found partially overlapping block: %s -- %s", prevB.String(), currB.String())
			if err := block.MarkForNoCompact(ctx, logger, cg.bkt, prevB.ULID, overlappingReason,
				reason.Error(), o.noCompaction); err != nil {
				return retry(err)
			}
			if err := block.MarkForNoCompact(ctx, logger, cg.bkt, currB.ULID, overlappingReason,
				reason.Error(), o.noCompaction); err != nil {
				return retry(err)
			}
			return retry(reason)
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
		var outer, inner *metadata.Meta
		if prevB.MaxTime >= currB.MaxTime {
			inner = currB
			outer = prevB
		} else if prevB.MaxTime < currB.MaxTime {
			inner = prevB
			outer = currB
			prev = curr
		}
		if outer.Thanos.Source == metadata.ReceiveSource {
			level.Warn(logger).Log("msg", "bypass if larger blocks are from receive",
				"outer", outer.String(), "inner", inner.String())
			continue
		}
		reason = retry(fmt.Errorf("found full overlapping block: %s > %s", outer.String(), inner.String()))
		if err := block.MarkForNoCompact(ctx, logger, cg.bkt, inner.ULID, overlappingReason, reason.Error(),
			o.noCompaction); err != nil {
			return err
		}
		if err := block.MarkForNoDownsample(ctx, logger, cg.bkt, inner.ULID, overlappingReason, reason.Error(),
			o.noDownsampling); err != nil {
			return err
		}
	}
	return reason
}

func (o OverlappingCompactionLifecycleCallback) PostCompactionCallback(_ context.Context, _ log.Logger, _ *Group, _ ulid.ULID) error {
	return nil
}

func (o OverlappingCompactionLifecycleCallback) GetBlockPopulator(_ context.Context, _ log.Logger, _ *Group) (tsdb.BlockPopulator, error) {
	return tsdb.DefaultBlockPopulator{}, nil
}

func (o OverlappingCompactionLifecycleCallback) HandleError(ctx context.Context, logger log.Logger, g *Group, toCompact []*metadata.Meta, compactErr error) int {
	handledErrs := 0
	if compactErr == nil {
		return handledErrs
	}
	level.Error(logger).Log("msg", "failed to compact blocks", "err", compactErr)
	if strings.Contains(compactErr.Error(), symbolTableSizeExceedsError) {
		for _, m := range toCompact {
			if m.Stats.NumSeries < symbolTableSizeLimit {
				level.Warn(logger).Log("msg", "bypass small blocks", "block", m.String(), "series", m.Stats.NumSeries)
				continue
			}
			handledErrs++
			if err := block.MarkForNoCompact(ctx, logger, g.bkt, m.ULID, symbolTableSizeExceedsError,
				fmt.Sprintf("failed to compact blocks: %s", m.ULID.String()), o.noCompaction); err != nil {
				level.Error(logger).Log("msg", "failed to mark block for no compact", "block", m.String(), "err", err)
			}
		}
	}
	return handledErrs
}
