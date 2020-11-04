// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type tsdbBasedPlanner struct {
	logger log.Logger

	ranges []int64
}

var _ Planner = &tsdbBasedPlanner{}

// NewTSDBBasedPlanner is planner with the same functionality as Prometheus' TSDB plus special handling of excluded blocks.
// TODO(bwplotka): Consider upstreaming this to Prometheus.
// It's the same functionality just without accessing filesystem, and special handling of excluded blocks.
func NewTSDBBasedPlanner(logger log.Logger, ranges []int64) *tsdbBasedPlanner {
	return &tsdbBasedPlanner{logger: logger, ranges: ranges}
}

func (p *tsdbBasedPlanner) Plan(_ context.Context, metasByMinTime []*metadata.Meta) ([]*metadata.Meta, error) {
	res := selectOverlappingMetas(metasByMinTime)
	if len(res) > 0 {
		return res, nil
	}

	// No overlapping blocks, do compaction the usual way.
	// We do not include a recently created block with max(minTime), so the block which was just created from WAL.
	// This gives users a window of a full block size to piece-wise backup new data without having to care about data overlap.
	metasByMinTime = metasByMinTime[:len(metasByMinTime)-1]
	res = append(res, selectMetas(p.ranges, metasByMinTime)...)
	if len(res) > 0 {
		return res, nil
	}

	// Compact any blocks with big enough time range that have >5% tombstones.
	for i := len(metasByMinTime) - 1; i >= 0; i-- {
		meta := metasByMinTime[i]
		if meta.MaxTime-meta.MinTime < p.ranges[len(p.ranges)/2] {
			break
		}
		if float64(meta.Stats.NumTombstones)/float64(meta.Stats.NumSeries+1) > 0.05 {
			return []*metadata.Meta{metasByMinTime[i]}, nil
		}
	}

	return nil, nil
}

// selectMetas returns the dir metas that should be compacted into a single new block.
// If only a single block range is configured, the result is always nil.
func selectMetas(ranges []int64, metasByMinTime []*metadata.Meta) []*metadata.Meta {
	if len(ranges) < 2 || len(metasByMinTime) < 1 {
		return nil
	}

	highTime := metasByMinTime[len(metasByMinTime)-1].MinTime

	for _, iv := range ranges[1:] {
		parts := splitByRange(metasByMinTime, iv)
		if len(parts) == 0 {
			continue
		}

	Outer:
		for _, p := range parts {
			// Do not select the range if it has a block whose compaction failed.
			for _, m := range p {
				if m.Compaction.Failed {
					continue Outer
				}
			}

			mint := p[0].MinTime
			maxt := p[len(p)-1].MaxTime
			// Pick the range of blocks if it spans the full range (potentially with gaps)
			// or is before the most recent block.
			// This ensures we don't compact blocks prematurely when another one of the same
			// size still fits in the range.
			if (maxt-mint == iv || maxt <= highTime) && len(p) > 1 {
				return p
			}
		}
	}

	return nil
}

// selectOverlappingMetas returns all dirs with overlapping time ranges.
// It expects sorted input by mint and returns the overlapping dirs in the same order as received.
func selectOverlappingMetas(metasByMinTime []*metadata.Meta) []*metadata.Meta {
	if len(metasByMinTime) < 2 {
		return nil
	}
	var overlappingMetas []*metadata.Meta
	globalMaxt := metasByMinTime[0].MaxTime
	for i, m := range metasByMinTime[1:] {
		if m.MinTime < globalMaxt {
			if len(overlappingMetas) == 0 {
				// When it is the first overlap, need to add the last one as well.
				overlappingMetas = append(overlappingMetas, metasByMinTime[i])
			}
			overlappingMetas = append(overlappingMetas, m)
		} else if len(overlappingMetas) > 0 {
			break
		}

		if m.MaxTime > globalMaxt {
			globalMaxt = m.MaxTime
		}
	}
	return overlappingMetas
}

// splitByRange splits the directories by the time range. The range sequence starts at 0.
//
// For example, if we have blocks [0-10, 10-20, 50-60, 90-100] and the split range tr is 30
// it returns [0-10, 10-20], [50-60], [90-100].
func splitByRange(metasByMinTime []*metadata.Meta, tr int64) [][]*metadata.Meta {
	var splitDirs [][]*metadata.Meta

	for i := 0; i < len(metasByMinTime); {
		var (
			group []*metadata.Meta
			t0    int64
			m     = metasByMinTime[i]
		)
		// Compute start of aligned time range of size tr closest to the current block's start.
		if m.MinTime >= 0 {
			t0 = tr * (m.MinTime / tr)
		} else {
			t0 = tr * ((m.MinTime - tr + 1) / tr)
		}
		// Skip blocks that don't fall into the range. This can happen via mis-alignment or
		// by being the multiple of the intended range.
		if m.MaxTime > t0+tr {
			i++
			continue
		}

		// Add all dirs to the current group that are within [t0, t0+tr].
		for ; i < len(metasByMinTime); i++ {
			// Either the block falls into the next range or doesn't fit at all (checked above).
			if metasByMinTime[i].MaxTime > t0+tr {
				break
			}
			group = append(group, metasByMinTime[i])
		}

		if len(group) > 0 {
			splitDirs = append(splitDirs, group)
		}
	}

	return splitDirs
}
