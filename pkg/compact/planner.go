// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type tsdbBasedPlanner struct {
	logger log.Logger

	ranges []int64

	noCompBlocksFunc func() map[ulid.ULID]*metadata.NoCompactMark
}

var _ Planner = &tsdbBasedPlanner{}

// NewTSDBBasedPlanner is planner with the same functionality as Prometheus' TSDB.
// TODO(bwplotka): Consider upstreaming this to Prometheus.
// It's the same functionality just without accessing filesystem.
func NewTSDBBasedPlanner(logger log.Logger, ranges []int64) *tsdbBasedPlanner {
	return &tsdbBasedPlanner{
		logger: logger,
		ranges: ranges,
		noCompBlocksFunc: func() map[ulid.ULID]*metadata.NoCompactMark {
			return make(map[ulid.ULID]*metadata.NoCompactMark)
		},
	}
}

// NewPlanner is a default Thanos planner with the same functionality as Prometheus' TSDB plus special handling of excluded blocks.
// It's the same functionality just without accessing filesystem, and special handling of excluded blocks.
func NewPlanner(logger log.Logger, ranges []int64, noCompBlocks *GatherNoCompactionMarkFilter) *tsdbBasedPlanner {
	return &tsdbBasedPlanner{logger: logger, ranges: ranges, noCompBlocksFunc: noCompBlocks.NoCompactMarkedBlocks}
}

// TODO(bwplotka): Consider smarter algorithm, this prefers smaller iterative compactions vs big single one: https://github.com/thanos-io/thanos/issues/3405
func (p *tsdbBasedPlanner) Plan(_ context.Context, metasByMinTime []*metadata.Meta) ([]*metadata.Meta, error) {
	noCompactMarked := p.noCompBlocksFunc()
	notExcludedMetasByMinTime := make([]*metadata.Meta, 0, len(metasByMinTime))
	for _, meta := range metasByMinTime {
		if _, excluded := noCompactMarked[meta.ULID]; excluded {
			continue
		}
		notExcludedMetasByMinTime = append(notExcludedMetasByMinTime, meta)
	}

	res := selectOverlappingMetas(notExcludedMetasByMinTime)
	if len(res) > 0 {
		return res, nil
	}
	// No overlapping blocks, do compaction the usual way.

	// We do not include a recently producted block with max(minTime), so the block which was just uploaded to bucket.
	// This gives users a window of a full block size maintenance if needed.
	if _, excluded := noCompactMarked[metasByMinTime[len(metasByMinTime)-1].ULID]; !excluded {
		notExcludedMetasByMinTime = notExcludedMetasByMinTime[:len(notExcludedMetasByMinTime)-1]
	}
	metasByMinTime = metasByMinTime[:len(metasByMinTime)-1]
	res = append(res, selectMetas(p.ranges, noCompactMarked, metasByMinTime)...)
	if len(res) > 0 {
		return res, nil
	}

	// Compact any blocks with big enough time range that have >5% tombstones.
	for i := len(notExcludedMetasByMinTime) - 1; i >= 0; i-- {
		meta := notExcludedMetasByMinTime[i]
		if meta.MaxTime-meta.MinTime < p.ranges[len(p.ranges)/2] {
			break
		}
		if float64(meta.Stats.NumTombstones)/float64(meta.Stats.NumSeries+1) > 0.05 {
			return []*metadata.Meta{notExcludedMetasByMinTime[i]}, nil
		}
	}

	return nil, nil
}

// selectMetas returns the dir metas that should be compacted into a single new block.
// If only a single block range is configured, the result is always nil.
// Copied and adjusted from https://github.com/prometheus/prometheus/blob/3d8826a3d42566684283a9b7f7e812e412c24407/tsdb/compact.go#L229.
func selectMetas(ranges []int64, noCompactMarked map[ulid.ULID]*metadata.NoCompactMark, metasByMinTime []*metadata.Meta) []*metadata.Meta {
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

			if len(p) < 2 {
				continue
			}

			mint := p[0].MinTime
			maxt := p[len(p)-1].MaxTime

			// Pick the range of blocks if it spans the full range (potentially with gaps) or is before the most recent block.
			// This ensures we don't compact blocks prematurely when another one of the same size still would fits in the range
			// after upload.
			if maxt-mint != iv && maxt > highTime {
				continue
			}

			// Check if any of resulted blocks are excluded. Exclude them in a way that does not introduce gaps to the system
			// as well as preserve the ranges that would be used if they were not excluded.
			// This is meant as short-term workaround to create ability for marking some blocks to not be touched for compaction.
			lastExcluded := 0
			for i, id := range p {
				if _, excluded := noCompactMarked[id.ULID]; !excluded {
					continue
				}
				if len(p[lastExcluded:i]) > 1 {
					return p[lastExcluded:i]
				}
				lastExcluded = i + 1
			}
			if len(p[lastExcluded:]) > 1 {
				return p[lastExcluded:]
			}
		}
	}

	return nil
}

// selectOverlappingMetas returns all dirs with overlapping time ranges.
// It expects sorted input by mint and returns the overlapping dirs in the same order as received.
// Copied and adjusted from https://github.com/prometheus/prometheus/blob/3d8826a3d42566684283a9b7f7e812e412c24407/tsdb/compact.go#L268.
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
// Copied and adjusted from: https://github.com/prometheus/prometheus/blob/3d8826a3d42566684283a9b7f7e812e412c24407/tsdb/compact.go#L294.
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

		// Add all metas to the current group that are within [t0, t0+tr].
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
