// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/testutil"
)

type tsdbPlannerAdapter struct {
	dir  string
	comp tsdb.Compactor
}

func (p *tsdbPlannerAdapter) Plan(_ context.Context, metasByMinTime []*metadata.Meta) ([]*metadata.Meta, error) {
	// TSDB planning works based on the meta.json files in the given dir. Mock it up.
	for _, meta := range metasByMinTime {
		bdir := filepath.Join(p.dir, meta.ULID.String())
		if err := os.MkdirAll(bdir, 0777); err != nil {
			return nil, errors.Wrap(err, "create planning block dir")
		}
		if err := meta.WriteToDir(log.NewNopLogger(), bdir); err != nil {
			return nil, errors.Wrap(err, "write planning meta file")
		}
	}
	plan, err := p.comp.Plan(p.dir)
	if err != nil {
		return nil, err
	}

	var res []*metadata.Meta
	for _, pdir := range plan {
		meta, err := metadata.ReadFromDir(pdir)
		if err != nil {
			return nil, errors.Wrapf(err, "read meta from %s", pdir)
		}
		res = append(res, meta)
	}
	return res, nil
}

// Adapted from https://github.com/prometheus/prometheus/blob/6c56a1faaaad07317ff585bda75b99bdba0517ad/tsdb/compact_test.go#L167
func TestPlanners_Plan_Compatibility(t *testing.T) {
	ranges := []int64{
		20,
		60,
		180,
		540,
		1620,
	}

	// This mimics our default ExponentialBlockRanges with min block size equals to 20.
	tsdbComp, err := tsdb.NewLeveledCompactor(context.Background(), nil, nil, ranges, nil)
	testutil.Ok(t, err)
	tsdbPlanner := &tsdbPlannerAdapter{comp: tsdbComp}
	tsdbBasedPlanner := NewTSDBBasedPlanner(log.NewNopLogger(), ranges)

	for _, c := range []struct {
		name     string
		metas    []*metadata.Meta
		expected []*metadata.Meta
	}{
		{
			name: "Outside range",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
			},
		},
		{
			name: "We should wait for four blocks of size 20 to appear before compacting.",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
			},
		},
		{
			name: `We should wait for a next block of size 20 to appear before compacting
		the existing ones. We have three, but we ignore the fresh one from WAl`,
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
			},
		},
		{
			name: "Block to fill the entire parent range appeared – should be compacted",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
			},
		},
		{
			name: "There are blocks to fill the entire 2nd parent range.",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(6, nil), MinTime: 0, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(7, nil), MinTime: 60, MaxTime: 120}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(8, nil), MinTime: 120, MaxTime: 180}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(9, nil), MinTime: 180, MaxTime: 200}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(10, nil), MinTime: 200, MaxTime: 220}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(6, nil), MinTime: 0, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(7, nil), MinTime: 60, MaxTime: 120}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(8, nil), MinTime: 120, MaxTime: 180}},
			},
		},
		{
			name: `Block for the next parent range appeared with gap with size 20. Nothing will happen in the first one
		anymore but we ignore fresh one still, so no compaction`,
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
			},
		},
		{
			name: `Block for the next parent range appeared, and we have a gap with size 20 between second and third block.
		We will not get this missed gap anymore and we should compact just these two.`,
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(5, nil), MinTime: 80, MaxTime: 100}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
			},
		},
		{
			name: "We have 20, 20, 20, 60, 60 range blocks. '5' is marked as fresh one",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 120}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(5, nil), MinTime: 120, MaxTime: 180}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
			},
		},
		{
			name: "There are blocks to fill the entire 2nd parent range, but there is a gap",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(6, nil), MinTime: 0, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(8, nil), MinTime: 120, MaxTime: 180}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(9, nil), MinTime: 180, MaxTime: 200}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(10, nil), MinTime: 200, MaxTime: 220}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(6, nil), MinTime: 0, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(8, nil), MinTime: 120, MaxTime: 180}},
			},
		},
		{
			name: "We have 20, 60, 20, 60, 240 range blocks. We can compact 20 + 60 + 60",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 120}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(5, nil), MinTime: 960, MaxTime: 980}}, // Fresh one.
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(6, nil), MinTime: 120, MaxTime: 180}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(7, nil), MinTime: 720, MaxTime: 960}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 120}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(6, nil), MinTime: 120, MaxTime: 180}},
			},
		},
		{
			name: "Do not select large blocks that have many tombstones when there is no fresh block",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 540, Stats: tsdb.BlockStats{
					NumSeries:     10,
					NumTombstones: 3,
				}}},
			},
		},
		{
			name: "Select large blocks that have many tombstones when fresh appears",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 540, Stats: tsdb.BlockStats{
					NumSeries:     10,
					NumTombstones: 3,
				}}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 540, MaxTime: 560}},
			},
			expected: []*metadata.Meta{{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 540, Stats: tsdb.BlockStats{
				NumSeries:     10,
				NumTombstones: 3,
			}}}},
		},
		{
			name: "For small blocks, do not compact tombstones, even when fresh appears.",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 60, Stats: tsdb.BlockStats{
					NumSeries:     10,
					NumTombstones: 3,
				}}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 60, MaxTime: 80}},
			},
		},
		{
			name: `Regression test: we were stuck in a compact loop where we always recompacted
		the same block when tombstones and series counts were zero`,
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 540, Stats: tsdb.BlockStats{
					NumSeries:     0,
					NumTombstones: 0,
				}}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 540, MaxTime: 560}},
			},
		},
		{
			name: `Regression test: we were wrongly assuming that new block is fresh from WAL when its ULID is newest.
		We need to actually look on max time instead.

		With previous, wrong approach "8" block was ignored, so we were wrongly compacting 5 and 7 and introducing
		block overlaps`,
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(5, nil), MinTime: 0, MaxTime: 360}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(6, nil), MinTime: 540, MaxTime: 560}}, // Fresh one.
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(7, nil), MinTime: 360, MaxTime: 420}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(8, nil), MinTime: 420, MaxTime: 540}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(7, nil), MinTime: 360, MaxTime: 420}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(8, nil), MinTime: 420, MaxTime: 540}},
			},
		},
		// |--------------|
		//               |----------------|
		//                                |--------------|
		{
			name: "Overlapping blocks 1",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 19, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 19, MaxTime: 40}},
			},
		},
		// |--------------|
		//                |--------------|
		//                        |--------------|
		{
			name: "Overlapping blocks 2",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 30, MaxTime: 50}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 30, MaxTime: 50}},
			},
		},
		// |--------------|
		//         |---------------------|
		//                       |--------------|
		{
			name: "Overlapping blocks 3",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 10, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 30, MaxTime: 50}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 10, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 30, MaxTime: 50}},
			},
		},
		// |--------------|
		//               |--------------------------------|
		//                |--------------|
		//                               |--------------|
		{
			name: "Overlapping blocks 4",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(5, nil), MinTime: 0, MaxTime: 360}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(6, nil), MinTime: 340, MaxTime: 560}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(7, nil), MinTime: 360, MaxTime: 420}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(8, nil), MinTime: 420, MaxTime: 540}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(5, nil), MinTime: 0, MaxTime: 360}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(6, nil), MinTime: 340, MaxTime: 560}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(7, nil), MinTime: 360, MaxTime: 420}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(8, nil), MinTime: 420, MaxTime: 540}},
			},
		},
		// |--------------|
		//               |--------------|
		//                                            |--------------|
		//                                                          |--------------|
		{
			name: "Overlapping blocks 5",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 10}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 9, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 30, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 39, MaxTime: 50}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 10}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 9, MaxTime: 20}},
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, e := range c.expected {
				// Add here to avoid boilerplate.
				e.Thanos.Labels = make(map[string]string)
			}
			for _, e := range c.metas {
				// Add here to avoid boilerplate.
				e.Thanos.Labels = make(map[string]string)
			}

			// For compatibility.
			t.Run("tsdbPlannerAdapter", func(t *testing.T) {
				dir, err := ioutil.TempDir("", "test-compact")
				testutil.Ok(t, err)
				defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

				metasByMinTime := make([]*metadata.Meta, len(c.metas))
				for i := range metasByMinTime {
					metasByMinTime[i] = c.metas[i]
				}
				sort.Slice(metasByMinTime, func(i, j int) bool {
					return metasByMinTime[i].MinTime < metasByMinTime[j].MinTime
				})

				tsdbPlanner.dir = dir
				plan, err := tsdbPlanner.Plan(context.Background(), metasByMinTime)
				testutil.Ok(t, err)
				testutil.Equals(t, c.expected, plan)
			})
			t.Run("tsdbBasedPlanner", func(t *testing.T) {
				metasByMinTime := make([]*metadata.Meta, len(c.metas))
				for i := range metasByMinTime {
					metasByMinTime[i] = c.metas[i]
				}
				sort.Slice(metasByMinTime, func(i, j int) bool {
					return metasByMinTime[i].MinTime < metasByMinTime[j].MinTime
				})

				plan, err := tsdbBasedPlanner.Plan(context.Background(), metasByMinTime)
				testutil.Ok(t, err)
				testutil.Equals(t, c.expected, plan)
			})
		})
	}
}

// Adapted form: https://github.com/prometheus/prometheus/blob/6c56a1faaaad07317ff585bda75b99bdba0517ad/tsdb/compact_test.go#L377
func TestRangeWithFailedCompactionWontGetSelected(t *testing.T) {
	ranges := []int64{
		20,
		60,
		180,
		540,
		1620,
	}

	// This mimics our default ExponentialBlockRanges with min block size equals to 20.
	tsdbComp, err := tsdb.NewLeveledCompactor(context.Background(), nil, nil, ranges, nil)
	testutil.Ok(t, err)
	tsdbPlanner := &tsdbPlannerAdapter{comp: tsdbComp}
	tsdbBasedPlanner := NewTSDBBasedPlanner(log.NewNopLogger(), ranges)

	for _, c := range []struct {
		metas []*metadata.Meta
	}{
		{
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
			},
		},
		{
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(5, nil), MinTime: 80, MaxTime: 100}},
			},
		},
		{
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 120}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(5, nil), MinTime: 120, MaxTime: 180}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(6, nil), MinTime: 180, MaxTime: 200}},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			c.metas[1].Compaction.Failed = true
			// For compatibility.
			t.Run("tsdbPlannerAdapter", func(t *testing.T) {
				dir, err := ioutil.TempDir("", "test-compact")
				testutil.Ok(t, err)
				defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

				tsdbPlanner.dir = dir
				plan, err := tsdbPlanner.Plan(context.Background(), c.metas)
				testutil.Ok(t, err)
				testutil.Equals(t, []*metadata.Meta(nil), plan)
			})
			t.Run("tsdbBasedPlanner", func(t *testing.T) {
				plan, err := tsdbBasedPlanner.Plan(context.Background(), c.metas)
				testutil.Ok(t, err)
				testutil.Equals(t, []*metadata.Meta(nil), plan)
			})
		})
	}
}

func TestTSDBBasedPlanner_PlanWithNoCompactMarks(t *testing.T) {
	ranges := []int64{
		20,
		60,
		180,
		540,
		1620,
	}

	g := &GatherNoCompactionMarkFilter{}
	tsdbBasedPlanner := NewPlanner(log.NewNopLogger(), ranges, g)

	for _, c := range []struct {
		name           string
		metas          []*metadata.Meta
		noCompactMarks map[ulid.ULID]*metadata.NoCompactMark

		expected []*metadata.Meta
	}{
		{
			name: "Outside range and excluded",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
			},
			noCompactMarks: map[ulid.ULID]*metadata.NoCompactMark{
				ulid.MustNew(1, nil): {},
			},
		},
		{
			name: "Blocks to fill the entire parent, but with first one excluded.",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
			},
			noCompactMarks: map[ulid.ULID]*metadata.NoCompactMark{
				ulid.MustNew(1, nil): {},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
			},
		},
		{
			name: "Blocks to fill the entire parent, but with second one excluded.",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
			},
			noCompactMarks: map[ulid.ULID]*metadata.NoCompactMark{
				ulid.MustNew(2, nil): {},
			},
		},
		{
			name: "Blocks to fill the entire parent, but with last one excluded.",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
			},
			noCompactMarks: map[ulid.ULID]*metadata.NoCompactMark{
				ulid.MustNew(4, nil): {},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
			},
		},
		{
			name: "Blocks to fill the entire parent, but with last one fist excluded.",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
			},
			noCompactMarks: map[ulid.ULID]*metadata.NoCompactMark{
				ulid.MustNew(1, nil): {},
				ulid.MustNew(4, nil): {},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
			},
		},
		{
			name: "Blocks to fill the entire parent, but with all of them excluded.",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
			},
			noCompactMarks: map[ulid.ULID]*metadata.NoCompactMark{
				ulid.MustNew(1, nil): {},
				ulid.MustNew(2, nil): {},
				ulid.MustNew(3, nil): {},
				ulid.MustNew(4, nil): {},
			},
		},
		{
			name: `Block for the next parent range appeared, and we have a gap with size 20 between second and third block.
		Second block is excluded.`,
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(5, nil), MinTime: 80, MaxTime: 100}},
			},
			noCompactMarks: map[ulid.ULID]*metadata.NoCompactMark{
				ulid.MustNew(2, nil): {},
			},
		},
		{
			name: "We have 20, 60, 20, 60, 240 range blocks. We could compact 20 + 60 + 60, but sixth 6th is excluded",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 120}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(5, nil), MinTime: 960, MaxTime: 980}}, // Fresh one.
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(6, nil), MinTime: 120, MaxTime: 180}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(7, nil), MinTime: 720, MaxTime: 960}},
			},
			noCompactMarks: map[ulid.ULID]*metadata.NoCompactMark{
				ulid.MustNew(6, nil): {},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 120}},
			},
		},
		{
			name: "We have 20, 60, 20, 60, 240 range blocks. We could compact 20 + 60 + 60, but 4th is excluded",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 120}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(5, nil), MinTime: 960, MaxTime: 980}}, // Fresh one.
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(6, nil), MinTime: 120, MaxTime: 180}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(7, nil), MinTime: 720, MaxTime: 960}},
			},
			noCompactMarks: map[ulid.ULID]*metadata.NoCompactMark{
				ulid.MustNew(4, nil): {},
			},
		},
		{
			name: "Do not select large blocks that have many tombstones when fresh appears but are excluded",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 540, Stats: tsdb.BlockStats{
					NumSeries:     10,
					NumTombstones: 3,
				}}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 540, MaxTime: 560}},
			},
			noCompactMarks: map[ulid.ULID]*metadata.NoCompactMark{
				ulid.MustNew(1, nil): {},
			},
		},
		// |--------------|
		//               |----------------|
		//                                |--------------|
		{
			name: "Overlapping blocks 1, but one is excluded",
			metas: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 19, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
			},
			noCompactMarks: map[ulid.ULID]*metadata.NoCompactMark{
				ulid.MustNew(1, nil): {},
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			metasByMinTime := make([]*metadata.Meta, len(c.metas))
			for i := range metasByMinTime {
				metasByMinTime[i] = c.metas[i]
			}
			sort.Slice(metasByMinTime, func(i, j int) bool {
				return metasByMinTime[i].MinTime < metasByMinTime[j].MinTime
			})
			g.noCompactMarkedMap = c.noCompactMarks
			plan, err := tsdbBasedPlanner.Plan(context.Background(), metasByMinTime)
			testutil.Ok(t, err)
			testutil.Equals(t, c.expected, plan)
		})
	}
}

func TestLargeTotalIndexSizeFilter_Plan(t *testing.T) {
	ranges := []int64{
		20,
		60,
		180,
		540,
		1620,
	}

	bkt := objstore.NewInMemBucket()
	g := &GatherNoCompactionMarkFilter{}

	marked := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	planner := WithLargeTotalIndexSizeFilter(NewPlanner(log.NewNopLogger(), ranges, g), bkt, 100, marked)
	var lastMarkValue float64
	for _, c := range []struct {
		name  string
		metas []*metadata.Meta

		expected      []*metadata.Meta
		expectedMarks float64
	}{
		{
			name: "Outside range and excluded",
			metas: []*metadata.Meta{
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 100}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
			},
			expectedMarks: 0,
		},
		{
			name: "Blocks to fill the entire parent, but with first one too large.",
			metas: []*metadata.Meta{
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 41}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
			},
			expectedMarks: 1,
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
			},
		},
		{
			name: "Blocks to fill the entire parent, but with second one too large.",
			metas: []*metadata.Meta{
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 41}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 20}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
			},
			expectedMarks: 1,
		},
		{
			name: "Blocks to fill the entire parent, but with last size exceeded (should not matter and not even marked).",
			metas: []*metadata.Meta{
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 10}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 10}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 10}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 90}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
			},
		},
		{
			name: "Blocks to fill the entire parent, but with pre-last one and first too large.",
			metas: []*metadata.Meta{
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 90}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 50}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 90}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 50, MaxTime: 60}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 90}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(5, nil), MinTime: 60, MaxTime: 80}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 50}},
			},
			expectedMarks: 2,
		},
		{
			name: `Block for the next parent range appeared, and we have a gap with size 20 between second and third block.
		Second block is excluded.`,
			metas: []*metadata.Meta{
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 90}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 80}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(5, nil), MinTime: 80, MaxTime: 100}},
			},
			expectedMarks: 1,
		},
		{
			name: "We have 20, 60, 20, 60, 240 range blocks. We could compact 20 + 60 + 60, but sixth 6th is excluded",
			metas: []*metadata.Meta{
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 120}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(5, nil), MinTime: 960, MaxTime: 980}}, // Fresh one.
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 90}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(6, nil), MinTime: 120, MaxTime: 180}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(7, nil), MinTime: 720, MaxTime: 960}},
			},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(4, nil), MinTime: 60, MaxTime: 120}},
			},
			expectedMarks: 1,
		},
		// |--------------|
		//               |----------------|
		//                                |--------------|
		{
			name: "Overlapping blocks 1, but total is too large",
			metas: []*metadata.Meta{
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 90}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 20}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(2, nil), MinTime: 19, MaxTime: 40}},
				{Thanos: metadata.Thanos{Files: []metadata.File{{RelPath: block.IndexFilename, SizeBytes: 30}}},
					BlockMeta: tsdb.BlockMeta{Version: 1, ULID: ulid.MustNew(3, nil), MinTime: 40, MaxTime: 60}},
			},
			expectedMarks: 1,
		},
	} {
		if !t.Run(c.name, func(t *testing.T) {
			t.Run("from meta", func(t *testing.T) {
				obj := bkt.Objects()
				for o := range obj {
					delete(obj, o)
				}

				metasByMinTime := make([]*metadata.Meta, len(c.metas))
				for i := range metasByMinTime {
					orig := c.metas[i]
					m := &metadata.Meta{}
					*m = *orig
					metasByMinTime[i] = m
				}
				sort.Slice(metasByMinTime, func(i, j int) bool {
					return metasByMinTime[i].MinTime < metasByMinTime[j].MinTime
				})

				plan, err := planner.Plan(context.Background(), metasByMinTime)
				testutil.Ok(t, err)

				for _, m := range plan {
					// For less boilerplate.
					m.Thanos = metadata.Thanos{}
				}
				testutil.Equals(t, c.expected, plan)
				testutil.Equals(t, c.expectedMarks, promtest.ToFloat64(marked)-lastMarkValue)
				lastMarkValue = promtest.ToFloat64(marked)
			})
			t.Run("from bkt", func(t *testing.T) {
				obj := bkt.Objects()
				for o := range obj {
					delete(obj, o)
				}

				metasByMinTime := make([]*metadata.Meta, len(c.metas))
				for i := range metasByMinTime {
					orig := c.metas[i]
					m := &metadata.Meta{}
					*m = *orig
					metasByMinTime[i] = m
				}
				sort.Slice(metasByMinTime, func(i, j int) bool {
					return metasByMinTime[i].MinTime < metasByMinTime[j].MinTime
				})

				for _, m := range metasByMinTime {
					testutil.Ok(t, bkt.Upload(context.Background(), filepath.Join(m.ULID.String(), block.IndexFilename), bytes.NewReader(make([]byte, m.Thanos.Files[0].SizeBytes))))
					m.Thanos = metadata.Thanos{}
				}

				plan, err := planner.Plan(context.Background(), metasByMinTime)
				testutil.Ok(t, err)
				testutil.Equals(t, c.expected, plan)
				testutil.Equals(t, c.expectedMarks, promtest.ToFloat64(marked)-lastMarkValue)

				lastMarkValue = promtest.ToFloat64(marked)
			})

		}) {
			return
		}
	}
}
