// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package verifier

import (
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestDuplicatedBlocks(t *testing.T) {
	b1 := tsdb.BlockMeta{
		MaxTime: 0,
		MinTime: 10,
		Compaction: tsdb.BlockMetaCompaction{
			Sources: []ulid.ULID{
				ulid.MustNew(0, nil),
				ulid.MustNew(1, nil),
				ulid.MustNew(2, nil),
			},
			Level: 2,
		},
		Stats: tsdb.BlockStats{
			NumChunks:     941384,
			NumSamples:    112567234,
			NumSeries:     60915,
			NumTombstones: 0,
		},
	}

	dupB1 := b1

	b2 := b1
	b2.MinTime = 1

	b3 := b1
	b3.Stats.NumTombstones = 1

	b4 := b1
	b4.Compaction.Sources = b4.Compaction.Sources[1:]

	b5 := b1

	testutil.Equals(t, [][]tsdb.BlockMeta{
		{b1, dupB1, b5},
	}, duplicatedBlocks([]tsdb.BlockMeta{b1, dupB1, b2, b3, b4, b5}))
}
