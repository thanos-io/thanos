package verifier

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/ulid"
)

func TestFindOverlappedBlocks_Found(t *testing.T) {
	var metas []block.Meta
	var ids []ulid.ULID

	// Create ten blocks that does not overlap.
	for i := 0; i < 10; i++ {
		var m block.Meta

		m.Version = 1
		m.ULID = ulid.MustNew(uint64(i), nil)
		m.MinTime = int64(i * 10)
		m.MaxTime = int64((i + 1) * 10)

		ids = append(ids, m.ULID)
		metas = append(metas, m)
	}

	overlappedBlocks := findOverlappedBlocks(metas)
	testutil.Assert(t, len(overlappedBlocks) == 0, "we found unexpected overlaps")

	// Add overlaps.
	o1 := block.Meta{}
	o1.Version = 1
	o1.ULID = ulid.MustNew(uint64(11), nil)
	// Small one.
	o1.MinTime = int64(15)
	o1.MaxTime = int64(17)

	o2 := block.Meta{}
	o2.Version = 1
	o2.ULID = ulid.MustNew(uint64(12), nil)
	o2.MinTime = int64(21)
	o2.MaxTime = int64(31)

	o3 := block.Meta{}
	o3.Version = 1
	o3.ULID = ulid.MustNew(uint64(13), nil)
	o3.MinTime = int64(33)
	o3.MaxTime = int64(39)
	o3b := block.Meta{}
	o3b.Version = 1
	o3b.ULID = ulid.MustNew(uint64(14), nil)
	o3b.MinTime = int64(34)
	o3b.MaxTime = int64(36)

	overlappedBlocks = findOverlappedBlocks(append(metas, o1, o2, o3, o3b))

	expectedOverlaps := [][]block.Meta{
		{metas[1], o1},
		{metas[2], o2},
		{metas[3], o2},
		{metas[3], o3, o3b},
	}

	testutil.Equals(t, expectedOverlaps, overlappedBlocks)
}
