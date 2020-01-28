// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package shipper

import (
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path"
	"sort"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestShipperTimestamps(t *testing.T) {
	dir, err := ioutil.TempDir("", "shipper-test")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	s := New(nil, nil, dir, nil, nil, metadata.TestSource)

	// Missing thanos meta file.
	_, _, err = s.Timestamps()
	testutil.NotOk(t, err)

	meta := &Meta{Version: MetaVersion1}
	testutil.Ok(t, WriteMetaFile(log.NewNopLogger(), dir, meta))

	// Nothing uploaded, nothing in the filesystem. We assume that
	// we are still waiting for TSDB to dump first TSDB block.
	mint, maxt, err := s.Timestamps()
	testutil.Ok(t, err)
	testutil.Equals(t, int64(0), mint)
	testutil.Equals(t, int64(math.MinInt64), maxt)

	id1 := ulid.MustNew(1, nil)
	testutil.Ok(t, os.Mkdir(path.Join(dir, id1.String()), os.ModePerm))
	testutil.Ok(t, metadata.Write(log.NewNopLogger(), path.Join(dir, id1.String()), &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id1,
			MaxTime: 2000,
			MinTime: 1000,
			Version: 1,
		},
	}))
	mint, maxt, err = s.Timestamps()
	testutil.Ok(t, err)
	testutil.Equals(t, int64(1000), mint)
	testutil.Equals(t, int64(math.MinInt64), maxt)

	id2 := ulid.MustNew(2, nil)
	testutil.Ok(t, os.Mkdir(path.Join(dir, id2.String()), os.ModePerm))
	testutil.Ok(t, metadata.Write(log.NewNopLogger(), path.Join(dir, id2.String()), &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id2,
			MaxTime: 4000,
			MinTime: 2000,
			Version: 1,
		},
	}))
	mint, maxt, err = s.Timestamps()
	testutil.Ok(t, err)
	testutil.Equals(t, int64(1000), mint)
	testutil.Equals(t, int64(math.MinInt64), maxt)

	meta = &Meta{
		Version:  MetaVersion1,
		Uploaded: []ulid.ULID{id1},
	}
	testutil.Ok(t, WriteMetaFile(log.NewNopLogger(), dir, meta))
	mint, maxt, err = s.Timestamps()
	testutil.Ok(t, err)
	testutil.Equals(t, int64(1000), mint)
	testutil.Equals(t, int64(2000), maxt)
}

func TestIterBlockMetas(t *testing.T) {
	var metas []*metadata.Meta
	dir, err := ioutil.TempDir("", "shipper-test")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	id1 := ulid.MustNew(1, nil)
	testutil.Ok(t, os.Mkdir(path.Join(dir, id1.String()), os.ModePerm))
	testutil.Ok(t, metadata.Write(log.NewNopLogger(), path.Join(dir, id1.String()), &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id1,
			MaxTime: 2000,
			MinTime: 1000,
			Version: 1,
		},
	}))

	id2 := ulid.MustNew(2, nil)
	testutil.Ok(t, os.Mkdir(path.Join(dir, id2.String()), os.ModePerm))
	testutil.Ok(t, metadata.Write(log.NewNopLogger(), path.Join(dir, id2.String()), &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id2,
			MaxTime: 5000,
			MinTime: 4000,
			Version: 1,
		},
	}))

	id3 := ulid.MustNew(3, nil)
	testutil.Ok(t, os.Mkdir(path.Join(dir, id3.String()), os.ModePerm))
	testutil.Ok(t, metadata.Write(log.NewNopLogger(), path.Join(dir, id3.String()), &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id3,
			MaxTime: 3000,
			MinTime: 2000,
			Version: 1,
		},
	}))

	shipper := New(nil, nil, dir, nil, nil, metadata.TestSource)
	if err := shipper.iterBlockMetas(func(m *metadata.Meta) error {
		metas = append(metas, m)
		return nil
	}); err != nil {
		testutil.Ok(t, err)
	}
	testutil.Equals(t, sort.SliceIsSorted(metas, func(i, j int) bool {
		return metas[i].BlockMeta.MinTime < metas[j].BlockMeta.MinTime
	}), true)
}

func BenchmarkIterBlockMetas(b *testing.B) {
	var metas []*metadata.Meta
	dir, err := ioutil.TempDir("", "shipper-test")
	testutil.Ok(b, err)
	defer func() {
		testutil.Ok(b, os.RemoveAll(dir))
	}()

	for i := 0; i < 100; i++ {
		id := ulid.MustNew(uint64(i), nil)
		testutil.Ok(b, os.Mkdir(path.Join(dir, id.String()), os.ModePerm))
		testutil.Ok(b,
			metadata.Write(
				log.NewNopLogger(),
				path.Join(dir, id.String()),
				&metadata.Meta{
					BlockMeta: tsdb.BlockMeta{
						ULID:    id,
						MaxTime: int64((i + 1) * 1000),
						MinTime: int64(i * 1000),
						Version: 1,
					},
				},
			),
		)
	}
	rand.Shuffle(len(metas), func(i, j int) {
		metas[i], metas[j] = metas[j], metas[i]
	})
	b.ResetTimer()

	shipper := New(nil, nil, dir, nil, nil, metadata.TestSource)
	if err := shipper.iterBlockMetas(func(m *metadata.Meta) error {
		metas = append(metas, m)
		return nil
	}); err != nil {
		testutil.Ok(b, err)
	}
}
