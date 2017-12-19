package compact

import (
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/improbable-eng/thanos/pkg/objstore"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"

	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/ulid"
)

func TestSyncer_SyncMetas(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-syncer")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	bkt, close := testutil.NewObjectStoreBucket(t)
	defer close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	randr := rand.New(rand.NewSource(0))

	// Generate 15 blocks. Initially the first 10 are on disk and the last
	// 10 are in the bucket.
	// After starting the Syncer, the first 10 should be loaded.
	// After the first synchronization the first 5 should be dropped and the
	// last 5 be loaded from the bucket.
	var ids []ulid.ULID
	var bdirs []string

	for i := 0; i < 15; i++ {
		id, err := ulid.New(uint64(i), randr)
		testutil.Ok(t, err)

		bdir := filepath.Join(dir, id.String())
		bdirs = append(bdirs, bdir)
		ids = append(ids, id)

		var meta block.Meta
		meta.Version = 1
		meta.ULID = id

		testutil.Ok(t, os.MkdirAll(bdir, 0777))
		testutil.Ok(t, block.WriteMetaFile(bdir, &meta))
	}
	for i, id := range ids[5:] {
		testutil.Ok(t, objstore.UploadDir(ctx, bkt, bdirs[i+5], id.String()))
	}
	for _, d := range bdirs[10:] {
		testutil.Ok(t, os.RemoveAll(d))
	}

	sy, err := NewSyncer(nil, dir, bkt, 0)
	testutil.Ok(t, err)

	got, err := sy.Groups()[0].IDs()
	testutil.Ok(t, err)
	testutil.Equals(t, ids[:10], got)

	testutil.Ok(t, sy.SyncMetas(ctx))

	got, err = sy.Groups()[0].IDs()
	testutil.Ok(t, err)
	testutil.Equals(t, ids[5:], got)
}

func TestSyncer_GarbageCollect(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-syncer")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	bkt, close := testutil.NewObjectStoreBucket(t)
	defer close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Generate 10 source block metas and construct higher level blocks
	// that are higher compactions of them.
	randr := rand.New(rand.NewSource(0))
	var metas []*block.Meta
	var ids []ulid.ULID

	for i := 0; i < 10; i++ {
		var m block.Meta

		id, err := ulid.New(0, randr)
		testutil.Ok(t, err)

		m.Version = 1
		m.ULID = id
		m.Compaction.Sources = []ulid.ULID{id}
		m.Compaction.Level = 1

		ids = append(ids, id)
		metas = append(metas, &m)
	}

	id1, err := ulid.New(100, randr)
	testutil.Ok(t, err)

	var m1 block.Meta
	m1.Version = 1
	m1.ULID = id1
	m1.Compaction.Level = 2
	m1.Compaction.Sources = ids[:4]

	id2, err := ulid.New(200, randr)
	testutil.Ok(t, err)

	var m2 block.Meta
	m2.Version = 1
	m2.ULID = id2
	m2.Compaction.Level = 2
	m2.Compaction.Sources = ids[4:8] // last two source IDs is not part of a level 2 block.

	id3, err := ulid.New(300, randr)
	testutil.Ok(t, err)

	var m3 block.Meta
	m3.Version = 1
	m3.ULID = id3
	m3.Compaction.Level = 3
	m3.Compaction.Sources = ids[:9] // last source ID is not part of level 3 block.

	metas = append(metas, &m1, &m2, &m3)

	// Create all blocks in the bucket.
	for _, m := range metas {
		bdir := filepath.Join(dir, m.ULID.String())
		testutil.Ok(t, os.MkdirAll(bdir, 0777))
		testutil.Ok(t, block.WriteMetaFile(bdir, m))
		testutil.Ok(t, objstore.UploadFile(ctx, bkt,
			path.Join(bdir, "meta.json"),
			path.Join(m.ULID.String(), "meta.json")))
	}

	// Do one initial synchronization with the bucket.
	sy, err := NewSyncer(nil, dir, bkt, 0)
	testutil.Ok(t, err)
	testutil.Ok(t, sy.SyncMetas(ctx))

	testutil.Ok(t, sy.GarbageCollect(ctx))

	// Only the level 3 block and the last source block should be left.
	exp := []ulid.ULID{metas[9].ULID, metas[12].ULID}

	var rem []ulid.ULID
	err = bkt.Iter(ctx, "", func(n string) error {
		rem = append(rem, ulid.MustParse(n[:len(n)-1]))
		return nil
	})
	testutil.Ok(t, err)

	sort.Slice(rem, func(i, j int) bool {
		return rem[i].Compare(rem[j]) < 0
	})
	testutil.Equals(t, exp, rem)

	// After another sync the changes should also be reflected in the local groups.
	testutil.Ok(t, sy.SyncMetas(ctx))

	rem, err = sy.Groups()[0].IDs()
	testutil.Ok(t, err)
	testutil.Equals(t, exp, rem)
}

func TestGroup_Compact(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-syncer")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	bkt, close := testutil.NewObjectStoreBucket(t)
	defer close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	b1, err := testutil.CreateBlock(dir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
	}, 100, 0, 1000)
	testutil.Ok(t, err)

	b2, err := testutil.CreateBlock(dir, []labels.Labels{
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "a", Value: "5"}},
	}, 100, 1001, 2000)
	testutil.Ok(t, err)

	b3, err := testutil.CreateBlock(dir, []labels.Labels{
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "a", Value: "5"}},
		{{Name: "a", Value: "6"}},
	}, 100, 2001, 3000)
	testutil.Ok(t, err)

	testutil.Ok(t, objstore.UploadDir(ctx, bkt, filepath.Join(dir, b1.String()), b1.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, bkt, filepath.Join(dir, b2.String()), b2.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, bkt, filepath.Join(dir, b3.String()), b3.String()))

	g, err := NewGroup(log.NewLogfmtLogger(os.Stderr), bkt, dir, nil)
	testutil.Ok(t, err)

	comp, err := tsdb.NewLeveledCompactor(nil, log.NewLogfmtLogger(os.Stderr), []int64{1000, 3000}, nil)
	testutil.Ok(t, err)

	id, err := g.Compact(ctx, comp)
	testutil.Ok(t, err)
	testutil.Assert(t, id != ulid.ULID{}, "no compaction took place")

	resDir := filepath.Join(dir, id.String())
	testutil.Ok(t, objstore.DownloadDir(ctx, bkt, id.String(), resDir))

	meta, err := block.ReadMetaFile(resDir)
	testutil.Ok(t, err)

	testutil.Equals(t, uint64(6), meta.Stats.NumSeries)
	testutil.Equals(t, uint64(3*4*100), meta.Stats.NumSamples)
	testutil.Equals(t, 2, meta.Compaction.Level)
	testutil.Equals(t, []ulid.ULID{b1, b2, b3}, meta.Compaction.Sources)
}
