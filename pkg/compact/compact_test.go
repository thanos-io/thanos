package compact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

// TODO(bplotka): Add leaktest when this is done: https://github.com/improbable-eng/thanos/issues/234
func TestSyncer_SyncMetas(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-compact-sync")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	bkt, cleanup, err := testutil.NewObjectStoreBucket(t)
	testutil.Ok(t, err)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	sy, err := NewSyncer(nil, nil, bkt, 0)
	testutil.Ok(t, err)

	// Generate 15 blocks. Initially the first 10 are synced into memory and only the last
	// 10 are in the bucket.
	// After the first synchronization the first 5 should be dropped and the
	// last 5 be loaded from the bucket.
	var ids []ulid.ULID
	var metas []*block.Meta

	for i := 0; i < 15; i++ {
		id, err := ulid.New(uint64(i), nil)
		testutil.Ok(t, err)

		var meta block.Meta
		meta.Version = 1
		meta.ULID = id

		if i < 10 {
			sy.blocks[id] = &meta
		}
		ids = append(ids, id)
		metas = append(metas, &meta)
	}
	for _, m := range metas[5:] {
		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(&m))
		testutil.Ok(t, bkt.Upload(ctx, path.Join(m.ULID.String(), "meta.json"), &buf))
	}

	groups, err := sy.Groups()
	testutil.Ok(t, err)
	testutil.Equals(t, ids[:10], groups[0].IDs())

	testutil.Ok(t, sy.SyncMetas(ctx))

	groups, err = sy.Groups()
	testutil.Ok(t, err)
	testutil.Equals(t, ids[5:], groups[0].IDs())
}

// TODO(bplotka): Add leaktest when this is done: https://github.com/improbable-eng/thanos/issues/234
func TestSyncer_GarbageCollect(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-compact-gc")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	bkt, cleanup, err := testutil.NewObjectStoreBucket(t)
	testutil.Ok(t, err)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Generate 10 source block metas and construct higher level blocks
	// that are higher compactions of them.
	var metas []*block.Meta
	var ids []ulid.ULID

	for i := 0; i < 10; i++ {
		var m block.Meta

		m.Version = 1
		m.ULID = ulid.MustNew(uint64(i), nil)
		m.Compaction.Sources = []ulid.ULID{m.ULID}
		m.Compaction.Level = 1

		ids = append(ids, m.ULID)
		metas = append(metas, &m)
	}

	var m1 block.Meta
	m1.Version = 1
	m1.ULID = ulid.MustNew(100, nil)
	m1.Compaction.Level = 2
	m1.Compaction.Sources = ids[:4]
	m1.Thanos.Downsample.Resolution = 0

	var m2 block.Meta
	m2.Version = 1
	m2.ULID = ulid.MustNew(200, nil)
	m2.Compaction.Level = 2
	m2.Compaction.Sources = ids[4:8] // last two source IDs is not part of a level 2 block.
	m2.Thanos.Downsample.Resolution = 0

	var m3 block.Meta
	m3.Version = 1
	m3.ULID = ulid.MustNew(300, nil)
	m3.Compaction.Level = 3
	m3.Compaction.Sources = ids[:9] // last source ID is not part of level 3 block.
	m3.Thanos.Downsample.Resolution = 0

	var m4 block.Meta
	m4.Version = 14
	m4.ULID = ulid.MustNew(400, nil)
	m4.Compaction.Level = 2
	m4.Compaction.Sources = ids[9:] // covers the last block but is a different resolution. Must not trigger deletion.
	m4.Thanos.Downsample.Resolution = 1000

	// Create all blocks in the bucket.
	for _, m := range append(metas, &m1, &m2, &m3, &m4) {
		fmt.Println("create", m.ULID)
		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(&m))
		testutil.Ok(t, bkt.Upload(ctx, path.Join(m.ULID.String(), "meta.json"), &buf))
	}

	// Do one initial synchronization with the bucket.
	sy, err := NewSyncer(nil, nil, bkt, 0)
	testutil.Ok(t, err)
	testutil.Ok(t, sy.SyncMetas(ctx))

	testutil.Ok(t, sy.GarbageCollect(ctx))

	var rem []ulid.ULID
	err = bkt.Iter(ctx, "", func(n string) error {
		rem = append(rem, ulid.MustParse(n[:len(n)-1]))
		return nil
	})
	testutil.Ok(t, err)

	sort.Slice(rem, func(i, j int) bool {
		return rem[i].Compare(rem[j]) < 0
	})
	// Only the level 3 block, the last source block in both resolutions should be left.
	testutil.Equals(t, []ulid.ULID{metas[9].ULID, m3.ULID, m4.ULID}, rem)

	// After another sync the changes should also be reflected in the local groups.
	testutil.Ok(t, sy.SyncMetas(ctx))

	// Only the level 3 block, the last source block in both resolutions should be left.
	groups, err := sy.Groups()
	testutil.Ok(t, err)

	testutil.Equals(t, "0@{}", groups[0].Key())
	testutil.Equals(t, []ulid.ULID{metas[9].ULID, m3.ULID}, groups[0].IDs())
	testutil.Equals(t, "1000@{}", groups[1].Key())
	testutil.Equals(t, []ulid.ULID{m4.ULID}, groups[1].IDs())
}

// TODO(bplotka): Add leaktest when this is done: https://github.com/improbable-eng/thanos/issues/234
func TestGroup_Compact(t *testing.T) {
	prepareDir, err := ioutil.TempDir("", "test-compact-prepare")
	testutil.Ok(t, err)
	defer os.RemoveAll(prepareDir)

	bkt, cleanup, err := testutil.NewObjectStoreBucket(t)
	testutil.Ok(t, err)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var metas []*block.Meta
	b1, err := testutil.CreateBlock(prepareDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
	}, 100, 0, 1000)
	testutil.Ok(t, err)

	meta, err := block.ReadMetaFile(filepath.Join(prepareDir, b1.String()))
	testutil.Ok(t, err)
	metas = append(metas, meta)

	b3, err := testutil.CreateBlock(prepareDir, []labels.Labels{
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "a", Value: "5"}},
		{{Name: "a", Value: "6"}},
	}, 100, 2001, 3000)
	testutil.Ok(t, err)

	// Mix order to make sure compact is able to deduct min time / max time.
	meta, err = block.ReadMetaFile(filepath.Join(prepareDir, b3.String()))
	testutil.Ok(t, err)
	metas = append(metas, meta)

	// Empty block. This can happen when TSDB does not have any samples for min-block-size time.
	b2, err := testutil.CreateBlock(prepareDir, []labels.Labels{}, 100, 1001, 2000)
	testutil.Ok(t, err)

	meta, err = block.ReadMetaFile(filepath.Join(prepareDir, b2.String()))
	testutil.Ok(t, err)
	metas = append(metas, meta)

	// Due to TSDB compaction delay (not compacting fresh block), we need one more block to be pushed to trigger compaction.
	freshB, err := testutil.CreateBlock(prepareDir, []labels.Labels{
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "a", Value: "5"}},
	}, 100, 3001, 4000)
	testutil.Ok(t, err)

	meta, err = block.ReadMetaFile(filepath.Join(prepareDir, freshB.String()))
	testutil.Ok(t, err)
	metas = append(metas, meta)

	// Upload and forget about tmp dir with all blocks. We want to ensure same state we will have on compactor.
	testutil.Ok(t, objstore.UploadDir(ctx, bkt, filepath.Join(prepareDir, b1.String()), b1.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, bkt, filepath.Join(prepareDir, b2.String()), b2.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, bkt, filepath.Join(prepareDir, b3.String()), b3.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, bkt, filepath.Join(prepareDir, freshB.String()), freshB.String()))

	// Create fresh, empty directory for actual test.
	dir, err := ioutil.TempDir("", "test-compact")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	metrics := newSyncerMetrics(nil)
	g, err := newGroup(
		nil,
		bkt,
		nil,
		0,
		metrics.compactions.WithLabelValues(""),
		metrics.compactionFailures.WithLabelValues(""),
		metrics.garbageCollectedBlocks,
	)
	testutil.Ok(t, err)

	comp, err := tsdb.NewLeveledCompactor(nil, log.NewLogfmtLogger(os.Stderr), []int64{1000, 3000}, nil)
	testutil.Ok(t, err)

	id, err := g.Compact(ctx, dir, comp)
	testutil.Ok(t, err)
	testutil.Assert(t, id == ulid.ULID{}, "group should be empty, but somehow compaction took place")

	// Add all metas that would be gathered by syncMetas.
	for _, m := range metas {
		testutil.Ok(t, g.Add(m))
	}

	id, err = g.Compact(ctx, dir, comp)
	testutil.Ok(t, err)
	testutil.Assert(t, id != ulid.ULID{}, "no compaction took place")

	resDir := filepath.Join(dir, id.String())
	testutil.Ok(t, objstore.DownloadDir(ctx, bkt, id.String(), resDir))

	meta, err = block.ReadMetaFile(resDir)
	testutil.Ok(t, err)

	testutil.Equals(t, int64(0), meta.MinTime)
	testutil.Equals(t, int64(3000), meta.MaxTime)
	testutil.Equals(t, uint64(6), meta.Stats.NumSeries)
	testutil.Equals(t, uint64(2*4*100), meta.Stats.NumSamples) // Only 2 times 4*100 because one block was empty.
	testutil.Equals(t, 2, meta.Compaction.Level)
	testutil.Equals(t, []ulid.ULID{b1, b3, b2}, meta.Compaction.Sources)

	// Check object storage. All blocks that were included in new compacted one should be removed.
	err = bkt.Iter(ctx, "", func(n string) error {
		id := ulid.MustParse(n[:len(n)-1])
		for _, source := range meta.Compaction.Sources {
			if id.Compare(source) == 0 {
				return errors.Errorf("Unexpectedly found %s block in bucket", source.String())
			}
		}
		return nil
	})
	testutil.Ok(t, err)
}

func TestHaltError(t *testing.T) {
	err := errors.New("test")
	testutil.Assert(t, !IsHaltError(err), "halt error")

	err = halt(errors.New("test"))
	testutil.Assert(t, IsHaltError(err), "not a halt error")

	err = errors.Wrap(halt(errors.New("test")), "something")
	testutil.Assert(t, IsHaltError(err), "not a halt error")

	err = errors.Wrap(errors.Wrap(halt(errors.New("test")), "something"), "something2")
	testutil.Assert(t, IsHaltError(err), "not a halt error")
}

func TestRetryError(t *testing.T) {
	err := errors.New("test")
	testutil.Assert(t, !IsRetryError(err), "retry error")

	err = retry(errors.New("test"))
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(retry(errors.New("test")), "something")
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(errors.Wrap(retry(errors.New("test")), "something"), "something2")
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(retry(errors.Wrap(halt(errors.New("test")), "something")), "something2")
	testutil.Assert(t, IsHaltError(err), "not a halt error. Retry should not hide halt error")
}