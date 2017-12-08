package compact

import (
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

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
		testutil.Ok(t, uploadBlock(ctx, bkt, id, bdirs[i+5]))
	}
	for _, d := range bdirs[10:] {
		testutil.Ok(t, os.RemoveAll(d))
	}

	sy, err := NewSyncer(nil, dir, bkt)
	testutil.Ok(t, err)

	got, err := sy.Groups()[0].IDs()
	testutil.Ok(t, err)
	testutil.Equals(t, ids[:10], got)

	testutil.Ok(t, sy.SyncMetas(ctx))

	got, err = sy.Groups()[0].IDs()
	testutil.Ok(t, err)
	testutil.Equals(t, ids[5:], got)
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

	testutil.Ok(t, uploadBlock(ctx, bkt, b1, filepath.Join(dir, b1.String())))
	testutil.Ok(t, uploadBlock(ctx, bkt, b2, filepath.Join(dir, b2.String())))
	testutil.Ok(t, uploadBlock(ctx, bkt, b3, filepath.Join(dir, b3.String())))

	g, err := NewGroup(log.NewLogfmtLogger(os.Stderr), bkt, dir, nil)
	testutil.Ok(t, err)

	comp, err := tsdb.NewLeveledCompactor(nil, log.NewLogfmtLogger(os.Stderr), []int64{1000, 3000}, nil)
	testutil.Ok(t, err)

	id, err := g.Compact(ctx, comp)
	testutil.Ok(t, err)
	testutil.Assert(t, id != ulid.ULID{}, "no compaction took place")

	resDir := filepath.Join(dir, id.String())
	testutil.Ok(t, downloadBlock(ctx, bkt, id.String(), resDir))

	meta, err := block.ReadMetaFile(resDir)
	testutil.Ok(t, err)

	testutil.Equals(t, uint64(6), meta.Stats.NumSeries)
	testutil.Equals(t, uint64(3*4*100), meta.Stats.NumSamples)
	testutil.Equals(t, 2, meta.Compaction.Level)
	testutil.Equals(t, []ulid.ULID{b1, b2, b3}, meta.Compaction.Sources)
}
