package compact

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/improbable-eng/thanos/pkg/block"

	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/ulid"
)

func TestSyncer_SyncMetas(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-syncer")
	testutil.Ok(t, err)
	// defer os.RemoveAll(dir)
	fmt.Println(dir)

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
