package shipper

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/tsdb/labels"

	"context"
	"io/ioutil"

	"os"

	"time"

	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore/inmem"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb"
)

func TestShipper_UploadBlocks(t *testing.T) {
	dir, err := ioutil.TempDir("", "shipper-test")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	var gotMinTime int64
	bucket := inmem.NewBucket()
	shipper := New(nil, nil, dir, bucket, func() labels.Labels {
		return labels.FromStrings("prometheus", "prom-1")
	}, func(mint int64) {
		gotMinTime = mint
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create 10 directories with ULIDs as names
	const numDirs = 10
	rands := rand.New(rand.NewSource(0))

	expBlocks := map[ulid.ULID]struct{}{}
	expFiles := map[string][]byte{}

	now := time.Now()
	for i := 0; i < numDirs; i++ {
		id := ulid.MustNew(uint64(i), rands)
		bdir := filepath.Join(dir, id.String())
		tmp := bdir + ".tmp"

		testutil.Ok(t, os.Mkdir(tmp, 0777))

		meta := block.Meta{
			BlockMeta: tsdb.BlockMeta{
				MinTime: timestamp.FromTime(now.Add(time.Duration(i) * time.Hour)),
				MaxTime: timestamp.FromTime(now.Add((time.Duration(i) * time.Hour) + 1)),
			},
		}
		meta.Version = 1
		meta.ULID = id

		metab, err := json.Marshal(&meta)
		testutil.Ok(t, err)

		testutil.Ok(t, ioutil.WriteFile(tmp+"/meta.json", metab, 0666))
		testutil.Ok(t, ioutil.WriteFile(tmp+"/index", []byte("indexcontents"), 0666))

		// Running shipper while a block is being written to temp dir should not trigger uploads.
		shipper.Sync(ctx)

		testutil.Ok(t, os.MkdirAll(tmp+"/chunks", 0777))
		testutil.Ok(t, ioutil.WriteFile(tmp+"/chunks/0001", []byte("chunkcontents1"), 0666))
		testutil.Ok(t, ioutil.WriteFile(tmp+"/chunks/0002", []byte("chunkcontents2"), 0666))

		testutil.Ok(t, os.Rename(tmp, bdir))

		// After rename sync should upload the block.
		shipper.Sync(ctx)

		expBlocks[id] = struct{}{}

		// The external labels must be attached to the meta file on upload.
		meta.Thanos.Labels = map[string]string{"prometheus": "prom-1"}

		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetIndent("", "\t")

		testutil.Ok(t, enc.Encode(&meta))

		expFiles[id.String()+"/meta.json"] = buf.Bytes()
		expFiles[id.String()+"/index"] = []byte("indexcontents")
		expFiles[id.String()+"/chunks/0001"] = []byte("chunkcontents1")
		expFiles[id.String()+"/chunks/0002"] = []byte("chunkcontents2")
	}

	testutil.Equals(t, timestamp.FromTime(now), gotMinTime)

	for id := range expBlocks {
		ok, _ := bucket.Exists(nil, id.String())
		testutil.Assert(t, ok, "block %s was not uploaded", id)
	}
	for fn, exp := range expFiles {
		act, ok := bucket.Files()[fn]
		testutil.Assert(t, ok, "file %s was not uploaded", fn)
		testutil.Equals(t, exp, act)
	}
}
