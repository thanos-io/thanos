package shipper

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/objtesting"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

func TestShipper_UploadBlocks_e2e(t *testing.T) {
	objtesting.ForeachStore(t, func(t testing.TB, bkt objstore.Bucket) {
		dir, err := ioutil.TempDir("", "shipper-e2e-test")
		testutil.Ok(t, err)
		defer func() {
			testutil.Ok(t, os.RemoveAll(dir))
		}()

		extLset := labels.FromStrings("prometheus", "prom-1")
		shipper := New(log.NewLogfmtLogger(os.Stderr), nil, dir, bkt, func() labels.Labels { return extLset }, block.TestSource)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create 10 new blocks that should actually be uploaded.
		var (
			expBlocks = map[ulid.ULID]struct{}{}
			expFiles  = map[string][]byte{}
			randr     = rand.New(rand.NewSource(0))
			now       = time.Now()
			ids       []ulid.ULID
		)
		for i := 0; i < 10; i++ {
			id := ulid.MustNew(uint64(i), randr)
			ids = append(ids, id)

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
			meta.Thanos.Source = block.TestSource

			metab, err := json.Marshal(&meta)
			testutil.Ok(t, err)

			testutil.Ok(t, ioutil.WriteFile(tmp+"/meta.json", metab, 0666))
			testutil.Ok(t, ioutil.WriteFile(tmp+"/index", []byte("indexcontents"), 0666))

			// Running shipper while a block is being written to temp dir should not trigger uploads.
			b, err := shipper.SyncNonCompacted(ctx)
			testutil.Ok(t, err)
			testutil.Equals(t, 0, b)

			shipMeta, err := ReadMetaFile(dir)
			testutil.Ok(t, err)
			if len(shipMeta.Uploaded) == 0 {
				shipMeta.Uploaded = []ulid.ULID{}
			}
			testutil.Equals(t, &Meta{Version: 1, Uploaded: ids[:i]}, shipMeta)

			testutil.Ok(t, os.MkdirAll(tmp+"/chunks", 0777))
			testutil.Ok(t, ioutil.WriteFile(tmp+"/chunks/0001", []byte("chunkcontents1"), 0666))
			testutil.Ok(t, ioutil.WriteFile(tmp+"/chunks/0002", []byte("chunkcontents2"), 0666))

			testutil.Ok(t, os.Rename(tmp, bdir))

			// After rename sync should upload the block.
			b, err = shipper.SyncNonCompacted(ctx)
			testutil.Ok(t, err)
			testutil.Equals(t, 1, b)

			// The external labels must be attached to the meta file on upload.
			meta.Thanos.Labels = extLset.Map()

			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			enc.SetIndent("", "\t")

			testutil.Ok(t, enc.Encode(&meta))

			// We will delete the fifth block and do not expect it to be re-uploaded later
			if i != 4 {
				expBlocks[id] = struct{}{}

				expFiles[id.String()+"/meta.json"] = buf.Bytes()
				expFiles[id.String()+"/index"] = []byte("indexcontents")
				expFiles[id.String()+"/chunks/0001"] = []byte("chunkcontents1")
				expFiles[id.String()+"/chunks/0002"] = []byte("chunkcontents2")
			} else {
				testutil.Ok(t, block.Delete(ctx, bkt, ids[4]))
			}
			// The shipper meta file should show all blocks as uploaded.
			shipMeta, err = ReadMetaFile(dir)
			testutil.Ok(t, err)
			testutil.Equals(t, &Meta{Version: 1, Uploaded: ids[:i+1]}, shipMeta)

			// Verify timestamps were updated correctly.
			minTotal, maxSync, err := shipper.Timestamps()
			testutil.Ok(t, err)
			testutil.Equals(t, timestamp.FromTime(now), minTotal)
			testutil.Equals(t, meta.MaxTime, maxSync)
		}

		for id := range expBlocks {
			ok, _ := bkt.Exists(nil, path.Join(id.String(), block.MetaFilename))
			testutil.Assert(t, ok, "block %s was not uploaded", id)
		}
		for fn, exp := range expFiles {
			rc, err := bkt.Get(ctx, fn)
			testutil.Ok(t, err)
			act, err := ioutil.ReadAll(rc)
			testutil.Ok(t, err)
			testutil.Ok(t, rc.Close())
			testutil.Equals(t, string(exp), string(act))
		}
		// Verify the fifth block is still deleted by the end.
		ok, err := bkt.Exists(ctx, ids[4].String()+"/meta.json")
		testutil.Ok(t, err)
		testutil.Assert(t, ok == false, "fifth block was reuploaded")
	})
}
