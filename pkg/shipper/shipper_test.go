package shipper

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/tsdb/labels"

	"context"
	"io/ioutil"

	"os"

	"github.com/improbable-eng/thanos/pkg/testutil"
)

type inMemStorage struct {
	t      *testing.T
	blocks map[ulid.ULID]struct{}
	files  map[string]string
}

func newInMemStorage(t *testing.T) *inMemStorage {
	return &inMemStorage{
		t:      t,
		blocks: map[ulid.ULID]struct{}{},
		files:  map[string]string{},
	}
}

func (r *inMemStorage) Exists(_ context.Context, id ulid.ULID) (bool, error) {
	_, exists := r.blocks[id]
	return exists, nil
}

func (r *inMemStorage) Upload(_ context.Context, id ulid.ULID, dir string) error {
	r.t.Logf("upload called: %s %s", id, dir)
	// Double check if shipper checks Exists method properly.
	_, exists := r.blocks[id]
	testutil.Assert(r.t, !exists, "target should not exists")

	r.blocks[id] = struct{}{}

	return filepath.Walk(dir, func(name string, fi os.FileInfo, err error) error {
		if !fi.IsDir() {
			b, err := ioutil.ReadFile(name)
			if err != nil {
				return err
			}
			name = filepath.Join(id.String(), strings.TrimPrefix(name, dir))
			r.t.Logf("upload file %s, %s", name, string(b))
			r.files[name] = string(b)
		}
		return nil
	})
}

func TestShipper_UploadBlocks(t *testing.T) {
	dir, err := ioutil.TempDir("", "shipper-test")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	storage := newInMemStorage(t)
	shipper := New(nil, nil, dir, storage, func() labels.Labels {
		return labels.FromStrings("prometheus", "prom-1")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create 10 directories with ULIDs as names
	const numDirs = 10
	rands := rand.New(rand.NewSource(0))

	expBlocks := map[ulid.ULID]struct{}{}
	expFiles := map[string]string{}

	for i := 0; i < numDirs; i++ {
		id := ulid.MustNew(uint64(i), rands)
		bdir := filepath.Join(dir, id.String())
		tmp := bdir + ".tmp"

		testutil.Ok(t, os.Mkdir(tmp, 0777))

		meta := blockMeta{}
		meta.Version = 1
		meta.ULID = id

		metab, err := json.Marshal(&meta)
		testutil.Ok(t, err)

		testutil.Ok(t, ioutil.WriteFile(tmp+"/meta.json", metab, 0666))
		testutil.Ok(t, ioutil.WriteFile(tmp+"/index", []byte("indexcontents"), 0666))

		// Running shipper while a block is being written to temp dir should not trigger uploads.
		shipper.Sync(ctx)

		testutil.Ok(t, ioutil.WriteFile(tmp+"/chunks", []byte("chunkcontents"), 0666))
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

		expFiles[id.String()+"/meta.json"] = buf.String()
		expFiles[id.String()+"/index"] = "indexcontents"
		expFiles[id.String()+"/chunks"] = "chunkcontents"
	}

	for id := range expBlocks {
		_, ok := storage.blocks[id]
		testutil.Assert(t, ok, "block %s was not uploaded", id)
	}
	for fn, exp := range expFiles {
		act, ok := storage.files[fn]
		testutil.Assert(t, ok, "file %s was not uploaded", fn)
		testutil.Equals(t, exp, act)
	}
}
