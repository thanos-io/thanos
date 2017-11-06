package shipper

import (
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/oklog/ulid"

	"context"
	"io/ioutil"

	"os"

	"github.com/improbable-eng/promlts/pkg/testutil"
)

type inMemStorage struct {
	t    *testing.T
	dirs map[string]struct{}
}

func newInMemStorage(t *testing.T) *inMemStorage {
	return &inMemStorage{
		t:    t,
		dirs: make(map[string]struct{}),
	}
}

func (r *inMemStorage) Exists(_ context.Context, id string) (bool, error) {
	_, exists := r.dirs[id]
	return exists, nil
}

func (r *inMemStorage) Upload(_ context.Context, dir string) error {
	r.t.Logf("upload called: %s", dir)
	// Double check if shipper checks Exists method properly.
	_, exists := r.dirs[dir]
	testutil.Assert(r.t, !exists, "target should not exists")

	r.dirs[dir] = struct{}{}
	return nil
}

func TestShipper_UploadULIDDirs(t *testing.T) {
	dir, err := ioutil.TempDir("", "shipper-test-snapshots")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	storage := newInMemStorage(t)
	shipper := New(nil, nil, dir, storage, IsULIDDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start shipping blocks.
	go func() { testutil.Ok(t, shipper.Run(ctx, 50*time.Millisecond)) }()

	// Create 10 directories with ULIDs as names
	const numDirs = 10
	rands := rand.New(rand.NewSource(0))

	var expected []string

	for i := 0; i < numDirs; i++ {
		id := ulid.MustNew(uint64(i), rands)
		bdir := filepath.Join(dir, id.String())
		tmp := bdir + ".tmp"

		testutil.Ok(t, os.Mkdir(tmp, 0777))
		testutil.Ok(t, os.Rename(tmp, bdir))

		expected = append(expected, bdir)
	}

	time.Sleep(500 * time.Millisecond)

	for _, exp := range expected {
		ok, err := storage.Exists(ctx, exp)
		testutil.Ok(t, err)
		testutil.Assert(t, ok, "ULID directory %s expected in store", exp)
	}
	testutil.Equals(t, numDirs, len(storage.dirs))
}
