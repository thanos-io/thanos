package store

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/tsdb/fileutil"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
	"github.com/improbable-eng/thanos/pkg/testutil"
)

func randBucketName(t *testing.T) string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	testutil.Ok(t, err)
	return fmt.Sprintf("test-%x", b)
}

func TestGCSStore_downloadBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	project, ok := os.LookupEnv("GCP_PROJECT")
	// TODO(fabxc): make it run against a mock store if no actual bucket is configured.
	if !ok {
		return
	}

	gcsClient, err := storage.NewClient(ctx)
	testutil.Ok(t, err)
	defer gcsClient.Close()

	bkt := gcsClient.Bucket(randBucketName(t))
	testutil.Ok(t, bkt.Create(ctx, project, nil))

	defer deleteAllBucket(t, ctx, bkt)

	expBlocks := []ulid.ULID{}
	expIndexes := map[string][]byte{}

	randr := rand.New(rand.NewSource(0))

	// Generate 5 blocks with random data and add it to the bucket.
	for i := int64(0); i < 3; i++ {
		id := ulid.MustNew(uint64(i), randr)
		expBlocks = append(expBlocks, id)

		for _, s := range []string{
			path.Join(id.String(), "index"),
			path.Join(id.String(), "chunks/0001"),
			path.Join(id.String(), "chunks/0002"),
			path.Join(id.String(), "meta.json"),
		} {
			w := bkt.Object(s).NewWriter(ctx)
			b := make([]byte, 64*1024)

			_, err := randr.Read(b)
			testutil.Ok(t, err)
			_, err = w.Write(b)
			testutil.Ok(t, err)
			testutil.Ok(t, w.Close())

			if strings.HasSuffix(s, "index") {
				expIndexes[s] = b
			}
		}
	}

	dir, err := ioutil.TempDir("", "test-syncBlocks")
	testutil.Ok(t, err)
	fmt.Println(dir)

	s, err := NewGCSStore(nil, bkt, dir)
	testutil.Ok(t, err)
	defer s.Close()

	testutil.Ok(t, s.downloadBlocks(ctx))

	fns, err := fileutil.ReadDir(dir)
	testutil.Ok(t, err)

	testutil.Assert(t, len(expBlocks) == len(fns), "invalid block count")
	for _, id := range expBlocks {
		_, err := os.Stat(filepath.Join(dir, id.String()))
		testutil.Assert(t, err == nil, "block %s not synced", id)
	}
	// For each block we expect a downloaded index file.
	for fn, data := range expIndexes {
		b, err := ioutil.ReadFile(filepath.Join(dir, fn))
		testutil.Ok(t, err)
		testutil.Equals(t, data, b)
	}
}

func deleteAllBucket(t testing.TB, ctx context.Context, bkt *storage.BucketHandle) {
	objs := bkt.Objects(ctx, nil)
	for {
		oi, err := objs.Next()
		if err == iterator.Done {
			break
		}
		testutil.Ok(t, err)
		testutil.Ok(t, bkt.Object(oi.Name).Delete(ctx))
	}
	testutil.Ok(t, bkt.Delete(ctx))
}
