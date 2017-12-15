package testutil

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"

	"google.golang.org/api/iterator"
)

// NewObjectStoreBucket creates a new GCS bucket with a random name and a cleanup function
// that deletes it.
//
// TODO(fabxc): define object storage interface and have this method return
// mocks or actual remote buckets depending on env vars.
func NewObjectStoreBucket(t testing.TB) (*gcs.Bucket, func()) {
	project, ok := os.LookupEnv("GCP_PROJECT")
	// TODO(fabxc): make it run against a mock store if no actual bucket is configured.
	if !ok {
		t.Skip("test can only run against a defined GCP_PROJECT")
	}
	ctx, cancel := context.WithCancel(context.Background())

	gcsClient, err := storage.NewClient(ctx)
	Ok(t, err)

	src := rand.NewSource(time.Now().UnixNano())
	Ok(t, err)
	name := fmt.Sprintf("test_%s_%x", strings.ToLower(t.Name()), src.Int63())

	bkt := gcsClient.Bucket(name)
	Ok(t, bkt.Create(ctx, project, nil))

	return gcs.NewBucket(bkt), func() {
		deleteAllBucket(t, ctx, bkt)
		cancel()
		gcsClient.Close()
	}
}

func deleteAllBucket(t testing.TB, ctx context.Context, bkt *storage.BucketHandle) {
	var wg sync.WaitGroup

	objs := bkt.Objects(ctx, nil)
	for {
		oi, err := objs.Next()
		if err == iterator.Done {
			break
		}
		Ok(t, err)

		wg.Add(1)
		go func() {
			if err := bkt.Object(oi.Name).Delete(ctx); err != nil {
				t.Logf("deleting object %s failed: %s", oi.Name, err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	Ok(t, bkt.Delete(ctx))
}
