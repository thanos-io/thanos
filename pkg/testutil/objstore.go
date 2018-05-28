package testutil

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"sync"

	"cloud.google.com/go/storage"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"google.golang.org/api/iterator"
)

// NewObjectStoreBucket creates a new GCS bucket with a random name and a cleanup function
// that deletes it.
//
// TODO(fabxc): define object storage interface and have this method return
// mocks or actual remote buckets depending on env vars.
func NewObjectStoreBucket(t testing.TB) (*gcs.Bucket, func(), error) {
	project, ok := os.LookupEnv("GCP_PROJECT")
	// TODO(fabxc): make it run against a mock store if no actual bucket is configured.
	if !ok {
		t.Skip("test can only run against a defined GCP_PROJECT")
	}

	ctx, cancel := context.WithCancel(context.Background())
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	src := rand.NewSource(time.Now().UnixNano())
	name := fmt.Sprintf("test_%s_%x", strings.ToLower(t.Name()), src.Int63())

	bkt := gcsClient.Bucket(name)
	err = bkt.Create(ctx, project, nil)
	if err != nil {
		cancel()
		gcsClient.Close()
		return nil, nil, err
	}

	return gcs.NewBucket(name, bkt, nil), func() {
		deleteAllBucket(t, ctx, bkt)
		cancel()
		gcsClient.Close()
	}, nil
}

func deleteAllBucket(t testing.TB, ctx context.Context, bkt *storage.BucketHandle) {
	var wg sync.WaitGroup

	objs := bkt.Objects(ctx, nil)
	for {
		oi, err := objs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Logf("iterating over bucket objects failed: %s", err)
			wg.Wait()
			return
		}

		wg.Add(1)
		go func() {
			if err := bkt.Object(oi.Name).Delete(ctx); err != nil {
				t.Logf("deleting object %s failed: %s", oi.Name, err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	err := bkt.Delete(ctx)
	if err != nil {
		t.Logf("deleting bucket failed: %s", err)
	}
}
