package objstore

import (
	"context"
	"strings"
	"sync"
	"testing"
)

// EmptyBucket deletes all objects from bucket. This operation is required to properly delete bucket as a whole.
// It is used for testing only.
// TODO(bplotka): Add retries.
func EmptyBucket(t testing.TB, ctx context.Context, bkt Bucket) {
	var wg sync.WaitGroup

	queue := []string{""}
	for len(queue) > 0 {
		elem := queue[0]
		queue = queue[1:]

		err := bkt.Iter(ctx, elem, func(p string) error {
			if strings.HasSuffix(p, DirDelim) {
				queue = append(queue, p)
				return nil
			}

			wg.Add(1)
			go func() {
				if err := bkt.Delete(ctx, p); err != nil {
					t.Logf("deleting object %s failed: %s", p, err)
				}
				wg.Done()
			}()
			return nil
		})
		if err != nil {
			t.Logf("iterating over bucket objects failed: %s", err)
			wg.Wait()
			return
		}
	}
	wg.Wait()
}
