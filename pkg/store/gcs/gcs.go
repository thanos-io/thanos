// Package gcs implements common object storage abstractions against Google Cloud Storage.
package gcs

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// Bucket implements the store.Bucket interface against GCS.
type Bucket struct {
	bkt *storage.BucketHandle
}

// NewBucket returns a new Bucket against the given bucket handle.
func NewBucket(b *storage.BucketHandle) *Bucket {
	return &Bucket{bkt: b}
}

// Iter calls f for each entry in the given directory. The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	it := b.bkt.Objects(ctx, &storage.Query{
		Prefix:    dir,
		Delimiter: "/",
	})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		if err := f(attrs.Prefix + attrs.Name); err != nil {
			return err
		}
	}
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.bkt.Object(name).NewReader(ctx)
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.bkt.Object(name).NewRangeReader(ctx, off, length)
}

// Handle returns the underlying GCS bucket handle.
func (b *Bucket) Handle() *storage.BucketHandle {
	return b.bkt
}
