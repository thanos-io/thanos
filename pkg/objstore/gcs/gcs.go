// Package gcs implements common object storage abstractions against Google Cloud Storage.
package gcs

import (
	"context"
	"io"

	"os"

	"cloud.google.com/go/storage"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/iterator"
)

const (
	// Class A operations.
	opObjectsList  = "objects.list"
	opObjectInsert = "object.insert"

	// Class B operation.
	opObjectGet = "object.get"
)

// Bucket implements the store.Bucket and shipper.Bucket interfaces against GCS.
type Bucket struct {
	bkt *storage.BucketHandle

	opsTotal *prometheus.CounterVec
}

// NewBucket returns a new Bucket against the given bucket handle.
func NewBucket(b *storage.BucketHandle, r prometheus.Registerer, bucketName string) *Bucket {
	bkt := &Bucket{bkt: b}
	bkt.opsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "thanos_objstore_gcs_bucket_operations_total",
		Help:        "Total number of operations that were executed against a Google Compute Storage bucket.",
		ConstLabels: prometheus.Labels{"bucket": bucketName},
	}, []string{"operation"})

	if r != nil {
		r.MustRegister(bkt.opsTotal)
	}
	return bkt
}

// Iter calls f for each entry in the given directory. The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	b.opsTotal.WithLabelValues(opObjectsList).Inc()
	it := b.bkt.Objects(ctx, &storage.Query{
		Prefix:    dir,
		Delimiter: "/",
	})
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
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
	b.opsTotal.WithLabelValues(opObjectGet).Inc()
	return b.bkt.Object(name).NewReader(ctx)
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.opsTotal.WithLabelValues(opObjectGet).Inc()
	return b.bkt.Object(name).NewRangeReader(ctx, off, length)
}

// Handle returns the underlying GCS bucket handle.
// Used for testing purposes (we return handle, so it is not instrumented).
func (b *Bucket) Handle() *storage.BucketHandle {
	return b.bkt
}

// Exists checks if the given directory exists at the remote site (and contains at least one element).
func (b *Bucket) Exists(ctx context.Context, dir string) (bool, error) {
	b.opsTotal.WithLabelValues(opObjectsList).Inc()
	objs := b.bkt.Objects(ctx, &storage.Query{
		Delimiter: "/",
		Prefix:    dir,
	})
	for {
		_, err := objs.Next()
		if err == iterator.Done {
			return false, nil
		}

		if err != nil {
			return false, err
		}

		// The first object found with the given filter indicates that the directory exists.
		return true, nil
	}
}

// Upload writes the file specified in src to remote GCS location specified as target.
func (b *Bucket) Upload(ctx context.Context, src, target string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}

	b.opsTotal.WithLabelValues(opObjectInsert).Inc()
	w := b.bkt.Object(target).NewWriter(ctx)

	_, err = io.Copy(w, f)
	if err != nil {
		return err
	}
	return w.Close()
}

// Delete removes all data prefixed with the dir.
// NOTE: object.Delete operation is free so no worth to increment gcs operations metric.
func (b *Bucket) Delete(ctx context.Context, dir string) error {
	b.opsTotal.WithLabelValues(opObjectsList).Inc()
	objs := b.bkt.Objects(ctx, &storage.Query{
		Delimiter: "/",
		Prefix:    dir,
	})
	for {
		oa, err := objs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		if err := b.bkt.Object(oa.Name).Delete(ctx); err != nil {
			return err
		}
	}
	return nil
}
