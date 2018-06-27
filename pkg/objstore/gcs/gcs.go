// Package gcs implements common object storage abstractions against Google Cloud Storage.
package gcs

import (
	"context"
	"io"
	"strings"

	"fmt"
	"math/rand"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/iterator"
)

const (
	// Class A operations.
	opObjectsList  = "objects.list"
	opObjectInsert = "object.insert"

	// Class B operation.
	opObjectGet = "object.get"

	// Free operations.
	opObjectDelete = "object.delete"
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

// Bucket implements the store.Bucket and shipper.Bucket interfaces against GCS.
type Bucket struct {
	bkt      *storage.BucketHandle
	opsTotal *prometheus.CounterVec

	closer io.Closer
}

// NewBucket returns a new Bucket against the given bucket handle.
func NewBucket(name string, cl *storage.Client, reg prometheus.Registerer) *Bucket {
	bkt := &Bucket{
		bkt: cl.Bucket(name),
		opsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_objstore_gcs_bucket_operations_total",
			Help:        "Total number of operations that were executed against a Google Compute Storage bucket.",
			ConstLabels: prometheus.Labels{"bucket": name},
		}, []string{"operation"}),
		closer: cl,
	}
	if reg != nil {
		reg.MustRegister()
	}
	return bkt
}

// Iter calls f for each entry in the given directory. The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	b.opsTotal.WithLabelValues(opObjectsList).Inc()
	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if dir != "" {
		dir = strings.TrimSuffix(dir, DirDelim) + DirDelim
	}
	it := b.bkt.Objects(ctx, &storage.Query{
		Prefix:    dir,
		Delimiter: DirDelim,
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

// Exists checks if the given object exists.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	b.opsTotal.WithLabelValues(opObjectGet).Inc()

	if _, err := b.bkt.Object(name).Attrs(ctx); err == nil {
		return true, nil
	} else if err != storage.ErrObjectNotExist {
		return false, err
	}
	return false, nil
}

// Upload writes the file specified in src to remote GCS location specified as target.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	b.opsTotal.WithLabelValues(opObjectInsert).Inc()

	w := b.bkt.Object(name).NewWriter(ctx)

	if _, err := io.Copy(w, r); err != nil {
		return err
	}
	return w.Close()
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	b.opsTotal.WithLabelValues(opObjectDelete).Inc()

	return b.bkt.Object(name).Delete(ctx)
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	return err == storage.ErrObjectNotExist
}

func (b *Bucket) Close() error {
	return b.closer.Close()
}

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB, project string) (objstore.Bucket, func(), error) {
	ctx, cancel := context.WithCancel(context.Background())
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	src := rand.NewSource(time.Now().UnixNano())
	name := fmt.Sprintf("test_%s_%x", strings.ToLower(t.Name()), src.Int63())

	bkt := gcsClient.Bucket(name)
	if err = bkt.Create(ctx, project, nil); err != nil {
		cancel()
		_ = gcsClient.Close()
		return nil, nil, err
	}

	b := NewBucket(name, gcsClient, nil)

	t.Log("created temporary GCS bucket for GCS tests with name", name, "in project", project)
	return b, func() {
		objstore.EmptyBucket(t, ctx, b)
		if err := bkt.Delete(ctx); err != nil {
			t.Logf("deleting bucket failed: %s", err)
		}
		cancel()
		if err := b.Close(); err != nil {
			t.Logf("closing bucket failed: %s", err)
		}
	}, nil
}
