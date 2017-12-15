package objstore

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Bucket provides read and write access to an object storage bucket.
type Bucket interface {
	BucketReader

	// Upload writes the file specified in src to remote GCS location specified as target.
	Upload(ctx context.Context, name string, r io.Reader) error

	// Delete removes the object with the given names.
	Delete(ctx context.Context, name string) error
}

// BucketReader provides read access to an object storage bucket.
type BucketReader interface {
	// Iter calls f for each entry in the given directory. The argument to f is the full
	// object name including the prefix of the inspected directory.
	Iter(ctx context.Context, dir string, f func(string) error) error

	// Get returns a reader for the given object name.
	Get(ctx context.Context, name string) (io.ReadCloser, error)

	// GetRange returns a new range reader for the given object name and range.
	GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error)

	// Exists checks if the given object exists in the bucket.
	Exists(ctx context.Context, name string) (bool, error)
}

// UploadDir uploads all files in srcdir to the bucket with into a top-level directory
// named dstdir.
func UploadDir(ctx context.Context, bkt Bucket, srcdir, dstdir string) error {
	df, err := os.Stat(srcdir)
	if err != nil {
		return errors.Wrap(err, "stat dir")
	}
	if !df.IsDir() {
		return errors.Errorf("%s is not a directory", srcdir)
	}
	return filepath.Walk(srcdir, func(src string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			return nil
		}
		r, err := os.Open(src)
		if err != nil {
			return errors.Wrapf(err, "open file %s", src)
		}
		defer r.Close()

		dst := filepath.Join(dstdir, strings.TrimPrefix(src, srcdir))

		if err := bkt.Upload(ctx, dst, r); err != nil {
			return errors.Wrapf(err, "upload file %s as %s", src, dst)
		}
		return nil
	})
}

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

// DeleteDir removes all objects prefixed with dir from the bucket.
func DeleteDir(ctx context.Context, bkt Bucket, dir string) error {
	bkt.Iter(ctx, dir, func(name string) error {
		// If we hit a directory, call DeleteDir recursively.
		if strings.HasSuffix(name, DirDelim) {
			return DeleteDir(ctx, bkt, name)
		}
		return bkt.Delete(ctx, name)
	})
	return nil
}

// DownloadFile downloads the src file from the bucket to dst. If dst is an existing
// directory, a file with the same name as the source is created in dst.
func DownloadFile(ctx context.Context, bkt BucketReader, src, dst string) error {
	if fi, err := os.Stat(dst); err == nil {
		if fi.IsDir() {
			dst = filepath.Join(dst, filepath.Base(src))
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	rc, err := bkt.Get(ctx, src)
	if err != nil {
		return errors.Wrap(err, "get file")
	}
	defer rc.Close()

	f, err := os.Create(dst)
	if err != nil {
		return errors.Wrap(err, "create file")
	}
	defer func() {
		f.Close()
		// Best-effort cleanup.
		if err != nil {
			os.Remove(dst)
		}
	}()
	if _, err = io.Copy(f, rc); err != nil {
		return errors.Wrap(err, "copy object to file")
	}
	return nil
}

// DownloadDir downloads all object found in the directory into the local directory.
func DownloadDir(ctx context.Context, bkt BucketReader, src, dst string) error {
	if err := os.MkdirAll(dst, 0777); err != nil {
		return errors.Wrap(err, "create dir")
	}
	err := bkt.Iter(ctx, src, func(name string) error {
		if strings.HasSuffix(name, DirDelim) {
			return DownloadDir(ctx, bkt, name, filepath.Join(dst, filepath.Base(name)))
		}
		return DownloadFile(ctx, bkt, name, dst)
	})
	// Best-effort cleanup if the download failed.
	if err != nil {
		os.RemoveAll(dst)
	}
	return err
}

// BucketWithMetrics takes a bucket and registers metrics with the given registry for
// operations run against the bucket.
func BucketWithMetrics(name string, b Bucket, r prometheus.Registerer) Bucket {
	bkt := &metricBucket{
		Bucket: b,
		ops: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_store_bucket_operations_total",
			Help:        "Total number of store operations that were executed against a bucket.",
			ConstLabels: prometheus.Labels{"bucket": name},
		}, []string{"operation"}),
	}
	if r != nil {
		r.MustRegister(bkt.ops)
	}
	return bkt
}

type metricBucket struct {
	Bucket
	ops *prometheus.CounterVec
}

func (b *metricBucket) Iter(ctx context.Context, dir string, f func(name string) error) error {
	b.ops.WithLabelValues("iter").Inc()
	return b.Bucket.Iter(ctx, dir, f)
}

func (b *metricBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.ops.WithLabelValues("get").Inc()
	return b.Bucket.Get(ctx, name)
}

func (b *metricBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.ops.WithLabelValues("get_range").Inc()
	return b.Bucket.GetRange(ctx, name, off, length)
}
