package objstore

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Bucket provides read and write access to an object storage bucket.
// NOTE: We assume strong consistency for write-read flow.
type Bucket interface {
	io.Closer
	BucketReader

	// Upload the contents of the reader as an object into the bucket.
	Upload(ctx context.Context, name string, r io.Reader) error

	// Delete removes the object with the given name.
	Delete(ctx context.Context, name string) error

	// Name returns the bucket name for the provider.
	Name() string
}

// BucketReader provides read access to an object storage bucket.
type BucketReader interface {
	// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
	// object name including the prefix of the inspected directory.
	Iter(ctx context.Context, dir string, f func(string) error) error

	// Get returns a reader for the given object name.
	Get(ctx context.Context, name string) (io.ReadCloser, error)

	// GetRange returns a new range reader for the given object name and range.
	GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error)

	// Exists checks if the given object exists in the bucket.
	// TODO(bplotka): Consider removing Exists in favor of helper that do Get & IsObjNotFoundErr (less code to maintain).
	Exists(ctx context.Context, name string) (bool, error)

	// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
	IsObjNotFoundErr(err error) bool
}

// UploadDir uploads all files in srcdir to the bucket with into a top-level directory
// named dstdir. It is a caller responsibility to clean partial upload in case of failure.
func UploadDir(ctx context.Context, logger log.Logger, bkt Bucket, srcdir, dstdir string) error {
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
		dst := filepath.Join(dstdir, strings.TrimPrefix(src, srcdir))

		return UploadFile(ctx, logger, bkt, src, dst)
	})
}

// UploadFile uploads the file with the given name to the bucket.
// It is a caller responsibility to clean partial upload in case of failure
func UploadFile(ctx context.Context, logger log.Logger, bkt Bucket, src, dst string) error {
	r, err := os.Open(src)
	if err != nil {
		return errors.Wrapf(err, "open file %s", src)
	}
	defer runutil.CloseWithLogOnErr(logger, r, "close file %s", src)

	if err := bkt.Upload(ctx, dst, r); err != nil {
		return errors.Wrapf(err, "upload file %s as %s", src, dst)
	}
	return nil
}

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

// DeleteDir removes all objects prefixed with dir from the bucket.
func DeleteDir(ctx context.Context, bkt Bucket, dir string) error {
	return bkt.Iter(ctx, dir, func(name string) error {
		// If we hit a directory, call DeleteDir recursively.
		if strings.HasSuffix(name, DirDelim) {
			return DeleteDir(ctx, bkt, name)
		}
		return bkt.Delete(ctx, name)
	})
}

// DownloadFile downloads the src file from the bucket to dst. If dst is an existing
// directory, a file with the same name as the source is created in dst.
// If destination file is already existing, download file will overwrite it.
func DownloadFile(ctx context.Context, logger log.Logger, bkt BucketReader, src, dst string) error {
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
	defer runutil.CloseWithLogOnErr(logger, rc, "download block's file reader")

	f, err := os.Create(dst)
	if err != nil {
		return errors.Wrap(err, "create file")
	}
	defer runutil.CloseWithLogOnErr(logger, f, "download block's output file")

	defer func() {
		if err != nil {
			if rerr := os.Remove(dst); rerr != nil {
				level.Warn(logger).Log("msg", "failed to remove partially downloaded file", "file", dst, "err", rerr)
			}
		}
	}()
	if _, err = io.Copy(f, rc); err != nil {
		return errors.Wrap(err, "copy object to file")
	}
	return nil
}

// DownloadDir downloads all object found in the directory into the local directory.
func DownloadDir(ctx context.Context, logger log.Logger, bkt BucketReader, src, dst string) error {
	if err := os.MkdirAll(dst, 0777); err != nil {
		return errors.Wrap(err, "create dir")
	}

	var downloadedFiles []string
	if err := bkt.Iter(ctx, src, func(name string) error {
		if strings.HasSuffix(name, DirDelim) {
			return DownloadDir(ctx, logger, bkt, name, filepath.Join(dst, filepath.Base(name)))
		}
		if err := DownloadFile(ctx, logger, bkt, name, dst); err != nil {
			return err
		}

		downloadedFiles = append(downloadedFiles, dst)
		return nil
	}); err != nil {
		// Best-effort cleanup if the download failed.
		for _, f := range downloadedFiles {
			if rerr := os.Remove(f); rerr != nil {
				level.Warn(logger).Log("msg", "failed to remove file on partial dir download error", "file", f, "err", rerr)
			}
		}
		return err
	}

	return nil
}

// BucketWithMetrics takes a bucket and registers metrics with the given registry for
// operations run against the bucket.
func BucketWithMetrics(name string, b Bucket, r prometheus.Registerer) Bucket {
	bkt := &metricBucket{
		bkt: b,

		ops: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_objstore_bucket_operations_total",
			Help:        "Total number of operations against a bucket.",
			ConstLabels: prometheus.Labels{"bucket": name},
		}, []string{"operation"}),

		opsFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_objstore_bucket_operation_failures_total",
			Help:        "Total number of operations against a bucket that failed.",
			ConstLabels: prometheus.Labels{"bucket": name},
		}, []string{"operation"}),

		opsDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "thanos_objstore_bucket_operation_duration_seconds",
			Help:        "Duration of operations against the bucket",
			ConstLabels: prometheus.Labels{"bucket": name},
			Buckets:     []float64{0.005, 0.01, 0.02, 0.04, 0.08, 0.15, 0.3, 0.6, 1, 1.5, 2.5, 5, 10, 20, 30},
		}, []string{"operation"}),
		lastSuccessfullUploadTime: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "thanos_objstore_bucket_last_successful_upload_time",
			Help: "Second timestamp of the last successful upload to the bucket.",
		}, []string{"bucket"}),
	}
	if r != nil {
		r.MustRegister(bkt.ops, bkt.opsFailures, bkt.opsDuration, bkt.lastSuccessfullUploadTime)
	}
	return bkt
}

type metricBucket struct {
	bkt Bucket

	ops                       *prometheus.CounterVec
	opsFailures               *prometheus.CounterVec
	opsDuration               *prometheus.HistogramVec
	lastSuccessfullUploadTime *prometheus.GaugeVec
}

func (b *metricBucket) Iter(ctx context.Context, dir string, f func(name string) error) error {
	const op = "iter"

	err := b.bkt.Iter(ctx, dir, f)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
	}
	b.ops.WithLabelValues(op).Inc()

	return err
}

func (b *metricBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	const op = "get"
	b.ops.WithLabelValues(op).Inc()

	rc, err := b.bkt.Get(ctx, name)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
		return nil, err
	}
	rc = newTimingReadCloser(
		rc,
		op,
		b.opsDuration,
		b.opsFailures,
	)

	return rc, nil
}

func (b *metricBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	const op = "get_range"
	b.ops.WithLabelValues(op).Inc()

	rc, err := b.bkt.GetRange(ctx, name, off, length)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
		return nil, err
	}
	rc = newTimingReadCloser(
		rc,
		op,
		b.opsDuration,
		b.opsFailures,
	)

	return rc, nil
}

func (b *metricBucket) Exists(ctx context.Context, name string) (bool, error) {
	const op = "exists"
	start := time.Now()

	ok, err := b.bkt.Exists(ctx, name)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
	}
	b.ops.WithLabelValues(op).Inc()
	b.opsDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())

	return ok, err
}

func (b *metricBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	const op = "upload"
	start := time.Now()

	err := b.bkt.Upload(ctx, name, r)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
	} else {
		//TODO: Use SetToCurrentTime() once we update the Prometheus client_golang
		b.lastSuccessfullUploadTime.WithLabelValues(b.bkt.Name()).Set(float64(time.Now().UnixNano()) / 1e9)
	}
	b.ops.WithLabelValues(op).Inc()
	b.opsDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())

	return err
}

func (b *metricBucket) Delete(ctx context.Context, name string) error {
	const op = "delete"
	start := time.Now()

	err := b.bkt.Delete(ctx, name)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
	}
	b.ops.WithLabelValues(op).Inc()
	b.opsDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())

	return err
}

func (b *metricBucket) IsObjNotFoundErr(err error) bool {
	return b.bkt.IsObjNotFoundErr(err)
}

func (b *metricBucket) Close() error {
	return b.bkt.Close()
}

func (b *metricBucket) Name() string {
	return b.bkt.Name()
}

type timingReadCloser struct {
	io.ReadCloser

	ok       bool
	start    time.Time
	op       string
	duration *prometheus.HistogramVec
	failed   *prometheus.CounterVec
}

func newTimingReadCloser(rc io.ReadCloser, op string, dur *prometheus.HistogramVec, failed *prometheus.CounterVec) *timingReadCloser {
	// Initialize the metrics with 0.
	dur.WithLabelValues(op)
	failed.WithLabelValues(op)
	return &timingReadCloser{
		ReadCloser: rc,
		ok:         true,
		start:      time.Now(),
		op:         op,
		duration:   dur,
		failed:     failed,
	}
}

func (rc *timingReadCloser) Close() error {
	err := rc.ReadCloser.Close()
	rc.duration.WithLabelValues(rc.op).Observe(time.Since(rc.start).Seconds())
	if rc.ok && err != nil {
		rc.failed.WithLabelValues(rc.op).Inc()
		rc.ok = false
	}
	return err
}

func (rc *timingReadCloser) Read(b []byte) (n int, err error) {
	n, err = rc.ReadCloser.Read(b)
	if rc.ok && err != nil && err != io.EOF {
		rc.failed.WithLabelValues(rc.op).Inc()
		rc.ok = false
	}
	return n, err
}
