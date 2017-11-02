package shipper

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/iterator"
)

// GCSRemote implements a remote for Google Cloud Storage.
type GCSRemote struct {
	logger  log.Logger
	metrics *gcsRemoteMetrics
	bucket  *storage.BucketHandle
}

type gcsRemoteMetrics struct {
	dirSyncs        prometheus.Counter
	dirSyncFailures prometheus.Counter
	uploads         prometheus.Counter
	uploadFailures  prometheus.Counter
}

func newGCSRemoteMetrics(r prometheus.Registerer) *gcsRemoteMetrics {
	var m gcsRemoteMetrics

	m.dirSyncs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "promlts_gcs_remote_dir_syncs_total",
		Help: "Total dir sync attempts",
	})
	m.dirSyncFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "promlts_gcs_remote_dir_sync_failures_total",
		Help: "Total number of failed  dir syncs",
	})
	m.uploads = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "promlts_gcs_remote_uploads_total",
		Help: "Total object upload attempts",
	})
	m.uploadFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "promlts_gcs_remote_upload_failures_total",
		Help: "Total number of failed object uploads",
	})

	if r != nil {
		prometheus.MustRegister()
	}
	return &m
}

// NewGCSRemote returns a new GCSRemote.
func NewGCSRemote(logger log.Logger, metricReg prometheus.Registerer, bucket *storage.BucketHandle) *GCSRemote {
	return &GCSRemote{
		logger:  logger,
		bucket:  bucket,
		metrics: newGCSRemoteMetrics(metricReg),
	}
}

func (r *GCSRemote) listDir(ctx context.Context, dir string) *storage.ObjectIterator {
	return r.bucket.Objects(ctx, &storage.Query{
		Delimiter: "/",
		Prefix:    dir,
	})
}

// Exists checks if the given directory exists at the remote site.
func (r *GCSRemote) Exists(ctx context.Context, dir string) (bool, error) {
	objs := r.listDir(ctx, dir)
	for {
		if _, err := objs.Next(); err == iterator.Done {
			break
		} else if err != nil {
			return false, err
		}
		// The first object found with the given filter indicates that the directory exists.
		// XXX(fabxc): do a full check whether the files add up as well?
		return true, nil
	}
	return false, nil
}

// Upload the given directory to the remote site.
func (r *GCSRemote) Upload(ctx context.Context, dir string) error {
	r.metrics.dirSyncs.Inc()

	parent := filepath.Dir(dir)

	err := filepath.Walk(dir, func(name string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			return nil
		}
		return r.uploadSingle(ctx, name, strings.TrimPrefix(name, parent))
	})
	if err == nil {
		return nil
	}
	r.metrics.dirSyncFailures.Inc()
	level.Error(r.logger).Log("msg", "upload failed; remove partial data", "dir", dir, "err", err)

	// We don't want to leave partially uploaded directories behind. Cleanup everything related to it
	// and use a uncanceled context.
	if err2 := r.delete(context.Background(), dir); err2 != nil {
		level.Error(r.logger).Log(
			"msg", "cleanup failed; partial data may be left behind", "dir", dir, "err", err2)
	}
	return err
}

func (r *GCSRemote) uploadSingle(ctx context.Context, src, target string) error {
	level.Debug(r.logger).Log("msg", "upload file", "file", src)
	r.metrics.uploads.Inc()

	f, err := os.Open(src)
	if err != nil {
		r.metrics.uploadFailures.Inc()
		return err
	}

	w := r.bucket.Object(target).NewWriter(ctx)
	_, err = io.Copy(w, f)

	if err != nil {
		r.metrics.uploadFailures.Inc()
		return err
	}
	return w.Close()
}

// delete removes all data prefixed with the dir.
func (r *GCSRemote) delete(ctx context.Context, dir string) error {
	objs := r.listDir(ctx, dir)
	for {
		oa, err := objs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if err := r.bucket.Object(oa.Name).Delete(ctx); err != nil {
			return err
		}
	}
	return nil
}
