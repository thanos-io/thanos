package shipper

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/oklog/ulid"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/iterator"
)

const (
	// Class A operations.
	gcsOperationObjectsList  = "objects.list"
	gcsOperationObjectInsert = "object.insert"
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
	gcsOperations   *prometheus.CounterVec
}

func newGCSRemoteMetrics(r prometheus.Registerer) *gcsRemoteMetrics {
	var m gcsRemoteMetrics

	m.dirSyncs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_gcs_remote_dir_syncs_total",
		Help: "Total dir sync attempts",
	})
	m.dirSyncFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_gcs_remote_dir_sync_failures_total",
		Help: "Total number of failed dir syncs",
	})
	m.uploads = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_gcs_remote_uploads_total",
		Help: "Total object upload attempts",
	})
	m.uploadFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_gcs_remote_upload_failures_total",
		Help: "Total number of failed object uploads",
	})
	m.gcsOperations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_gcs_operations_total",
		Help: "Number of Google Storage operations.",
	}, []string{"type"})

	if r != nil {
		prometheus.MustRegister(
			m.dirSyncs,
			m.dirSyncFailures,
			m.uploads,
			m.uploadFailures,
			m.gcsOperations,
		)
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
	r.metrics.gcsOperations.WithLabelValues(gcsOperationObjectsList).Inc()
	return r.bucket.Objects(ctx, &storage.Query{
		Delimiter: "/",
		Prefix:    dir,
	})
}

// Exists checks if the given directory exists at the remote site.
func (r *GCSRemote) Exists(ctx context.Context, id ulid.ULID) (bool, error) {
	objs := r.listDir(ctx, id.String())
	for {
		if _, err := objs.Next(); err == iterator.Done {
			break
		} else if err != nil {
			return false, err
		}
		// The first object found with the given filter indicates that the directory exists.
		// XXX(fabxc): do a more elaborate check whether local and remote files match?
		return true, nil
	}
	return false, nil
}

// Upload the given directory to the remote site.
func (r *GCSRemote) Upload(ctx context.Context, id ulid.ULID, dir string) error {
	r.metrics.dirSyncs.Inc()

	err := filepath.Walk(dir, func(name string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			return nil
		}
		return r.uploadSingle(ctx, name, filepath.Join(id.String(), strings.TrimPrefix(name, dir)))
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
	level.Debug(r.logger).Log("msg", "upload file", "src", src, "dst", target)
	r.metrics.uploads.Inc()

	f, err := os.Open(src)
	if err != nil {
		r.metrics.uploadFailures.Inc()
		return err
	}

	r.metrics.gcsOperations.WithLabelValues(gcsOperationObjectInsert).Inc()
	w := r.bucket.Object(target).NewWriter(ctx)

	_, err = io.Copy(w, f)
	if err != nil {
		r.metrics.uploadFailures.Inc()
		return err
	}
	return w.Close()
}

// delete removes all data prefixed with the dir.
// NOTE: object.Delete operation is free so no worth to increment gcs operations metric.
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
