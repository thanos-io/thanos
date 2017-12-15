// Package shipper detects directories on the local file system and uploads
// them to a block storage.
package shipper

import (
	"context"
	"math"
	"os"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/labels"
)

type metrics struct {
	dirSyncs        prometheus.Counter
	dirSyncFailures prometheus.Counter
	uploads         prometheus.Counter
	uploadFailures  prometheus.Counter
}

func newMetrics(r prometheus.Registerer) *metrics {
	var m metrics

	m.dirSyncs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_dir_syncs_total",
		Help: "Total dir sync attempts",
	})
	m.dirSyncFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_dir_sync_failures_total",
		Help: "Total number of failed dir syncs",
	})
	m.uploads = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_uploads_total",
		Help: "Total object upload attempts",
	})
	m.uploadFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_upload_failures_total",
		Help: "Total number of failed object uploads",
	})

	if r != nil {
		r.MustRegister(
			m.dirSyncs,
			m.dirSyncFailures,
			m.uploads,
			m.uploadFailures,
		)
	}
	return &m
}

// Shipper watches a directory for matching files and directories and uploads
// them to a remote data store.
type Shipper struct {
	logger log.Logger
	dir    string
	bucket objstore.Bucket
	match  func(os.FileInfo) bool
	labels func() labels.Labels
	// MaxTime timestamp does not make sense for sidecar, so we need to gossip minTime only. We always have freshest data.
	gossipMinTimeFn func(mint int64)

	metrics *metrics
}

// New creates a new shipper that detects new TSDB blocks in dir and uploads them
// to remote if necessary. It attaches the return value of the labels getter to uploaded data.
func New(
	logger log.Logger,
	r prometheus.Registerer,
	dir string,
	bucket objstore.Bucket,
	lbls func() labels.Labels,
	gossipMinTimeFn func(mint int64),
) *Shipper {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if lbls == nil {
		lbls = func() labels.Labels { return nil }
	}
	if gossipMinTimeFn == nil {
		gossipMinTimeFn = func(mint int64) {}
	}
	return &Shipper{
		logger:          logger,
		dir:             dir,
		bucket:          bucket,
		labels:          lbls,
		gossipMinTimeFn: gossipMinTimeFn,
		metrics:         newMetrics(r),
	}
}

// Sync performs a single synchronization if the local block data with the remote end.
func (s *Shipper) Sync(ctx context.Context) {
	names, err := fileutil.ReadDir(s.dir)
	if err != nil {
		level.Warn(s.logger).Log("msg", "read dir failed", "err", err)
	}

	var oldestBlockMinTime int64 = math.MaxInt64
	for _, fn := range names {
		id, err := ulid.Parse(fn)
		if err != nil {
			continue
		}
		dir := filepath.Join(s.dir, fn)

		fi, err := os.Stat(dir)
		if err != nil {
			level.Warn(s.logger).Log("msg", "open file failed", "err", err)
			continue
		}
		if !fi.IsDir() {
			continue
		}
		minTime, err := s.sync(ctx, id, dir)
		if err != nil {
			level.Error(s.logger).Log("msg", "shipping failed", "dir", dir, "err", err)
			continue
		}

		if minTime < oldestBlockMinTime || oldestBlockMinTime == math.MaxInt64 {
			oldestBlockMinTime = minTime
		}
	}

	if oldestBlockMinTime != math.MaxInt64 {
		s.gossipMinTimeFn(oldestBlockMinTime)
	}
}

func (s *Shipper) sync(ctx context.Context, id ulid.ULID, dir string) (minTime int64, err error) {
	meta, err := block.ReadMetaFile(dir)
	if err != nil {
		return 0, errors.Wrap(err, "read meta file")
	}
	// We only ship of the first compacted block level.
	if meta.Compaction.Level > 1 {
		return meta.MinTime, nil
	}
	ok, err := s.bucket.Exists(ctx, id.String())
	if err != nil {
		return 0, errors.Wrap(err, "check exists")
	}
	if ok {
		return meta.MinTime, nil
	}

	level.Info(s.logger).Log("msg", "upload new block", "id", id)

	// We hard-link the files into a temporary upload directory so we are not affected
	// by other operations happening against the TSDB directory.
	updir := filepath.Join(s.dir, "thanos", "upload")

	if err := os.RemoveAll(updir); err != nil {
		return 0, errors.Wrap(err, "clean upload directory")
	}
	if err := os.MkdirAll(updir, 0777); err != nil {
		return 0, errors.Wrap(err, "create upload dir")
	}
	defer os.RemoveAll(updir)

	if err := hardlinkBlock(dir, updir); err != nil {
		return 0, errors.Wrap(err, "hard link block")
	}
	// Attach current labels and write a new meta file with Thanos extensions.
	if lset := s.labels(); lset != nil {
		meta.Thanos.Labels = lset.Map()
	}
	if err := block.WriteMetaFile(updir, meta); err != nil {
		return 0, errors.Wrap(err, "write meta file")
	}
	err = objstore.UploadDir(ctx, s.bucket, updir, id.String())
	if err == nil {
		return meta.MinTime, nil
	}
	// Cleanup the dir with an uncancelable context.
	if err2 := objstore.DeleteDir(context.Background(), s.bucket, id.String()); err2 != nil {
		level.Warn(s.logger).Log("msg", "cleaning up block failed", "block", id, "err", err)
	}
	return 0, err
}

func hardlinkBlock(src, dst string) error {
	chunkDir := filepath.Join(dst, "chunks")

	if err := os.MkdirAll(chunkDir, 0777); err != nil {
		return errors.Wrap(err, "create chunks dir")
	}

	files, err := fileutil.ReadDir(filepath.Join(src, "chunks"))
	if err != nil {
		return errors.Wrap(err, "read chunk dir")
	}
	for i, fn := range files {
		files[i] = filepath.Join("chunks", fn)
	}
	files = append(files, "meta.json", "index")

	for _, fn := range files {
		if err := os.Link(filepath.Join(src, fn), filepath.Join(dst, fn)); err != nil {
			return errors.Wrapf(err, "hard link file %s", fn)
		}
	}
	return nil
}
