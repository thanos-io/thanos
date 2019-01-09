// Package shipper detects directories on the local file system and uploads
// them to a block storage.
package shipper

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"

	"github.com/prometheus/tsdb"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/runutil"
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
	logger  log.Logger
	dir     string
	workDir string
	metrics *metrics
	bucket  objstore.Bucket
	labels  func() labels.Labels
	source  block.SourceType
}

// New creates a new shipper that detects new TSDB blocks in dir and uploads them
// to remote if necessary. It attaches the return value of the labels getter to uploaded data.
func New(
	logger log.Logger,
	r prometheus.Registerer,
	dir string,
	bucket objstore.Bucket,
	lbls func() labels.Labels,
	source block.SourceType,
) *Shipper {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if lbls == nil {
		lbls = func() labels.Labels { return nil }
	}

	return &Shipper{
		logger:  logger,
		dir:     dir,
		bucket:  bucket,
		labels:  lbls,
		metrics: newMetrics(r),
		source:  source,
	}
}

// Timestamps returns the minimum timestamp for which data is available and the highest timestamp
// of blocks that were successfully uploaded.
func (s *Shipper) Timestamps() (minTime, maxSyncTime int64, err error) {
	meta, err := ReadMetaFile(s.dir)
	if err != nil {
		return 0, 0, errors.Wrap(err, "read shipper meta file")
	}
	// Build a map of blocks we already uploaded.
	hasUploaded := make(map[ulid.ULID]struct{}, len(meta.Uploaded))
	for _, id := range meta.Uploaded {
		hasUploaded[id] = struct{}{}
	}

	minTime = math.MaxInt64
	maxSyncTime = math.MinInt64

	if err := s.iterBlockMetas(func(m *block.Meta) error {
		if m.MinTime < minTime {
			minTime = m.MinTime
		}
		if _, ok := hasUploaded[m.ULID]; ok && m.MaxTime > maxSyncTime {
			maxSyncTime = m.MaxTime
		}
		return nil
	}); err != nil {
		return 0, 0, errors.Wrap(err, "iter Block metas for timestamp")
	}

	if minTime == math.MaxInt64 {
		// No block yet found. We cannot assume any min block size so propagate 0 minTime.
		minTime = 0
	}

	return minTime, maxSyncTime, nil
}

// SyncNonCompacted performs a single synchronization, which ensures all non-compacted local blocks have been uploaded
// to the object bucket once.
//
// It is not concurrency-safe, however it is compactor-safe.
func (s *Shipper) SyncNonCompacted(ctx context.Context) (uploaded int, err error) {
	meta, err := ReadMetaFile(s.dir)
	if err != nil {
		// If we encounter any error, proceed with an empty meta file and overwrite it later.
		// The meta file is only used to deduplicate uploads, which are properly handled
		// by the system if their occur anyway.
		if !os.IsNotExist(err) {
			level.Warn(s.logger).Log("msg", "reading meta file failed, will override it", "err", err)
		}
		meta = &Meta{Version: 1}
	}

	// Build a map of blocks we already uploaded.
	hasUploaded := make(map[ulid.ULID]struct{}, len(meta.Uploaded))
	for _, id := range meta.Uploaded {
		hasUploaded[id] = struct{}{}
	}

	// Reset the uploaded slice so we can rebuild it only with blocks that still exist locally.
	meta.Uploaded = nil

	var uploadErrs int
	// Sync non compacted blocks first.
	if err := s.iterBlockMetas(func(m *block.Meta) error {
		// Do not sync a block if we already uploaded or ignored it. If it is no longer found in the bucket,
		// it was generally removed by the compaction process.
		if _, uploaded := hasUploaded[m.ULID]; uploaded {
			meta.Uploaded = append(meta.Uploaded, m.ULID)
			return nil
		}

		// We only ship of the first compacted block level. See https://github.com/improbable-eng/thanos/issues/206
		// for details. This is however not needed for continous Thanos performance. See standalone `sync` command
		// on how to upload old, locally compacted blocks manually.
		if m.Compaction.Level > 1 {
			return nil
		}

		// Check against bucket if the meta file for this block exists.
		ok, err := s.bucket.Exists(ctx, path.Join(m.ULID.String(), block.MetaFilename))
		if err != nil {
			return errors.Wrap(err, "check exists")
		}
		if ok {
			return nil
		}

		if err := s.upload(ctx, m); err != nil {
			level.Error(s.logger).Log("msg", "shipping failed", "block", m.ULID, "err", err)
			// No error returned, just log line. This is because we want other blocks to be uploaded even
			// though this one failed. It will be retried on second Sync iteration.
			s.metrics.uploadFailures.Inc()
			uploadErrs++
			return nil
		}
		meta.Uploaded = append(meta.Uploaded, m.ULID)

		uploaded++
		s.metrics.uploads.Inc()
		return nil
	}); err != nil {
		s.metrics.dirSyncFailures.Inc()
		return uploaded, errors.Wrap(err, "iter local block metas")
	}

	if err := WriteMetaFile(s.logger, s.dir, meta); err != nil {
		level.Warn(s.logger).Log("msg", "updating meta file failed", "err", err)
	}

	s.metrics.dirSyncs.Inc()

	if uploadErrs > 0 {
		return uploaded, errors.Errorf("failed to sync %v blocks", uploadErrs)

	}
	return uploaded, nil
}

// SyncAll performs a single synchronization for all compacted and non-compacted blocks. This ensures all compacted local blocks
// that *can be* uploaded have been uploaded to the object bucket once. This function is *not* designed to be run
// periodically. It should be used as a manual command to export old compacted blocks for migration purposes.
//
// NOTE: It is currently required to turn off compactor first to use this command safely. Otherwise non-fixable overlaps might happen.
//
// It is not concurrency-safe, however it is safe to run it with `shipper.Sync` in different binary.
func SyncAll(
	ctx context.Context,
	logger log.Logger,
	dir string,
	bucket objstore.Bucket,
	lbls func() labels.Labels,
	source block.SourceType,
) (uploaded int, err error) {
	s := New(logger, nil, dir, bucket, lbls, source)

	var merr tsdb.MultiError
	u1, err := s.syncCompacted(ctx)
	if err != nil {
		merr.Add(err)
	}
	u2, err := s.SyncNonCompacted(ctx)
	if err != nil {
		merr.Add(err)
	}

	return u1 + u2, merr.Err()
}

func (s *Shipper) syncCompacted(ctx context.Context) (uploaded int, err error) {
	var (
		metas       []tsdb.BlockMeta
		lookupMetas = map[ulid.ULID]struct{}{}
	)

	level.Info(s.logger).Log("msg", "gathering all existing blocks from the remote bucket")
	if err := s.bucket.Iter(ctx, "", func(path string) error {
		id, ok := block.IsBlockDir(path)
		if !ok {
			return nil
		}

		m, err := block.DownloadMeta(ctx, s.logger, s.bucket, id)
		if err != nil {
			return err
		}

		if !labels.FromMap(m.Thanos.Labels).Equals(s.labels()) {
			return nil
		}

		metas = append(metas, m.BlockMeta)
		lookupMetas[m.ULID] = struct{}{}
		return nil
	}); err != nil {
		return 0, errors.Wrap(err, "get all block meta.")
	}

	var errs int
	if err := s.iterBlockMetas(func(m *block.Meta) error {
		if m.Compaction.Level < 2 {
			return nil
		}

		if _, exists := lookupMetas[m.ULID]; exists {
			return nil
		}

		if o := tsdb.OverlappingBlocks(append([]tsdb.BlockMeta{m.BlockMeta}, metas...)); len(o) > 0 {
			// TODO(bwplotka): Consider checking if overlaps relates to block in concern?
			level.Error(s.logger).Log("msg", "shipping compacted block blocked; overlap spotted.", "overlap", o.String(), "block", m.ULID)
			errs++
			return nil
		}

		if err := s.upload(ctx, m); err != nil {
			level.Error(s.logger).Log("msg", "shipping compacted block failed", "block", m.ULID, "err", err)
			errs++
			return nil
		}
		uploaded++
		return nil
	}); err != nil {
		return uploaded, errors.Wrap(err, "iter block metas failed")
	}

	if errs > 0 {
		return uploaded, errors.Errorf("failed to sync %v compacted blocks", errs)
	}
	return uploaded, nil
}

// sync uploads the block if not exists in remote storage.
func (s *Shipper) upload(ctx context.Context, meta *block.Meta) error {
	level.Info(s.logger).Log("msg", "upload new block", "id", meta.ULID)

	// We hard-link the files into a temporary upload directory so we are not affected
	// by other operations happening against the TSDB directory.
	updir := filepath.Join(s.dir, "thanos", "upload", meta.ULID.String())

	// Remove updir just in case.
	if err := os.RemoveAll(updir); err != nil {
		return errors.Wrap(err, "clean upload directory")
	}
	if err := os.MkdirAll(updir, 0777); err != nil {
		return errors.Wrap(err, "create upload dir")
	}
	defer func() {
		if err := os.RemoveAll(updir); err != nil {
			level.Error(s.logger).Log("msg", "failed to clean upload directory", "err", err)
		}
	}()

	dir := filepath.Join(s.dir, meta.ULID.String())
	if err := hardlinkBlock(dir, updir); err != nil {
		return errors.Wrap(err, "hard link block")
	}
	// Attach current labels and write a new meta file with Thanos extensions.
	if lset := s.labels(); lset != nil {
		meta.Thanos.Labels = lset.Map()
	}
	meta.Thanos.Source = s.source
	if err := block.WriteMetaFile(s.logger, updir, meta); err != nil {
		return errors.Wrap(err, "write meta file")
	}
	return block.Upload(ctx, s.logger, s.bucket, updir)
}

// iterBlockMetas calls f with the block meta for each block found in dir. It logs
// an error and continues if it cannot access a meta.json file.
// If f returns an error, the function returns with the same error.
func (s *Shipper) iterBlockMetas(f func(m *block.Meta) error) error {
	names, err := fileutil.ReadDir(s.dir)
	if err != nil {
		return errors.Wrap(err, "read dir")
	}
	for _, n := range names {
		if _, ok := block.IsBlockDir(n); !ok {
			continue
		}
		dir := filepath.Join(s.dir, n)

		fi, err := os.Stat(dir)
		if err != nil {
			level.Warn(s.logger).Log("msg", "open file failed", "err", err)
			continue
		}
		if !fi.IsDir() {
			continue
		}
		m, err := block.ReadMetaFile(dir)
		if err != nil {
			level.Warn(s.logger).Log("msg", "reading meta file failed", "err", err)
			continue
		}
		if err := f(m); err != nil {
			return err
		}
	}
	return nil
}

func hardlinkBlock(src, dst string) error {
	chunkDir := filepath.Join(dst, block.ChunksDirname)

	if err := os.MkdirAll(chunkDir, 0777); err != nil {
		return errors.Wrap(err, "create chunks dir")
	}

	files, err := fileutil.ReadDir(filepath.Join(src, block.ChunksDirname))
	if err != nil {
		return errors.Wrap(err, "read chunk dir")
	}
	for i, fn := range files {
		files[i] = filepath.Join(block.ChunksDirname, fn)
	}
	files = append(files, block.MetaFilename, block.IndexFilename)

	for _, fn := range files {
		if err := os.Link(filepath.Join(src, fn), filepath.Join(dst, fn)); err != nil {
			return errors.Wrapf(err, "hard link file %s", fn)
		}
	}
	return nil
}

// Meta defines the fomart thanos.shipper.json file that the shipper places in the data directory.
type Meta struct {
	Version  int         `json:"version"`
	Uploaded []ulid.ULID `json:"uploaded"`
	Ignored  []ulid.ULID `json:"ignored"`
}

// MetaFilename is the known JSON filename for meta information.
const MetaFilename = "thanos.shipper.json"

// WriteMetaFile writes the given meta into <dir>/thanos.shipper.json.
func WriteMetaFile(logger log.Logger, dir string, meta *Meta) error {
	// Make any changes to the file appear atomic.
	path := filepath.Join(dir, MetaFilename)
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")

	if err := enc.Encode(meta); err != nil {
		runutil.CloseWithLogOnErr(logger, f, "write meta file close")
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return renameFile(logger, tmp, path)
}

// ReadMetaFile reads the given meta from <dir>/thanos.shipper.json.
func ReadMetaFile(dir string) (*Meta, error) {
	b, err := ioutil.ReadFile(filepath.Join(dir, MetaFilename))
	if err != nil {
		return nil, err
	}
	var m Meta

	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m.Version != 1 {
		return nil, errors.Errorf("unexpected meta file version %d", m.Version)
	}

	return &m, nil
}

func renameFile(logger log.Logger, from, to string) error {
	if err := os.RemoveAll(to); err != nil {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}

	// Directory was renamed; sync parent dir to persist rename.
	pdir, err := fileutil.OpenDir(filepath.Dir(to))
	if err != nil {
		return err
	}

	if err = fileutil.Fsync(pdir); err != nil {
		runutil.CloseWithLogOnErr(logger, pdir, "rename file dir close")
		return err
	}
	return pdir.Close()
}
