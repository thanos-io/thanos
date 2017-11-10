// Package shipper detects directories on the local file system and uploads
// them to a block storage.
package shipper

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

// Remote represents a remote data store to which directories are uploaded.
type Remote interface {
	Exists(ctx context.Context, id ulid.ULID) (bool, error)
	Upload(ctx context.Context, id ulid.ULID, dir string) error
}

// Shipper watches a directory for matching files and directories and uploads
// them to a remote data store.
type Shipper struct {
	logger log.Logger
	dir    string
	remote Remote
	match  func(os.FileInfo) bool
	labels func() labels.Labels
}

// New creates a new shipper that detects new TSDB blocks in dir and uploads them
// to remote if necessary. It attaches the return value of the labels getter to uploaded data.
func New(
	logger log.Logger,
	metric prometheus.Registerer,
	dir string,
	remote Remote,
	lbls func() labels.Labels,
) *Shipper {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if lbls == nil {
		lbls = func() labels.Labels { return nil }
	}
	return &Shipper{
		logger: logger,
		dir:    dir,
		remote: remote,
		labels: lbls,
	}
}

// Sync performs a single synchronization if the local block data with the remote end.
func (s *Shipper) Sync(ctx context.Context) {
	names, err := readDir(s.dir)
	if err != nil {
		level.Warn(s.logger).Log("msg", "read dir failed", "err", err)
	}
	for _, dn := range names {
		dn = filepath.Join(s.dir, dn)

		fi, err := os.Stat(dn)
		if err != nil {
			level.Warn(s.logger).Log("msg", "open file failed", "err", err)
			continue
		}
		if !fi.IsDir() {
			continue
		}
		id, err := ulid.Parse(fi.Name())
		if err != nil {
			continue
		}
		if err := s.sync(ctx, id, dn); err != nil {
			level.Error(s.logger).Log("msg", "shipping failed", "dir", dn, "err", err)
		}
	}
}

// blockMeta is regular TSDB block meta extended by Thanos specific information.
type blockMeta struct {
	Version int `json:"version"`

	tsdb.BlockMeta

	Thanos thanosBlockMeta `json:"thanos"`
}

type thanosBlockMeta struct {
	Labels map[string]string `json:"labels"`
}

func (s *Shipper) sync(ctx context.Context, id ulid.ULID, dir string) error {
	meta, err := readMetaFile(dir)
	if err != nil {
		return errors.Wrap(err, "read meta file")
	}
	// We only ship of the first compacted block level.
	if meta.Compaction.Level > 1 {
		return nil
	}
	ok, err := s.remote.Exists(ctx, id)
	if err != nil {
		return errors.Wrap(err, "check exists")
	}
	if ok {
		return nil
	}

	level.Info(s.logger).Log("msg", "upload new block", "id", id)

	// We hard-link the files into a temporary upload directory so we are not affected
	// by other operations happening against the TSDB directory.
	updir := filepath.Join(s.dir, "thanos", "upload")

	if err := os.RemoveAll(updir); err != nil {
		return errors.Wrap(err, "clean upload directory")
	}
	if err := os.MkdirAll(updir, 0777); err != nil {
		return errors.Wrap(err, "create upload dir")
	}
	defer os.RemoveAll(updir)

	if err := os.Link(filepath.Join(dir, "index"), filepath.Join(updir, "index")); err != nil {
		return errors.Wrap(err, "link index file")
	}
	if err := os.Link(filepath.Join(dir, "chunks"), filepath.Join(updir, "chunks")); err != nil {
		return errors.Wrap(err, "link chunk directory")
	}
	// Tombstone files are skipped as they should not be used in the Thanos deployment model.

	// Attach current labels and write a new meta file with Thanos extensions.
	if lset := s.labels(); lset != nil {
		meta.Thanos.Labels = lset.Map()
	}
	if err := writeMetaFile(updir, meta); err != nil {
		return errors.Wrap(err, "write meta file")
	}
	return s.remote.Upload(ctx, id, updir)
}

// readDir returns the filenames in the given directory in sorted order.
func readDir(dirpath string) ([]string, error) {
	dir, err := os.Open(dirpath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

const metaFilename = "meta.json"

func writeMetaFile(dir string, meta *blockMeta) error {
	// Make any changes to the file appear atomic.
	path := filepath.Join(dir, metaFilename)
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")

	if err := enc.Encode(meta); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return renameFile(tmp, path)
}

func readMetaFile(dir string) (*blockMeta, error) {
	b, err := ioutil.ReadFile(filepath.Join(dir, metaFilename))
	if err != nil {
		return nil, err
	}
	var m blockMeta

	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m.Version != 1 {
		return nil, errors.Errorf("unexpected meta file version %d", m.Version)
	}
	return &m, nil
}

func renameFile(from, to string) error {
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
		pdir.Close()
		return err
	}
	return pdir.Close()
}
