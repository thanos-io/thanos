// Package shipper detects directories on the local file system and uploads
// them to a block storage.
package shipper

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

// Remote represents a remote data store to which directories are uploaded.
type Remote interface {
	Exists(ctx context.Context, id string) (bool, error)
	Upload(ctx context.Context, dir string) error
}

// Shipper watches a directory for matching files and directories and uploads
// them to a remote data store.
type Shipper struct {
	logger log.Logger
	dir    string
	remote Remote
	match  func(os.FileInfo) bool
}

// New creates a new shipper.
func New(
	logger log.Logger,
	metric prometheus.Registerer,
	dir string,
	remote Remote,
	match func(os.FileInfo) bool,
) *Shipper {
	return &Shipper{
		logger: logger,
		dir:    dir,
		remote: remote,
		match:  match,
	}
}

const syncInterval = 5 * time.Second

// IsULIDDir returns true if the described file is a directory with a name that is
// a valid ULID.
func IsULIDDir(fi os.FileInfo) bool {
	if !fi.IsDir() {
		return false
	}
	_, err := ulid.Parse(fi.Name())
	return err == nil
}

// Run the shipper.
func (s *Shipper) Run(ctx context.Context) error {
	tick := time.NewTicker(syncInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			names, err := readDir(s.dir)
			if err != nil {
				level.Warn(s.logger).Log("msg", "read dir failed", "err", err)
				continue
			}
			for _, dn := range names {
				dn = filepath.Join(s.dir, dn)

				fi, err := os.Stat(dn)
				if err != nil {
					level.Warn(s.logger).Log("msg", "open file failed", "err", err)
					continue
				}
				if !s.match(fi) {
					continue
				}
				if err := s.sync(ctx, dn); err != nil {
					level.Error(s.logger).Log("msg", "shipping failed", "dir", dn, "err", err)
				}
			}
		}
	}
}

func (s *Shipper) sync(ctx context.Context, dir string) error {
	ok, err := s.remote.Exists(ctx, dir)
	if err != nil {
		return errors.Wrap(err, "check exists")
	}
	if ok {
		return nil
	}
	level.Info(s.logger).Log("msg", "upload new block", "dir", dir)
	return s.remote.Upload(ctx, dir)
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
