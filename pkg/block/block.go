// Package block contains common functionality for interacting with TSDB blocks
// in the context of Thanos.
package block

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/fileutil"
)

const (
	// MetaFilename is the known JSON filename for meta information.
	MetaFilename = "meta.json"
	// IndexFilename is the known index file for block index.
	IndexFilename = "index"
	// ChunksDirname is the known dir name for chunks with compressed samples.
	ChunksDirname = "chunks"

	// DebugMetas is a directory for debug meta files that happen in the past. Useful for debugging.
	DebugMetas = "debug/metas"
)

type SourceType string

const (
	UnknownSource         SourceType = ""
	SidecarSource         SourceType = "sidecar"
	CompactorSource       SourceType = "compactor"
	CompactorRepairSource SourceType = "compactor.repair"
	RulerSource           SourceType = "ruler"
	BucketRepairSource    SourceType = "bucket.repair"
	TestSource            SourceType = "test"
)

// Meta describes the a block's meta. It wraps the known TSDB meta structure and
// extends it by Thanos-specific fields.
type Meta struct {
	Version int `json:"version"`

	tsdb.BlockMeta

	Thanos ThanosMeta `json:"thanos"`
}

// ThanosMeta holds block meta information specific to Thanos.
type ThanosMeta struct {
	Labels     map[string]string    `json:"labels"`
	Downsample ThanosDownsampleMeta `json:"downsample"`

	// Source is a real upload source of the block.
	Source SourceType `json:"source"`
}

type ThanosDownsampleMeta struct {
	Resolution int64 `json:"resolution"`
}

// WriteMetaFile writes the given meta into <dir>/meta.json.
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
		runutil.CloseWithLogOnErr(logger, f, "close meta")
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return renameFile(logger, tmp, path)
}

// ReadMetaFile reads the given meta from <dir>/meta.json.
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
		runutil.CloseWithLogOnErr(logger, pdir, "close dir")
		return err
	}
	return pdir.Close()
}

// Download downloads directory that is mean to be block directory.
func Download(ctx context.Context, logger log.Logger, bucket objstore.Bucket, id ulid.ULID, dst string) error {
	if err := objstore.DownloadDir(ctx, logger, bucket, id.String(), dst); err != nil {
		return err
	}

	chunksDir := filepath.Join(dst, ChunksDirname)
	_, err := os.Stat(chunksDir)
	if os.IsNotExist(err) {
		// This can happen if block is empty. We cannot easily upload empty directory, so create one here.
		return os.Mkdir(chunksDir, os.ModePerm)
	}

	if err != nil {
		return errors.Wrapf(err, "stat %s", chunksDir)
	}

	return nil
}

// Upload uploads block from given block dir that ends with block id.
// It makes sure cleanup is done on error to avoid partial block uploads.
// It also verifies basic features of Thanos block.
// TODO(bplotka): Ensure bucket operations have reasonable backoff retries.
func Upload(ctx context.Context, logger log.Logger, bkt objstore.Bucket, bdir string) error {
	df, err := os.Stat(bdir)
	if err != nil {
		return errors.Wrap(err, "stat bdir")
	}
	if !df.IsDir() {
		return errors.Errorf("%s is not a directory", bdir)
	}

	// Verify dir.
	id, err := ulid.Parse(df.Name())
	if err != nil {
		return errors.Wrap(err, "not a block dir")
	}

	meta, err := ReadMetaFile(bdir)
	if err != nil {
		// No meta or broken meta file.
		return errors.Wrap(err, "read meta")
	}

	if meta.Thanos.Labels == nil || len(meta.Thanos.Labels) == 0 {
		return errors.Errorf("empty external labels are not allowed for Thanos block.")
	}

	if err := objstore.UploadFile(ctx, logger, bkt, path.Join(bdir, MetaFilename), path.Join(DebugMetas, fmt.Sprintf("%s.json", id))); err != nil {
		return errors.Wrap(err, "upload meta file to debug dir")
	}

	if err := objstore.UploadDir(ctx, logger, bkt, path.Join(bdir, ChunksDirname), path.Join(id.String(), ChunksDirname)); err != nil {
		return cleanUp(bkt, id, errors.Wrap(err, "upload chunks"))
	}

	if err := objstore.UploadFile(ctx, logger, bkt, path.Join(bdir, IndexFilename), path.Join(id.String(), IndexFilename)); err != nil {
		return cleanUp(bkt, id, errors.Wrap(err, "upload index"))
	}

	// Meta.json always need to be uploaded as a last item. This will allow to assume block directories without meta file
	// to be pending uploads.
	if err := objstore.UploadFile(ctx, logger, bkt, path.Join(bdir, MetaFilename), path.Join(id.String(), MetaFilename)); err != nil {
		return cleanUp(bkt, id, errors.Wrap(err, "upload meta file"))
	}

	return nil
}

func cleanUp(bkt objstore.Bucket, id ulid.ULID, err error) error {
	// Cleanup the dir with an uncancelable context.
	cleanErr := Delete(context.Background(), bkt, id)
	if cleanErr != nil {
		return errors.Wrapf(err, "failed to clean block after upload issue. Partial block in system. Err: %s", err.Error())
	}
	return err
}

// Delete removes directory that is mean to be block directory.
// NOTE: Prefer this method instead of objstore.Delete to avoid deleting empty dir (whole bucket) by mistake.
func Delete(ctx context.Context, bucket objstore.Bucket, id ulid.ULID) error {
	return objstore.DeleteDir(ctx, bucket, id.String())
}

// DownloadMeta downloads only meta file from bucket by block ID.
func DownloadMeta(ctx context.Context, logger log.Logger, bkt objstore.Bucket, id ulid.ULID) (Meta, error) {
	rc, err := bkt.Get(ctx, path.Join(id.String(), MetaFilename))
	if err != nil {
		return Meta{}, errors.Wrapf(err, "meta.json bkt get for %s", id.String())
	}
	defer runutil.CloseWithLogOnErr(logger, rc, "download meta bucket client")

	var m Meta
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return Meta{}, errors.Wrapf(err, "decode meta.json for block %s", id.String())
	}
	return m, nil
}

func IsBlockDir(path string) (id ulid.ULID, ok bool) {
	id, err := ulid.Parse(filepath.Base(path))
	return id, err == nil
}

// InjectThanosMeta sets Thanos meta to the block meta JSON and saves it to the disk.
// NOTE: It should be used after writing any block by any Thanos component, otherwise we will miss crucial metadata.
func InjectThanosMeta(logger log.Logger, bdir string, meta ThanosMeta, downsampledMeta *tsdb.BlockMeta) (*Meta, error) {
	newMeta, err := ReadMetaFile(bdir)
	if err != nil {
		return nil, errors.Wrap(err, "read new meta")
	}
	newMeta.Thanos = meta

	// While downsampling we need to copy original compaction.
	if downsampledMeta != nil {
		newMeta.Compaction = downsampledMeta.Compaction
	}

	if err := WriteMetaFile(logger, bdir, newMeta); err != nil {
		return nil, errors.Wrap(err, "write new meta")
	}

	return newMeta, nil
}
