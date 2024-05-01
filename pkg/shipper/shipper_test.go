// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package shipper

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/objstore"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func TestShipperTimestamps(t *testing.T) {
	dir := t.TempDir()

	s := New(nil, nil, dir, nil, nil, metadata.TestSource, nil, false, metadata.NoneFunc, DefaultMetaFilename)

	// Missing thanos meta file.
	_, _, err := s.Timestamps()
	testutil.NotOk(t, err)

	meta := &Meta{Version: MetaVersion1}
	testutil.Ok(t, WriteMetaFile(log.NewNopLogger(), s.metadataFilePath, meta))

	// Nothing uploaded, nothing in the filesystem. We assume that
	// we are still waiting for TSDB to dump first TSDB block.
	mint, maxt, err := s.Timestamps()
	testutil.Ok(t, err)
	testutil.Equals(t, int64(0), mint)
	testutil.Equals(t, int64(math.MinInt64), maxt)

	id1 := ulid.MustNew(1, nil)
	testutil.Ok(t, os.Mkdir(path.Join(dir, id1.String()), os.ModePerm))
	testutil.Ok(t, metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id1,
			MaxTime: 2000,
			MinTime: 1000,
			Version: 1,
		},
	}.WriteToDir(log.NewNopLogger(), path.Join(dir, id1.String())))
	mint, maxt, err = s.Timestamps()
	testutil.Ok(t, err)
	testutil.Equals(t, int64(1000), mint)
	testutil.Equals(t, int64(math.MinInt64), maxt)

	id2 := ulid.MustNew(2, nil)
	testutil.Ok(t, os.Mkdir(path.Join(dir, id2.String()), os.ModePerm))
	testutil.Ok(t, metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id2,
			MaxTime: 4000,
			MinTime: 2000,
			Version: 1,
		},
	}.WriteToDir(log.NewNopLogger(), path.Join(dir, id2.String())))
	mint, maxt, err = s.Timestamps()
	testutil.Ok(t, err)
	testutil.Equals(t, int64(1000), mint)
	testutil.Equals(t, int64(math.MinInt64), maxt)

	meta = &Meta{
		Version:  MetaVersion1,
		Uploaded: []ulid.ULID{id1},
	}
	testutil.Ok(t, WriteMetaFile(log.NewNopLogger(), s.metadataFilePath, meta))
	mint, maxt, err = s.Timestamps()
	testutil.Ok(t, err)
	testutil.Equals(t, int64(1000), mint)
	testutil.Equals(t, int64(2000), maxt)
}

func TestIterBlockMetas(t *testing.T) {
	dir := t.TempDir()

	id1 := ulid.MustNew(1, nil)
	testutil.Ok(t, os.Mkdir(path.Join(dir, id1.String()), os.ModePerm))
	testutil.Ok(t, metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id1,
			MaxTime: 2000,
			MinTime: 1000,
			Version: 1,
		},
	}.WriteToDir(log.NewNopLogger(), path.Join(dir, id1.String())))

	id2 := ulid.MustNew(2, nil)
	testutil.Ok(t, os.Mkdir(path.Join(dir, id2.String()), os.ModePerm))
	testutil.Ok(t, metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id2,
			MaxTime: 5000,
			MinTime: 4000,
			Version: 1,
		},
	}.WriteToDir(log.NewNopLogger(), path.Join(dir, id2.String())))

	id3 := ulid.MustNew(3, nil)
	testutil.Ok(t, os.Mkdir(path.Join(dir, id3.String()), os.ModePerm))
	testutil.Ok(t, metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id3,
			MaxTime: 3000,
			MinTime: 2000,
			Version: 1,
		},
	}.WriteToDir(log.NewNopLogger(), path.Join(dir, id3.String())))

	shipper := New(nil, nil, dir, nil, nil, metadata.TestSource, nil, false, metadata.NoneFunc, DefaultMetaFilename)
	metas, err := shipper.blockMetasFromOldest()
	testutil.Ok(t, err)
	testutil.Equals(t, sort.SliceIsSorted(metas, func(i, j int) bool {
		return metas[i].BlockMeta.MinTime < metas[j].BlockMeta.MinTime
	}), true)
}

func BenchmarkIterBlockMetas(b *testing.B) {
	var metas []*metadata.Meta
	dir := b.TempDir()

	for i := 0; i < 100; i++ {
		id := ulid.MustNew(uint64(i), nil)
		testutil.Ok(b, os.Mkdir(path.Join(dir, id.String()), os.ModePerm))
		testutil.Ok(b,
			metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    id,
					MaxTime: int64((i + 1) * 1000),
					MinTime: int64(i * 1000),
					Version: 1,
				},
			}.WriteToDir(log.NewNopLogger(), path.Join(dir, id.String())),
		)
	}
	rand.Shuffle(len(metas), func(i, j int) {
		metas[i], metas[j] = metas[j], metas[i]
	})
	b.ResetTimer()

	shipper := New(nil, nil, dir, nil, nil, metadata.TestSource, nil, false, metadata.NoneFunc, DefaultMetaFilename)

	_, err := shipper.blockMetasFromOldest()
	testutil.Ok(b, err)
}

func TestShipperAddsSegmentFiles(t *testing.T) {
	dir := t.TempDir()

	inmemory := objstore.NewInMemBucket()

	lbls := labels.FromStrings("test", "test")
	s := New(nil, nil, dir, inmemory, func() labels.Labels { return lbls }, metadata.TestSource, nil, false, metadata.NoneFunc, DefaultMetaFilename)

	id := ulid.MustNew(1, nil)
	blockDir := path.Join(dir, id.String())
	chunksDir := path.Join(blockDir, block.ChunksDirname)
	testutil.Ok(t, os.MkdirAll(chunksDir, os.ModePerm))

	// Prepare minimal "block" for shipper (meta.json, index, one segment file).
	testutil.Ok(t, metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id,
			MaxTime: 2000,
			MinTime: 1000,
			Version: 1,
			Stats: tsdb.BlockStats{
				NumSamples: 1000, // Not really, but shipper needs nonzero value.
			},
		},
	}.WriteToDir(log.NewNopLogger(), path.Join(dir, id.String())))
	testutil.Ok(t, os.WriteFile(filepath.Join(blockDir, "index"), []byte("index file"), 0666))
	segmentFile := "00001"
	testutil.Ok(t, os.WriteFile(filepath.Join(chunksDir, segmentFile), []byte("hello world"), 0666))

	uploaded, err := s.Sync(context.Background())
	testutil.Ok(t, err)
	testutil.Equals(t, 1, uploaded)

	meta, err := block.DownloadMeta(context.Background(), log.NewNopLogger(), inmemory, id)
	testutil.Ok(t, err)

	testutil.Equals(t, []string{segmentFile}, meta.Thanos.SegmentFiles)
}

func TestReadMetaFile(t *testing.T) {
	t.Run("Missing meta file", func(t *testing.T) {
		// Create TSDB directory without meta file
		dpath := t.TempDir()
		fpath := filepath.Join(dpath, DefaultMetaFilename)

		_, err := ReadMetaFile(fpath)
		testutil.Equals(t, fmt.Sprintf(`failed to read %s: open %s: no such file or directory`, fpath, fpath), err.Error())
	})

	t.Run("Non-JSON meta file", func(t *testing.T) {
		dpath := t.TempDir()
		fpath := filepath.Join(dpath, DefaultMetaFilename)

		// Make an invalid JSON file
		testutil.Ok(t, os.WriteFile(fpath, []byte("{"), 0600))

		_, err := ReadMetaFile(fpath)
		testutil.Equals(t, fmt.Sprintf(`failed to parse %s as JSON: "{": unexpected end of JSON input`, fpath), err.Error())
	})

	t.Run("Wrongly versioned meta file", func(t *testing.T) {
		dpath := t.TempDir()
		fpath := filepath.Join(dpath, DefaultMetaFilename)
		testutil.Ok(t, os.WriteFile(fpath, []byte(`{"version": 2}`), 0600))

		_, err := ReadMetaFile(fpath)
		testutil.Equals(t, "unexpected meta file version 2", err.Error())
	})
}

func TestShipperExistingThanosLabels(t *testing.T) {
	dir := t.TempDir()

	inmemory := objstore.NewInMemBucket()

	lbls := labels.FromStrings("test", "test")
	s := New(nil, nil, dir, inmemory, func() labels.Labels { return lbls }, metadata.TestSource, nil, false, metadata.NoneFunc, DefaultMetaFilename)

	id := ulid.MustNew(1, nil)
	id2 := ulid.MustNew(2, nil)
	blockDir := path.Join(dir, id.String())
	chunksDir := path.Join(blockDir, block.ChunksDirname)
	testutil.Ok(t, os.MkdirAll(chunksDir, os.ModePerm))
	blockDir2 := path.Join(dir, id2.String())
	chunksDir2 := path.Join(blockDir2, block.ChunksDirname)
	testutil.Ok(t, os.MkdirAll(chunksDir2, os.ModePerm))

	// Prepare meta.json with Thanos labels.
	testutil.Ok(t, metadata.Meta{
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				"cluster": "us-west-2",
			},
		},
		BlockMeta: tsdb.BlockMeta{
			ULID:    id,
			MaxTime: 2000,
			MinTime: 1000,
			Version: 1,
			Stats: tsdb.BlockStats{
				NumSamples: 1000, // Not really, but shipper needs nonzero value.
			},
		},
	}.WriteToDir(log.NewNopLogger(), path.Join(dir, id.String())))
	// Prepare a meta.json with the same label name set.
	testutil.Ok(t, metadata.Meta{
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				"test":    "aaa",
				"cluster": "us-east-1",
			},
		},
		BlockMeta: tsdb.BlockMeta{
			ULID:    id2,
			MaxTime: 2000,
			MinTime: 1000,
			Version: 1,
			Stats: tsdb.BlockStats{
				NumSamples: 1000, // Not really, but shipper needs nonzero value.
			},
		},
	}.WriteToDir(log.NewNopLogger(), path.Join(dir, id2.String())))
	testutil.Ok(t, os.WriteFile(filepath.Join(blockDir, "index"), []byte("index file"), 0666))
	testutil.Ok(t, os.WriteFile(filepath.Join(blockDir2, "index"), []byte("index file"), 0666))

	uploaded, err := s.Sync(context.Background())
	testutil.Ok(t, err)
	testutil.Equals(t, 2, uploaded)

	meta, err := block.DownloadMeta(context.Background(), log.NewNopLogger(), inmemory, id)
	testutil.Ok(t, err)
	testutil.Equals(t, map[string]string{"cluster": "us-west-2", "test": "test"}, meta.Thanos.Labels)

	meta, err = block.DownloadMeta(context.Background(), log.NewNopLogger(), inmemory, id2)
	testutil.Ok(t, err)
	testutil.Equals(t, map[string]string{"cluster": "us-east-1", "test": "test"}, meta.Thanos.Labels)
}
