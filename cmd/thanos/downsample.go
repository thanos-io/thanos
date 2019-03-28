package main

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/block/metadata"
	"github.com/improbable-eng/thanos/pkg/compact/downsample"
	"github.com/improbable-eng/thanos/pkg/component"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/client"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunkenc"
	"gopkg.in/alecthomas/kingpin.v2"
)

func registerDownsample(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "continuously downsamples blocks in an object store bucket")

	dataDir := cmd.Flag("data-dir", "Data directory in which to cache blocks and process downsamplings.").
		Default("./data").String()

	objStoreConfig := regCommonObjStoreFlags(cmd, "", true)

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		return runDownsample(g, logger, reg, *dataDir, objStoreConfig)
	}
}

func runDownsample(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	dataDir string,
	objStoreConfig *pathOrContent,
) error {
	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}

	bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Downsample.String())
	if err != nil {
		return err
	}

	// Ensure we close up everything properly.
	defer func() {
		if err != nil {
			runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
		}
	}()

	// Start cycle of syncing blocks from the bucket and garbage collecting the bucket.
	{
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

			level.Info(logger).Log("msg", "start first pass of downsampling")

			if err := downsampleBucket(ctx, logger, bkt, dataDir); err != nil {
				return errors.Wrap(err, "downsampling failed")
			}

			level.Info(logger).Log("msg", "start second pass of downsampling")

			if err := downsampleBucket(ctx, logger, bkt, dataDir); err != nil {
				return errors.Wrap(err, "downsampling failed")
			}

			return nil
		}, func(error) {
			cancel()
		})
	}

	level.Info(logger).Log("msg", "starting downsample node")
	return nil
}

func downsampleBucket(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.Bucket,
	dir string,
) error {
	if err := os.RemoveAll(dir); err != nil {
		return errors.Wrap(err, "clean working directory")
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		return errors.Wrap(err, "create dir")
	}
	var metas []*metadata.Meta

	err := bkt.Iter(ctx, "", func(name string) error {
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}

		rc, err := bkt.Get(ctx, path.Join(id.String(), block.MetaFilename))
		if err != nil {
			return errors.Wrapf(err, "get meta for block %s", id)
		}
		defer runutil.CloseWithLogOnErr(logger, rc, "block reader")

		var m metadata.Meta
		if err := json.NewDecoder(rc).Decode(&m); err != nil {
			return errors.Wrap(err, "decode meta")
		}
		metas = append(metas, &m)

		return nil
	})
	if err != nil {
		return errors.Wrap(err, "retrieve bucket block metas")
	}

	// mapping from a hash over all source IDs to blocks. We don't need to downsample a block
	// if a downsampled version with the same hash already exists.
	sources5m := map[ulid.ULID]struct{}{}
	sources1h := map[ulid.ULID]struct{}{}

	for _, m := range metas {
		switch m.Thanos.Downsample.Resolution {
		case 0:
			continue
		case 5 * 60 * 1000:
			for _, id := range m.Compaction.Sources {
				sources5m[id] = struct{}{}
			}
		case 60 * 60 * 1000:
			for _, id := range m.Compaction.Sources {
				sources1h[id] = struct{}{}
			}
		default:
			return errors.Errorf("unexpected downsampling resolution %d", m.Thanos.Downsample.Resolution)
		}
	}

	for _, m := range metas {
		switch m.Thanos.Downsample.Resolution {
		case 0:
			missing := false
			for _, id := range m.Compaction.Sources {
				if _, ok := sources5m[id]; !ok {
					missing = true
					break
				}
			}
			if !missing {
				continue
			}
			// Only downsample blocks once we are sure to get roughly 2 chunks out of it.
			// NOTE(fabxc): this must match with at which block size the compactor creates downsampled
			// blocks. Otherwise we may never downsample some data.
			if m.MaxTime-m.MinTime < 40*60*60*1000 {
				continue
			}
			if err := processDownsampling(ctx, logger, bkt, m, dir, 5*60*1000); err != nil {
				return errors.Wrap(err, "downsampling to 5 min")
			}

		case 5 * 60 * 1000:
			missing := false
			for _, id := range m.Compaction.Sources {
				if _, ok := sources1h[id]; !ok {
					missing = true
					break
				}
			}
			if !missing {
				continue
			}
			// Only downsample blocks once we are sure to get roughly 2 chunks out of it.
			// NOTE(fabxc): this must match with at which block size the compactor creates downsampled
			// blocks. Otherwise we may never downsample some data.
			if m.MaxTime-m.MinTime < 10*24*60*60*1000 {
				continue
			}
			if err := processDownsampling(ctx, logger, bkt, m, dir, 60*60*1000); err != nil {
				return errors.Wrap(err, "downsampling to 60 min")
			}
		}
	}
	return nil
}

func processDownsampling(ctx context.Context, logger log.Logger, bkt objstore.Bucket, m *metadata.Meta, dir string, resolution int64) error {
	begin := time.Now()
	bdir := filepath.Join(dir, m.ULID.String())

	err := block.Download(ctx, logger, bkt, m.ULID, bdir)
	if err != nil {
		return errors.Wrapf(err, "download block %s", m.ULID)
	}
	level.Info(logger).Log("msg", "downloaded block", "id", m.ULID, "duration", time.Since(begin))

	if err := block.VerifyIndex(logger, filepath.Join(bdir, block.IndexFilename), m.MinTime, m.MaxTime); err != nil {
		return errors.Wrap(err, "input block index not valid")
	}

	begin = time.Now()

	var pool chunkenc.Pool
	if m.Thanos.Downsample.Resolution == 0 {
		pool = chunkenc.NewPool()
	} else {
		pool = downsample.NewPool()
	}

	b, err := tsdb.OpenBlock(logger, bdir, pool)
	if err != nil {
		return errors.Wrapf(err, "open block %s", m.ULID)
	}
	defer runutil.CloseWithLogOnErr(log.With(logger, "outcome", "potential left mmap file handlers left"), b, "tsdb reader")

	id, err := downsample.Downsample(logger, m, b, dir, resolution)
	if err != nil {
		return errors.Wrapf(err, "downsample block %s to window %d", m.ULID, resolution)
	}
	resdir := filepath.Join(dir, id.String())

	level.Info(logger).Log("msg", "downsampled block",
		"from", m.ULID, "to", id, "duration", time.Since(begin))

	if err := block.VerifyIndex(logger, filepath.Join(resdir, block.IndexFilename), m.MinTime, m.MaxTime); err != nil {
		return errors.Wrap(err, "output block index not valid")
	}

	begin = time.Now()

	err = block.Upload(ctx, logger, bkt, resdir)
	if err != nil {
		return errors.Wrapf(err, "upload downsampled block %s", id)
	}

	level.Info(logger).Log("msg", "uploaded block", "id", id, "duration", time.Since(begin))

	begin = time.Now()

	// It is not harmful if these fails.
	if err := os.RemoveAll(bdir); err != nil {
		level.Warn(logger).Log("msg", "failed to clean directory", "dir", bdir, "err", err)
	}
	if err := os.RemoveAll(resdir); err != nil {
		level.Warn(logger).Log("msg", "failed to clean directory", "resdir", bdir, "err", err)
	}

	return nil
}
