package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/compact/downsample"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"gopkg.in/alecthomas/kingpin.v2"
)

func registerDownsample(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "continously compacts blocks in an object store bucket")

	httpAddr := cmd.Flag("http-address", "listen host:port for HTTP endpoints").
		Default(defaultHTTPAddr).String()

	dataDir := cmd.Flag("data-dir", "data directory to cache blocks and process compactions").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks.").
		PlaceHolder("<bucket>").Required().String()

	syncDelay := cmd.Flag("sync-delay", "minimum age of blocks before they are being processed.").
		Default("2h").Duration()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer) error {
		return runDownsample(g, logger, reg, *httpAddr, *dataDir, *gcsBucket, *syncDelay)
	}
}

func runDownsample(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	httpAddr string,
	dataDir string,
	gcsBucket string,
	syncDelay time.Duration,
) error {
	gcsClient, err := storage.NewClient(context.Background())
	if err != nil {
		return errors.Wrap(err, "create GCS client")
	}
	var bkt objstore.Bucket
	bkt = gcs.NewBucket(gcsBucket, gcsClient.Bucket(gcsBucket), reg)
	bkt = objstore.BucketWithMetrics(gcsBucket, bkt, reg)

	// Start cycle of syncing blocks from the bucket and garbage collecting the bucket.
	{
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
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
	// Start metric and profiling endpoints.
	{
		mux := http.NewServeMux()
		registerMetrics(mux, reg)
		registerProfile(mux)

		l, err := net.Listen("tcp", httpAddr)
		if err != nil {
			return errors.Wrapf(err, "listen on address %s", httpAddr)
		}

		g.Add(func() error {
			return errors.Wrap(http.Serve(l, mux), "serve query")
		}, func(error) {
			l.Close()
		})
	}

	level.Info(logger).Log("msg", "starting compact node")
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
	var metas []*block.Meta

	err := bkt.Iter(ctx, "", func(name string) error {
		if !strings.HasSuffix(name, "/") {
			return nil
		}
		id, err := ulid.Parse(name[:len(name)-1])
		if err != nil {
			return nil
		}
		rc, err := bkt.Get(ctx, path.Join(id.String(), "meta.json"))
		if err != nil {
			return errors.Wrapf(err, "get meta for block %s", id)
		}
		defer rc.Close()

		var m block.Meta
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
		switch m.Thanos.DownsamplingWindow {
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
			return errors.Errorf("unexpected downsampling window size %d", m.Thanos.DownsamplingWindow)
		}
	}

	for _, m := range metas {
		switch m.Thanos.DownsamplingWindow {
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
			// blockes. Otherwise we may never downsample some data.
			if m.MaxTime-m.MinTime < 48*60*60*1000 {
				continue
			}
			if err := processDownsampling(ctx, logger, bkt, m, dir, 5*60*1000); err != nil {
				return err
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
			// blockes. Otherwise we may never downsample some data.
			if m.MaxTime-m.MinTime < 14*24*60*60*1000 {
				continue
			}
			if err := processDownsampling(ctx, logger, bkt, m, dir, 60*60*1000); err != nil {
				return err
			}
		}
	}
	return nil
}

func processDownsampling(ctx context.Context, logger log.Logger, bkt objstore.Bucket, m *block.Meta, dir string, window int64) error {
	begin := time.Now()
	bdir := filepath.Join(dir, m.ULID.String())

	err := objstore.DownloadDir(ctx, bkt, m.ULID.String(), bdir)
	if err != nil {
		return errors.Wrapf(err, "download block %s", m.ULID)
	}
	level.Info(logger).Log("msg", "downloaded block", "id", m.ULID, "duration", time.Since(begin))

	begin = time.Now()

	b, err := tsdb.OpenBlock(bdir, nil)
	if err != nil {
		return errors.Wrapf(err, "open block %s", m.ULID)
	}

	id, err := downsample.Downsample(ctx, m, b, dir, window)
	if err != nil {
		return errors.Wrapf(err, "downsample block %s", m.ULID, "window", window)
	}

	level.Info(logger).Log("msg", "downsampled block",
		"from", m.ULID, "to", id, "duration", time.Since(begin))

	begin = time.Now()

	err = objstore.UploadDir(ctx, bkt, filepath.Join(dir, id.String()), id.String())
	if err != nil {
		return errors.Wrapf(err, "upload downsampled block %s", id)
	}

	level.Info(logger).Log("msg", "uploaded block", "id", id, "duration", time.Since(begin))

	begin = time.Now()

	os.RemoveAll(bdir)
	os.RemoveAll(filepath.Join(dir, id.String()))

	return nil
}
