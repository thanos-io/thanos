package main

import (
	"context"
	"net"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/improbable-eng/thanos/pkg/compact"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/query/ui"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/tsdb"
	"gopkg.in/alecthomas/kingpin.v2"
)

func registerCompact(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "continously compacts blocks in an object store bucket")

	httpAddr := cmd.Flag("http-address", "listen host:port for HTTP endpoints").
		Default(defaultHTTPAddr).String()

	dataDir := cmd.Flag("tsdb.path", "data directory of TSDB").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks.").
		PlaceHolder("<bucket>").Required().String()

	// deleteOld := cmd.Flag("delete-old", "delete compacted blocks from the bucket").Bool()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer) error {
		return runCompact(g, logger, reg, *httpAddr, *dataDir, *gcsBucket)
	}
}

func runCompact(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	httpAddr string,
	dataDir string,
	gcsBucket string,
) error {

	gcsClient, err := storage.NewClient(context.Background())
	if err != nil {
		return errors.Wrap(err, "create GCS client")
	}
	bkt := gcs.NewBucket(gcsClient.Bucket(gcsBucket), reg, gcsBucket)

	sy, err := compact.NewSyncer(logger, dataDir, bkt)
	if err != nil {
		return err
	}

	{
		comp, err := tsdb.NewLeveledCompactor(reg, logger, []int64{
			2 * 3600 * 1000,
			8 * 3600 * 1000,
			2 * 24 * 3600 * 1000,
			14 * 24 * 3600 * 1000,
		}, nil)
		if err != nil {
			return errors.Wrap(err, "create compactor")
		}

		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				for _, g := range sy.Groups() {
					if err := g.Compact(ctx, comp); err != nil {
						level.Error(logger).Log("msg", "compaction failed", "err", err)
					}
				}
				return nil
			})
		}, func(error) {
			cancel()
		})
	}
	{
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				if err := sy.SyncMetas(ctx); err != nil {
					level.Error(logger).Log("msg", "sync failed", "err", err)
				}
				return nil
			})
		}, func(error) {
			cancel()
		})
	}
	{
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				if err := sy.GarbageCollect(ctx, bkt); err != nil {
					level.Error(logger).Log("msg", "garbage collection failed", "err", err)
				}
				return nil
			})
		}, func(error) {
			cancel()
		})
	}
	// Start query API + UI HTTP server.
	{
		router := route.New()
		ui.New(logger, nil).Register(router)

		mux := http.NewServeMux()
		registerMetrics(mux, reg)
		registerProfile(mux)
		mux.Handle("/", router)

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

	level.Info(logger).Log("msg", "starting query node")
	return nil
}
