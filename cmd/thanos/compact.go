package main

import (
	"context"
	"net"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/compact"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
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

	dataDir := cmd.Flag("data-dir", "data directory to cache blocks and process compactions").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks.").
		PlaceHolder("<bucket>").Required().String()

	s3Bucket := cmd.Flag("s3.bucket", "S3-Compatible API bucket name for stored blocks.").
		PlaceHolder("<bucket>").Envar("S3_BUCKET").String()

	s3Endpoint := cmd.Flag("s3.endpoint", "S3-Compatible API endpoint for stored blocks.").
		PlaceHolder("<api-url>").Envar("S3_ENDPOINT").String()

	s3AccessKey := cmd.Flag("s3.access-key", "Access key for an S3-Compatible API.").
		PlaceHolder("<key>").Envar("S3_ACCESS_KEY").String()

	s3SecretKey := cmd.Flag("s3.secret-key", "Secret key for an S3-Compatible API.").
		PlaceHolder("<key>").Envar("S3_SECRET_KEY").String()

	s3Insecure := cmd.Flag("s3.insecure", "Whether to use an insecure connection with an S3-Compatible API.").
		Default("false").Envar("S3_INSECURE").Bool()

	syncDelay := cmd.Flag("sync-delay", "minimum age of blocks before they are being processed.").
		Default("2h").Duration()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer) error {
		return runCompact(g, logger, reg, *httpAddr, *dataDir, *gcsBucket, *s3Bucket, *s3Endpoint, *s3AccessKey, *s3SecretKey, *s3Insecure, *syncDelay)
	}
}

func runCompact(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	httpAddr string,
	dataDir string,
	gcsBucket string,
	s3Bucket string,
	s3Endpoint string,
	s3AccessKey string,
	s3SecretKey string,
	s3Insecure bool,
	syncDelay time.Duration,
) error {
	var (
		bkt    objstore.Bucket
		bucket string
	)

	halted := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_compactor_halted",
		Help: "Set to 1 if the compactor halted due to an unexpected error",
	})
	halted.Set(0)

	reg.MustRegister(halted)

	s3Config := &s3.Config{
		Bucket:    s3Bucket,
		Endpoint:  s3Endpoint,
		AccessKey: s3AccessKey,
		SecretKey: s3SecretKey,
		Insecure:  s3Insecure,
	}

	if gcsBucket != "" {
		gcsClient, err := storage.NewClient(context.Background())
		if err != nil {
			return errors.Wrap(err, "create GCS client")
		}
		bkt = gcs.NewBucket(gcsBucket, gcsClient.Bucket(gcsBucket), reg)
		bucket = gcsBucket
	} else if s3Config.Validate() == nil {
		b, err := s3.NewBucket(s3Config, reg)
		if err != nil {
			return errors.Wrap(err, "create s3 client")
		}

		bkt = b
		bucket = s3Config.Bucket
	} else {
		return errors.New("no valid GCS or S3 configuration supplied")
	}

	bkt = objstore.BucketWithMetrics(bucket, bkt, reg)

	sy, err := compact.NewSyncer(logger, reg, dataDir, bkt, syncDelay)
	if err != nil {
		return err
	}
	// Start cycle of syncing blocks from the bucket and garbage collecting the bucket.
	{
		// Instantiate the compactor with different time slices. Timestamps in TSDB
		// are in milliseconds.
		comp, err := tsdb.NewLeveledCompactor(reg, logger, []int64{
			int64(2 * time.Hour / time.Millisecond),
			int64(8 * time.Hour / time.Millisecond),
			int64(2 * 24 * time.Hour / time.Millisecond),
			int64(14 * 24 * time.Hour / time.Millisecond),
		}, nil)
		if err != nil {
			return errors.Wrap(err, "create compactor")
		}

		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			return runutil.Repeat(5*time.Minute, ctx.Done(), func() error {
				if err := sy.SyncMetas(ctx); err != nil {
					level.Error(logger).Log("msg", "sync failed", "err", err)
				}
				if err := sy.GarbageCollect(ctx); err != nil {
					level.Error(logger).Log("msg", "garbage collection failed", "err", err)
				}
				for _, g := range sy.Groups() {
					if _, err := g.Compact(ctx, comp); err != nil {
						level.Error(logger).Log("msg", "compaction failed", "err", err)
						// The HaltError type signals that we hit a critical bug and should block
						// for investigation.
						if compact.IsHaltError(err) {
							level.Error(logger).Log("msg", "critical error detected; halting")
							halted.Set(1)
							select {}
						}
					}
				}
				return nil
			})
		}, func(error) {
			cancel()
		})
	}
	// Start metric and profiling endpoints.
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

	level.Info(logger).Log("msg", "starting compact node")
	return nil
}
