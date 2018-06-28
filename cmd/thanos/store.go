package main

import (
	"context"
	"math"
	"net"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/objstore/client"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

// registerStore registers a store command.
func registerStore(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "store node giving access to blocks in a GCS bucket")

	grpcBindAddr, httpBindAddr, newPeerFn := regCommonServerFlags(cmd)

	dataDir := cmd.Flag("tsdb.path", "Data directory of TSDB.").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks. If empty sidecar won't store any block inside Google Cloud Storage.").
		PlaceHolder("<bucket>").String()

	s3Config := s3.RegisterS3Params(cmd)

	indexCacheSize := cmd.Flag("index-cache-size", "Maximum size of items held in the index cache.").
		Default("250MB").Bytes()

	chunkPoolSize := cmd.Flag("chunk-pool-size", "Maximum size of concurrently allocatable bytes for chunks.").
		Default("2GB").Bytes()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, debugLogging bool) error {
		peer, err := newPeerFn(logger, reg, false, "", false)
		if err != nil {
			return errors.Wrap(err, "new cluster peer")
		}
		return runStore(g,
			logger,
			reg,
			tracer,
			*gcsBucket,
			s3Config,
			*dataDir,
			*grpcBindAddr,
			*httpBindAddr,
			peer,
			uint64(*indexCacheSize),
			uint64(*chunkPoolSize),
			name,
			debugLogging,
		)
	}
}

// runStore starts a daemon that serves queries to cluster peers using data from an object store.
func runStore(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	gcsBucket string,
	s3Config *s3.Config,
	dataDir string,
	grpcBindAddr string,
	httpBindAddr string,
	peer *cluster.Peer,
	indexCacheSizeBytes uint64,
	chunkPoolSizeBytes uint64,
	component string,
	verbose bool,
) error {
	{
		bkt, err := client.NewBucket(logger, &gcsBucket, *s3Config, reg, component)
		if err != nil {
			return err
		}

		// Ensure we close up everything properly.
		defer func() {
			if err != nil {
				runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
			}
		}()

		bs, err := store.NewBucketStore(
			logger,
			reg,
			bkt,
			dataDir,
			indexCacheSizeBytes,
			chunkPoolSizeBytes,
			verbose,
		)
		if err != nil {
			return errors.Wrap(err, "create object storage store")
		}

		begin := time.Now()
		level.Debug(logger).Log("msg", "initializing bucket store")
		if err := bs.InitialSync(context.Background()); err != nil {
			return errors.Wrap(err, "bucket store initial sync")
		}
		level.Debug(logger).Log("msg", "bucket store ready", "init_duration", time.Since(begin).String())

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

			err := runutil.Repeat(3*time.Minute, ctx.Done(), func() error {
				if err := bs.SyncBlocks(ctx); err != nil {
					level.Warn(logger).Log("msg", "syncing blocks failed", "err", err)
				}
				peer.SetTimestamps(bs.TimeRange())
				return nil
			})

			runutil.CloseWithLogOnErr(logger, bs, "bucket store")
			return err
		}, func(error) {
			cancel()
		})

		l, err := net.Listen("tcp", grpcBindAddr)
		if err != nil {
			return errors.Wrap(err, "listen API address")
		}

		s := grpc.NewServer(defaultGRPCServerOpts(logger, reg, tracer)...)
		storepb.RegisterStoreServer(s, bs)

		g.Add(func() error {
			level.Info(logger).Log("msg", "Listening for StoreAPI gRPC", "address", grpcBindAddr)
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			runutil.CloseWithLogOnErr(logger, l, "store gRPC listener")
		})
	}
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			// New gossip cluster.
			if err := peer.Join(
				cluster.PeerTypeStore,
				cluster.PeerMetadata{
					MinTime: math.MinInt64,
					MaxTime: math.MaxInt64,
				},
			); err != nil {
				return errors.Wrap(err, "join cluster")
			}

			<-ctx.Done()
			return nil
		}, func(error) {
			cancel()
			peer.Close(5 * time.Second)
		})
	}
	if err := metricHTTPListenGroup(g, logger, reg, httpBindAddr); err != nil {
		return err
	}

	level.Info(logger).Log("msg", "starting store node")
	return nil
}
