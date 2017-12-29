package main

import (
	"context"
	"math"
	"net"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
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

	grpcAddr := cmd.Flag("grpc-address", "listen address for gRPC endpoints").
		Default(defaultGRPCAddr).String()

	httpAddr := cmd.Flag("http-address", "listen address for HTTP endpoints").
		Default(defaultHTTPAddr).String()

	dataDir := cmd.Flag("tsdb.path", "data directory of TSDB").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks. If empty sidecar won't store any block inside Google Cloud Storage").
		PlaceHolder("<bucket>").Required().String()

	indexCacheSize := cmd.Flag("index-cache-size", "Maximum size of items held in the index cache.").
		Default("250MB").Bytes()

	chunkPoolSize := cmd.Flag("chunk-pool-size", "Maximum size of concurrently allocatble bytes for chunks.").
		Default("2GB").Bytes()

	peers := cmd.Flag("cluster.peers", "initial peers to join the cluster. It can be either <ip:port>, or <domain:port>").Strings()

	clusterBindAddr := cmd.Flag("cluster.address", "listen address for clutser").
		Default(defaultClusterAddr).String()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "explicit address to advertise in cluster").
		String()

	gossipInterval := cmd.Flag("cluster.gossip-interval", "interval between sending gossip messages. By lowering this value (more frequent) gossip messages are propagated across the cluster more quickly at the expense of increased bandwidth.").
		Default(cluster.DefaultGossipInterval.String()).Duration()

	pushPullInterval := cmd.Flag("cluster.pushpull-interval", "interval for gossip state syncs . Setting this interval lower (more frequent) will increase convergence speeds across larger clusters at the expense of increased bandwidth usage.").
		Default(cluster.DefaultPushPullInterval.String()).Duration()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer) error {
		pstate := cluster.PeerState{
			Type:    cluster.PeerTypeStore,
			APIAddr: *grpcAddr,
			Metadata: cluster.PeerMetadata{
				MinTime: math.MinInt64,
				MaxTime: math.MaxInt64,
			},
		}
		p, err := cluster.Join(
			logger,
			reg,
			*clusterBindAddr,
			*clusterAdvertiseAddr,
			*peers,
			pstate,
			false,
			*gossipInterval,
			*pushPullInterval,
		)
		if err != nil {
			return errors.Wrap(err, "join cluster")
		}
		return runStore(g,
			logger,
			reg,
			tracer,
			*gcsBucket,
			*dataDir,
			*grpcAddr,
			*httpAddr,
			p,
			uint64(*indexCacheSize),
			uint64(*chunkPoolSize),
		)
	}
}

// runStore starts a daemon that connects to a cluster of other store nodes through gossip.
// It also connects to a Google Cloud Storage bucket and serves data queries to a subset of its contents.
// The served subset is determined through HRW hashing against the block's ULIDs and the known peers.
func runStore(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	gcsBucket string,
	dataDir string,
	grpcAddr string,
	httpAddr string,
	peer *cluster.Peer,
	indexCacheSizeBytes uint64,
	chunkPoolSizeBytes uint64,
) error {
	{
		gcsClient, err := storage.NewClient(context.Background())
		if err != nil {
			return errors.Wrap(err, "create GCS client")
		}

		var bkt objstore.Bucket
		bkt = gcs.NewBucket(gcsBucket, gcsClient.Bucket(gcsBucket), reg)
		bkt = objstore.BucketWithMetrics(gcsBucket, bkt, reg)

		gs, err := store.NewBucketStore(
			logger,
			reg,
			bkt,
			dataDir,
			indexCacheSizeBytes,
			chunkPoolSizeBytes,
		)
		if err != nil {
			return errors.Wrap(err, "create GCS store")
		}
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			err := runutil.Repeat(3*time.Minute, ctx.Done(), func() error {
				if err := gs.SyncBlocks(ctx); err != nil {
					level.Warn(logger).Log("msg", "syncing blocks failed", "err", err)
				}
				peer.SetTimestamps(gs.TimeRange())
				return nil
			})

			gs.Close()
			gcsClient.Close()

			return err
		}, func(error) {
			cancel()
		})

		l, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			return errors.Wrap(err, "listen API address")
		}

		s := grpc.NewServer(defaultGRPCServerOpts(logger, reg, tracer)...)
		storepb.RegisterStoreServer(s, gs)

		g.Add(func() error {
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			l.Close()
		})
	}
	{
		mux := http.NewServeMux()
		registerMetrics(mux, reg)
		registerProfile(mux)

		l, err := net.Listen("tcp", httpAddr)
		if err != nil {
			return errors.Wrap(err, "listen metrics address")
		}

		g.Add(func() error {
			return errors.Wrap(http.Serve(l, mux), "serve metrics")
		}, func(error) {
			l.Close()
		})
	}

	level.Info(logger).Log("msg", "starting store node")
	return nil
}
