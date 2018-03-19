package main

import (
	"context"
	"math"
	"net"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
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

	grpcAddr := cmd.Flag("grpc-address", "listen address for gRPC endpoints").
		Default(defaultGRPCAddr).String()

	httpAddr := cmd.Flag("http-address", "listen address for HTTP endpoints").
		Default(defaultHTTPAddr).String()

	dataDir := cmd.Flag("tsdb.path", "data directory of TSDB").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks. If empty sidecar won't store any block inside Google Cloud Storage").
		PlaceHolder("<bucket>").String()

	s3Bucket := cmd.Flag("s3.bucket", "S3-Compatible API bucket name for stored blocks.").
		PlaceHolder("<bucket>").Envar("S3_BUCKET").String()

	s3Endpoint := cmd.Flag("s3.endpoint", "S3-Compatible API endpoint for stored blocks.").
		PlaceHolder("<api-url>").Envar("S3_ENDPOINT").String()

	s3AccessKey := cmd.Flag("s3.access-key", "Access key for an S3-Compatible API.").
		PlaceHolder("<key>").Envar("S3_ACCESS_KEY").String()

	s3SecretKey := os.Getenv("S3_SECRET_KEY")

	s3Insecure := cmd.Flag("s3.insecure", "Whether to use an insecure connection with an S3-Compatible API.").
		Default("false").Envar("S3_INSECURE").Bool()

	s3SignatureV2 := cmd.Flag("s3.signature-version2", "Whether to use S3 Signature Version 2; otherwise Signature Version 4 will be used.").
		Default("false").Envar("S3_SIGNATURE_VERSION2").Bool()

	indexCacheSize := cmd.Flag("index-cache-size", "Maximum size of items held in the index cache.").
		Default("250MB").Bytes()

	chunkPoolSize := cmd.Flag("chunk-pool-size", "Maximum size of concurrently allocatable bytes for chunks.").
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
		peer, err := cluster.New(logger, reg, *clusterBindAddr, *clusterAdvertiseAddr, *peers, false, *gossipInterval, *pushPullInterval)
		if err != nil {
			return errors.Wrap(err, "new cluster peer")
		}
		return runStore(g,
			logger,
			reg,
			tracer,
			*gcsBucket,
			*s3Bucket,
			*s3Endpoint,
			*s3AccessKey,
			s3SecretKey,
			*s3Insecure,
			*s3SignatureV2,
			*dataDir,
			*grpcAddr,
			*httpAddr,
			peer,
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
	s3Bucket string,
	s3Endpoint string,
	s3AccessKey string,
	s3SecretKey string,
	s3Insecure bool,
	s3SignatureV2 bool,
	dataDir string,
	grpcAddr string,
	httpAddr string,
	peer *cluster.Peer,
	indexCacheSizeBytes uint64,
	chunkPoolSizeBytes uint64,
) error {
	{
		var (
			bkt objstore.Bucket
			// closeFn gets called when the sync loop ends to close clients, clean up, etc
			closeFn = func() error { return nil }
			bucket  string
		)

		s3Config := &s3.Config{
			Bucket:      s3Bucket,
			Endpoint:    s3Endpoint,
			AccessKey:   s3AccessKey,
			SecretKey:   s3SecretKey,
			Insecure:    s3Insecure,
			SignatureV2: s3SignatureV2,
		}

		if gcsBucket != "" {
			gcsClient, err := storage.NewClient(context.Background())
			if err != nil {
				return errors.Wrap(err, "create GCS client")
			}

			bkt = gcs.NewBucket(gcsBucket, gcsClient.Bucket(gcsBucket), reg)
			closeFn = gcsClient.Close
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

		bs, err := store.NewBucketStore(
			logger,
			reg,
			bkt,
			dataDir,
			indexCacheSizeBytes,
			chunkPoolSizeBytes,
		)
		if err != nil {
			return errors.Wrap(err, "create object storage store")
		}
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			err := runutil.Repeat(3*time.Minute, ctx.Done(), func() error {
				if err := bs.SyncBlocks(ctx); err != nil {
					level.Warn(logger).Log("msg", "syncing blocks failed", "err", err)
				}
				peer.SetTimestamps(bs.TimeRange())
				return nil
			})

			bs.Close()
			closeFn()

			return err
		}, func(error) {
			cancel()
		})

		l, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			return errors.Wrap(err, "listen API address")
		}

		s := grpc.NewServer(defaultGRPCServerOpts(logger, reg, tracer)...)
		storepb.RegisterStoreServer(s, bs)

		g.Add(func() error {
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			l.Close()
		})
	}
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			err := peer.Join(cluster.PeerState{
				Type:    cluster.PeerTypeStore,
				APIAddr: grpcAddr,
				Metadata: cluster.PeerMetadata{
					MinTime: math.MinInt64,
					MaxTime: math.MaxInt64,
				},
			})
			if err != nil {
				return errors.Wrap(err, "join cluster")
			}

			<-ctx.Done()
			return nil
		}, func(error) {
			cancel()
			peer.Close(5 * time.Second)
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
