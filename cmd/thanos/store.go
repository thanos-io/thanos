package main

import (
	"context"
	"net"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/oklog/run"
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

	peers := cmd.Flag("cluster.peers", "initial peers to join the cluster. It can be either <ip:port>, or <domain:port>").Strings()

	clusterBindAddr := cmd.Flag("cluster.address", "listen address for clutser").
		Default(defaultClusterAddr).String()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "explicit address to advertise in cluster").
		String()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry) error {
		_, err := cluster.Join(
			logger,
			reg,
			*clusterBindAddr,
			*clusterAdvertiseAddr,
			*peers,
			cluster.PeerState{
				Type:    cluster.PeerTypeStore,
				APIAddr: *grpcAddr,
			},
			false,
		)
		if err != nil {
			return errors.Wrap(err, "join cluster")
		}
		return runStore(g, logger, reg, *gcsBucket, *dataDir, *grpcAddr, *httpAddr)
	}
}

// runStore starts a daemon that connects to a cluster of other store nodes through gossip.
// It also connects to a Google Cloud Storage bucket and serves data queries to a subset of its contents.
// The served subset is determined through HRW hashing against the block's ULIDs and the known peers.
func runStore(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	gcsBucket string,
	dataDir string,
	grpcAddr string,
	httpAddr string,
) error {
	{
		gcsClient, err := storage.NewClient(context.Background())
		if err != nil {
			return errors.Wrap(err, "create GCS client")
		}

		gs, err := store.NewGCSStore(logger, reg, gcsClient.Bucket(gcsBucket), dataDir)
		if err != nil {
			return errors.Wrap(err, "create GCS store")
		}
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			gs.SyncBlocks(ctx, 30*time.Second)

			gs.Close()
			gcsClient.Close()

			return nil
		}, func(error) {
			cancel()
		})

		l, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			return errors.Wrap(err, "listen API address")
		}

		met := grpc_prometheus.NewServerMetrics()
		met.EnableHandlingTimeHistogram(
			grpc_prometheus.WithHistogramBuckets([]float64{
				0.001, 0.01, 0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4,
			}),
		)
		s := grpc.NewServer(
			grpc.UnaryInterceptor(met.UnaryServerInterceptor()),
			grpc.StreamInterceptor(met.StreamServerInterceptor()),
		)
		storepb.RegisterStoreServer(s, gs)
		reg.MustRegister(met)

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
