package main

import (
	"context"
	"net"
	"net/http"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
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

	apiAddr := cmd.Flag("api-address", "listen host:port address for the store API").
		Default("0.0.0.0:19090").String()

	metricsAddr := cmd.Flag("metrics-address", "metrics host:port address for the sidecar").
		Default("0.0.0.0:19091").String()

	dataDir := cmd.Flag("tsdb.path", "data directory of TSDB").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks. If empty sidecar won't store any block inside Google Cloud Storage").
		PlaceHolder("<bucket>").Required().String()

	peers := cmd.Flag("cluster.peers", "initial peers to join the cluster").Strings()

	clusterBindAddr := cmd.Flag("cluster.address", "listen address for clutser").
		Default(defaultClusterAddr).String()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "explicit address to advertise in cluster").
		String()

	m[name] = func(g *run.Group, logger log.Logger, metrics *prometheus.Registry) error {
		peer, err := joinCluster(
			logger,
			cluster.PeerTypeStore,
			*clusterBindAddr,
			*clusterAdvertiseAddr,
			*apiAddr,
			*peers,
		)
		if err != nil {
			return errors.Wrap(err, "join cluster")
		}
		return runStore(g, logger, metrics, peer, *gcsBucket, *dataDir, *apiAddr, *metricsAddr)
	}
}

// runStore starts a daemon that connects to a cluster of other store nodes through gossip.
// It also connects to a Google Cloud Storage bucket and serves data queries to a subset of its contents.
// The served subset is determined through HRW hashing against the block's ULIDs and the known peers.
func runStore(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	peer *cluster.Peer,
	gcsBucket string,
	dataDir string,
	apiAddr string,
	metricsAddr string,
) error {
	{
		gcsClient, err := storage.NewClient(context.Background())
		if err != nil {
			return errors.Wrap(err, "create GCS client")
		}

		gs, err := store.NewGCSStore(logger, gcsClient.Bucket(gcsBucket), dataDir)
		if err != nil {
			return errors.Wrap(err, "create GCS store")
		}
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			gs.SyncBlocks(ctx)

			gs.Close()
			gcsClient.Close()

			return nil
		}, func(error) {
			cancel()
		})

		l, err := net.Listen("tcp", apiAddr)
		if err != nil {
			return errors.Wrap(err, "listen API address")
		}
		s := grpc.NewServer()
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

		l, err := net.Listen("tcp", metricsAddr)
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
