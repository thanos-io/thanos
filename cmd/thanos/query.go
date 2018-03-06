package main

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/improbable-eng/thanos/pkg/query/api"
	"github.com/improbable-eng/thanos/pkg/query/ui"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

// registerQuery registers a query command.
func registerQuery(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "query node exposing PromQL enabled Query API with data retrieved from multiple store nodes")

	httpAddr := cmd.Flag("http-address", "listen host:port for HTTP endpoints").
		Default(defaultHTTPAddr).String()

	grpcAddr := cmd.Flag("grpc-address", "listen host:port for gRPC endpoints").
		Default(defaultGRPCAddr).String()

	queryTimeout := cmd.Flag("query.timeout", "maximum time to process query by query node").
		Default("2m").Duration()

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "maximum number of queries processed concurrently by query node").
		Default("20").Int()

	replicaLabel := cmd.Flag("query.replica-label", "label to treat as a replica indicator along which data is deduplicated. Still you will be able to query without deduplication using 'dedup=false' parameter").
		String()

	peers := cmd.Flag("cluster.peers", "initial peers to join the cluster. It can be either <ip:port>, or <domain:port>").Strings()

	clusterBindAddr := cmd.Flag("cluster.address", "listen address for cluster").
		Default(defaultClusterAddr).String()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "explicit address to advertise in cluster").
		String()

	gossipInterval := cmd.Flag("cluster.gossip-interval", "interval between sending gossip messages. By lowering this value (more frequent) gossip messages are propagated across the cluster more quickly at the expense of increased bandwidth.").
		Default(cluster.DefaultGossipInterval.String()).Duration()

	pushPullInterval := cmd.Flag("cluster.pushpull-interval", "interval for gossip state syncs . Setting this interval lower (more frequent) will increase convergence speeds across larger clusters at the expense of increased bandwidth usage.").
		Default(cluster.DefaultPushPullInterval.String()).Duration()

	selectorLabels := cmd.Flag("selector-label", "query selector labels that will be exposed in info endpoint (repeated)").
		PlaceHolder("<name>=\"<value>\"").Strings()

	stores := cmd.Flag("store", "addresses of statically configured store API servers (repeatable)").
		PlaceHolder("<store>").Strings()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer) error {
		peer, err := cluster.New(logger, reg, *clusterBindAddr, *clusterAdvertiseAddr, *peers, true, *gossipInterval, *pushPullInterval)
		if err != nil {
			return errors.Wrap(err, "new cluster peer")
		}
		selectorLset, err := parseFlagLabels(*selectorLabels)
		if err != nil {
			return errors.Wrap(err, "parse federation labels")
		}
		return runQuery(g, logger, reg, tracer,
			*httpAddr,
			*grpcAddr,
			*maxConcurrentQueries,
			*queryTimeout,
			*replicaLabel,
			peer,
			selectorLset,
			*stores,
		)
	}
}

// runQuery starts a server that exposes PromQL Query API. It is responsible for querying configured
// store nodes, merging and duplicating the data to satisfy user query.
func runQuery(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	httpAddr string,
	grpcAddr string,
	maxConcurrentQueries int,
	queryTimeout time.Duration,
	replicaLabel string,
	peer *cluster.Peer,
	selectorLset labels.Labels,
	storeAddrs []string,
) error {
	var (
		stores = query.NewStoreSet(logger, reg, tracer, peer, storeAddrs)
		proxy  = store.NewProxyStore(logger, func(context.Context) ([]*store.Info, error) {
			return stores.Get(), nil
		}, selectorLset)
		queryableCreator = query.NewQueryableCreator(logger, proxy, replicaLabel)
		engine           = promql.NewEngine(logger, reg, maxConcurrentQueries, queryTimeout)
	)
	// Periodically update the store set with the addresses we see in our cluster.
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return runutil.Repeat(3*time.Minute, ctx.Done(), func() error {
				stores.UpdateStatic(ctx)
				return nil
			})
		}, func(error) {
			cancel()
		})
	}
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			err := peer.Join(cluster.PeerState{
				Type:    cluster.PeerTypeQuery,
				APIAddr: httpAddr,
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
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			return runutil.Repeat(5*time.Second, ctx.Done(), func() error {
				stores.UpdatePeers(ctx)
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

		api := v1.NewAPI(reg, engine, queryableCreator)
		api.Register(router.WithPrefix("/api/v1"), tracer, logger)

		mux := http.NewServeMux()
		registerMetrics(mux, reg)
		registerProfile(mux)
		mux.Handle("/", router)

		l, err := net.Listen("tcp", httpAddr)
		if err != nil {
			return errors.Wrapf(err, "listen HTTP on address %s", httpAddr)
		}

		g.Add(func() error {
			return errors.Wrap(http.Serve(l, mux), "serve query")
		}, func(error) {
			l.Close()
		})
	}
	// Start query (proxy) gRPC StoreAPI.
	{
		l, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			return errors.Wrapf(err, "listen gRPC on address")
		}
		logger := log.With(logger, "component", "query")

		s := grpc.NewServer(defaultGRPCServerOpts(logger, reg, tracer)...)
		storepb.RegisterStoreServer(s, proxy)

		g.Add(func() error {
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			s.Stop()
			l.Close()
		})
	}
	level.Info(logger).Log("msg", "starting query node", "peer", peer.Name())
	return nil
}
