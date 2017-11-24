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
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"
	"gopkg.in/alecthomas/kingpin.v2"
)

// registerQuery registers a query command.
func registerQuery(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "query node exposing PromQL enabled Query API with data retrieved from multiple store nodes")

	httpAddr := cmd.Flag("http-address", "listen host:port for HTTP endpoints").
		Default(defaultHTTPAddr).String()

	queryTimeout := cmd.Flag("query.timeout", "maximum time to process query by query node").
		Default("2m").Duration()

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "maximum number of queries processed concurrently by query node").
		Default("20").Int()

	replicaLabel := cmd.Flag("query.replica-label", "label to treat as a replica indicator along which data is deduplicated").
		String()

	peers := cmd.Flag("cluster.peers", "initial peers to join the cluster. It can be either <ip:port>, or <domain:port>").Strings()

	clusterBindAddr := cmd.Flag("cluster.address", "listen address for cluster").
		Default(defaultClusterAddr).String()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "explicit address to advertise in cluster").
		String()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry) error {
		peer, err := cluster.Join(
			logger,
			reg,
			*clusterBindAddr,
			*clusterAdvertiseAddr,
			*peers,
			cluster.PeerState{
				Type:    cluster.PeerTypeQuery,
				APIAddr: *httpAddr,
			},
			true,
		)
		if err != nil {
			return errors.Wrap(err, "join cluster")
		}
		return runQuery(g, logger, reg, *httpAddr, query.Config{
			QueryTimeout:         *queryTimeout,
			MaxConcurrentQueries: *maxConcurrentQueries,
		}, *replicaLabel, peer)
	}
}

// runQuery starts a server that exposes PromQL Query API. It is responsible for querying configured
// store nodes, merging and duplicating the data to satisfy user query.
func runQuery(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	httpAddr string,
	cfg query.Config,
	replicaLabel string,
	peer *cluster.Peer,
) error {
	var (
		stores    = cluster.NewStoreSet(logger, reg, peer)
		queryable = query.NewQueryable(logger, stores.Get, replicaLabel)
		engine    = promql.NewEngine(queryable, cfg.EngineOpts(logger))
		api       = v1.NewAPI(engine, queryable, cfg)
	)

	// Periodically update the store set with the addresses we see in our cluster.
	{
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			return runutil.Repeat(5*time.Second, ctx.Done(), func() error {
				stores.Update(ctx)
				return nil
			})
		}, func(error) {
			cancel()
		})
	}
	// Start query API + UI HTTP server.
	{
		router := route.New()
		api.Register(router.WithPrefix("/api/v1"))
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
