package main

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/okgroup"
	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/improbable-eng/thanos/pkg/query/api"
	"github.com/improbable-eng/thanos/pkg/query/ui"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"
	"gopkg.in/alecthomas/kingpin.v2"
)

// registerQuery registers a query command.
func registerQuery(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "query node exposing PromQL enabled Query API with data retrieved from multiple store nodes")

	apiAddr := cmd.Flag("api-address", "listen host:port address for the query API").
		Default("0.0.0.0:19099").String()

	queryTimeout := cmd.Flag("query.timeout", "maximum time to process query by query node").
		Default("2m").Duration()

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "maximum number of queries processed concurrently by query node").
		Default("20").Int()

	peers := cmd.Flag("cluster.peers", "initial peers to join the cluster").Strings()

	clusterBindAddr := cmd.Flag("cluster.address", "listen address for cluster").
		Default(defaultClusterAddr).String()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "explicit address to advertise in cluster").
		String()

	m[name] = func(logger log.Logger, metrics *prometheus.Registry) (okgroup.Group, error) {
		peer, err := joinCluster(
			logger,
			cluster.PeerTypeQuery,
			*clusterBindAddr,
			*clusterAdvertiseAddr,
			*apiAddr,
			*peers,
		)
		if err != nil {
			return okgroup.Group{}, errors.Wrap(err, "join cluster")
		}
		return runQuery(logger, metrics, *apiAddr, query.Config{
			QueryTimeout:         *queryTimeout,
			MaxConcurrentQueries: *maxConcurrentQueries,
		}, peer)
	}
}

// runQuery starts a server that exposes PromQL Query API. It is responsible for querying configured
// store nodes, merging and duplicating the data to satisfy user query.
func runQuery(
	logger log.Logger,
	reg *prometheus.Registry,
	apiAddr string,
	cfg query.Config,
	peer *cluster.Peer,
) (
	okgroup.Group, error,
) {
	var (
		stores    = cluster.NewStoreSet(logger, peer)
		queryable = query.NewQueryable(logger, stores.Get)
		engine    = promql.NewEngine(queryable, cfg.EngineOpts(logger))
		api       = v1.NewAPI(engine, queryable, cfg)
	)

	var g okgroup.Group

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

		l, err := net.Listen("tcp", apiAddr)
		if err != nil {
			return g, errors.Wrapf(err, "listen on address %s", apiAddr)
		}

		g.Add(func() error {
			return errors.Wrap(http.Serve(l, mux), "serve query")
		}, func(error) {
			l.Close()
		})
	}

	level.Info(logger).Log("msg", "starting query node")
	return g, nil
}

func discoverAddresses(ctx context.Context, target *url.URL) (map[string]struct{}, error) {
	host, port, err := net.SplitHostPort(target.Host)
	if err != nil {
		return nil, errors.Wrap(err, "split host/port")
	}
	var res net.Resolver

	addresses := map[string]struct{}{}

	switch target.Scheme {
	case "dns", "dnsip":
		ips, err := res.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, errors.Wrap(err, "LookupIP")
		}
		for _, ip := range ips {
			addresses[net.JoinHostPort(ip.String(), port)] = struct{}{}
		}
	case "dnssrv":
		_, records, err := res.LookupSRV(ctx, "", "tcp", host)
		if err != nil {
			return nil, errors.Wrap(err, "LookupSRV")
		}
		for _, rec := range records {
			addresses[net.JoinHostPort(rec.Target, strconv.Itoa(int(rec.Port)))] = struct{}{}
		}
	case "dnsaddr":
		names, err := res.LookupAddr(ctx, host)
		if err != nil {
			return nil, errors.Wrap(err, "LookupAddr")
		}
		for _, n := range names {
			addresses[net.JoinHostPort(n, port)] = struct{}{}
		}
	case "tcp":
		addresses[net.JoinHostPort(host, port)] = struct{}{}
	default:
		return nil, errors.Errorf("unsupported discovery scheme %s (one of dnsip, dnssrv, dnsaddr)", target.Scheme)
	}
	return addresses, nil
}
