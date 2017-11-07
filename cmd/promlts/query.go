package main

import (
	"net"
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/promlts/pkg/query"
	"github.com/improbable-eng/promlts/pkg/query/api"
	"github.com/oklog/oklog/pkg/group"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"
	"gopkg.in/alecthomas/kingpin.v2"
)

// registerQuery registers a query command.
func registerQuery(m map[string]runFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "query node exposing PromQL enabled Query API with data retrieved from multiple store nodes")

	apiAddr := cmd.Flag("api-address", "listen address for the query API").
		Default(":19099").String()

	storeAddresses := cmd.Flag("store.addresses", "comma delimited listen addresses of store APIs").
		Default("localhost:19090").Strings()

	queryTimeout := cmd.Flag("query.timeout", "maximum time to process query by query node").
		Default("2m").Duration()

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "maximum number of queries processed concurrently by query node").
		Default("20").Int()

	m[name] = func(logger log.Logger, metrics *prometheus.Registry) error {
		return runQuery(logger, metrics, *apiAddr, query.Config{
			StoreAddresses:       *storeAddresses,
			QueryTimeout:         *queryTimeout,
			MaxConcurrentQueries: *maxConcurrentQueries,
		})
	}
}

// runQuery starts a server that exposes PromQL Query API. It is responsible for querying configured
// store nodes, merging and duplicating the data to satisfy user query.
func runQuery(
	logger log.Logger,
	reg *prometheus.Registry,
	apiAddr string,
	cfg query.Config,
) error {
	// Set up query API engine.
	queryable := query.NewQueryable(logger, cfg.StoreAddresses)
	engine := promql.NewEngine(queryable, cfg.EngineOpts(logger))
	api := v1.NewAPI(engine, queryable, cfg)

	var g group.Group

	// Start query API HTTP server.
	{
		router := route.New().WithPrefix("/api/v1")
		api.Register(router)

		mux := http.NewServeMux()
		registerMetrics(mux, reg)
		registerProfile(mux)
		mux.Handle("/", router)

		l, err := net.Listen("tcp", apiAddr)
		if err != nil {
			return errors.Wrapf(err, "listen on address %s", apiAddr)
		}

		g.Add(func() error {
			return errors.Wrap(http.Serve(l, mux), "serve query")
		}, func(error) {
			l.Close()
		})
	}

	// Listen for termination signals.
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			return interrupt(cancel)
		}, func(error) {
			close(cancel)
		})
	}

	level.Info(logger).Log(
		"msg", "starting query node",
		"api-address", apiAddr,
		"store.addresses", cfg.StoreAddresses,
		"query.timeout", cfg.QueryTimeout,
		"query.max-concurrent", cfg.MaxConcurrentQueries,
	)
	return g.Run()
}
