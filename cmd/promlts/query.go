package main

import (
	"net"
	"net/http"

	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/promlts/pkg/okgroup"
	"github.com/improbable-eng/promlts/pkg/query"
	"github.com/improbable-eng/promlts/pkg/query/api"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"
	"gopkg.in/alecthomas/kingpin.v2"
	"net/url"
)

// registerQuery registers a query command.
func registerQuery(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "query node exposing PromQL enabled Query API with data retrieved from multiple store nodes")

	apiAddr := cmd.Flag("query.address", "listen address for the query API").
		Default("0.0.0.0:19099").URL()

	storeAddresses := cmd.Flag("query.store-addresses", "comma delimited listen addresses of store APIs").
		Default("localhost:19090").Strings()

	queryTimeout := cmd.Flag("query.timeout", "maximum time to process query by query node").
		Default("2m").Duration()

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "maximum number of queries processed concurrently by query node").
		Default("20").Int()

	m[name] = func(logger log.Logger, metrics prometheus.Registerer) (okgroup.Group, error) {
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
	reg prometheus.Registerer,
	apiAddr *url.URL,
	cfg query.Config,
) (okgroup.Group, error) {

	// Set up query API engine.
	queryable := query.NewQueryable(logger, cfg.StoreAddresses)
	engine := promql.NewEngine(queryable, cfg.EngineOpts(logger))
	api := v1.NewAPI(engine, queryable, cfg)

	var g okgroup.Group

	// Start query API HTTP server.
	{
		router := route.New().WithPrefix("/api/v1")
		api.Register(router)

		mux := http.NewServeMux()
		mux.Handle("/metrics", prometheus.Handler())
		registerProfile(mux)
		mux.Handle("/", router)

		l, err := net.Listen("tcp", apiAddr.String())
		if err != nil {
			return g, errors.Wrapf(err, "listen on address %s", apiAddr)
		}

		g.Add(func() error {
			return errors.Wrap(http.Serve(l, mux), "serve query")
		}, func(error) {
			l.Close()
		})
	}

	level.Info(logger).Log(
		"msg", "starting query node",
		"api-address", apiAddr,
		"store.addresses", strings.Join(cfg.StoreAddresses, ","),
		"query.timeout", cfg.QueryTimeout,
		"query.max-concurrent", cfg.MaxConcurrentQueries,
	)
	return g, nil
}
