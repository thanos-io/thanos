package main

import (
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/promlts/pkg/query"
	"github.com/oklog/oklog/pkg/group"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/prometheus/prometheus/web/api/v1"
	"gopkg.in/alecthomas/kingpin.v2"
)

// registerQuery registers a query command.
func registerQuery(app *kingpin.Application, name string) runFunc {
	cmd := app.Command(name, "query node exposing PromQL enabled Query API with data retrieved from multiple store nodes")

	apiAddr := cmd.Flag("api-address", "listen address for the query API").
		Default(":19099").String()

	storeAddresses := cmd.Flag("store.addresses", "comma delimited listen addresses of store APIs").
		Default("localhost:19090").Required().Strings()

	queryTimeout := cmd.Flag("query.timeout", "maximum time to process query by query node").
		Default("2m").Required().Duration()

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "maximum number of queries processed concurrently by query node").
		Default("20").Required().Int()

	return func(logger log.Logger, metrics prometheus.Registerer) error {
		return runQuery(logger, metrics, *apiAddr, *storeAddresses, *queryTimeout, *maxConcurrentQueries)
	}
}

type noopTargetRetriever struct{}

func (r noopTargetRetriever) Targets() []*retrieval.Target { return nil }

type noopAlertmanagerRetriever struct{}

func (r noopAlertmanagerRetriever) Alertmanagers() []*url.URL { return nil }

var (
	emptyConfigFunc = func() config.Config {
		return config.Config{}
	}
	passTestReady = func(f http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			f(w, r)
		}
	}
)

// runQuery starts a server that exposes PromQL Query API. It is responsible for querying configured
// store nodes, merging and duplicating the data to satisfy user query.
func runQuery(
	logger log.Logger,
	reg prometheus.Registerer,
	apiAddr string,
	storeAddresses []string,
	queryTimeout time.Duration,
	maxConcurrentQueries int,
) error {
	level.Info(logger).Log(
		"msg", "I'm a query node",
		"api-address", apiAddr,
		"store.addresses", storeAddresses,
		"query.timeout", queryTimeout,
		"query.max-concurrent", maxConcurrentQueries,
	)

	var client http.Client

	// Set up query API engine.
	queryable := query.NewQueryable(&client, storeAddresses)
	engine := promql.NewEngine(queryable, &promql.EngineOptions{
		Logger:               logger,
		Timeout:              queryTimeout,
		MaxConcurrentQueries: maxConcurrentQueries,
	})
	api := v1.NewAPI(
		engine,
		queryable,
		&noopTargetRetriever{},
		&noopAlertmanagerRetriever{},
		emptyConfigFunc,
		passTestReady,
	)

	var g group.Group

	// Start query API HTTP server.
	{
		router := route.New()
		api.Register(router)

		mux := http.NewServeMux()
		mux.Handle("/metrics", prometheus.Handler())
		mux.Handle("/api/prom/api/v1", router)

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
	return g.Run()
}
