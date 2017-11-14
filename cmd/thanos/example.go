package main

import (
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/okgroup"
	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/improbable-eng/thanos/pkg/cluster"
)

func registerExample(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "run all the Thanos services in the same binary: prometheus sidecar, query and store node.")

	// Query flags.
	apiAddr := cmd.Flag("query.address", "listen address for the query API").
		Default("localhost:19099").String()

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "maximum number of queries processed concurrently by query node").
		Default("20").Int()

	queryTimeout := cmd.Flag("query.timeout", "maximum time to process query by query node").
		Default("2m").Duration()

	metricsAddr := cmd.Flag("sidecar.metrics-address", "metrics address for the sidecar").
		Default("localhost:19091").String()

	promURL := cmd.Flag("sidecar.prometheus-url", "URL at which to reach Prometheus's API").
		Default("http://localhost:9090").URL()

	dataDir := cmd.Flag("sidecar.tsdb-path", "data directory of TSDB").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks. If empty sidecar won't store any block inside Google Cloud Storage").
		PlaceHolder("<bucket>").String()

	m[name] = func(logger log.Logger, metrics *prometheus.Registry) (okgroup.Group, error) {
		var g okgroup.Group

		queryGroup, err := runQuery(logger, metrics, *apiAddr, query.Config{
			QueryTimeout:         *queryTimeout,
			MaxConcurrentQueries: *maxConcurrentQueries,
		}, cluster.JoinConfig{})
		if err != nil {
			return g, errors.Wrap(err, "query setup")
		}
		g.AddGroup(queryGroup)

		sidecarGroup, err := runSidecar(logger, metrics, *apiAddr, *metricsAddr, *promURL, *dataDir, cluster.JoinConfig{}, *gcsBucket)
		if err != nil {
			return g, errors.Wrap(err, "sidecar setup")
		}
		g.AddGroup(sidecarGroup)

		return g, nil
	}
}
