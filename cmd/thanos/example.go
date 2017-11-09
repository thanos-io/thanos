package main

import (
	"gopkg.in/alecthomas/kingpin.v2"

	"fmt"
	"net/url"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/okgroup"
	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
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

	// Sidecar flags.
	storeAddress := cmd.Flag("sidecar.address", "listen address of sidecar store API").
		Default("localhost:19090").String()

	metricsAddr := cmd.Flag("sidecar.metrics-address", "metrics address for the sidecar").
		Default("localhost:19091").String()

	promURL := cmd.Flag("sidecar.prometheus-url", "URL at which to reach Prometheus's API").
		Default("http://localhost:9090").URL()

	dataDir := cmd.Flag("sidecar.tsdb-path", "data directory of TSDB").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks. If empty sidecar won't store any block inside Google Cloud Storage").
		PlaceHolder("<bucket>").String()

	// Store node flags.
	maxDiskCacheSize := cmd.Flag("store.disk-cache-size", "maximum size of on-disk cache").
		Default("100GB").Bytes()

	maxMemCacheSize := cmd.Flag("store.mem-cache-size", "maximum size of in-memory cache").
		Default("4GB").Bytes()

	m[name] = func(logger log.Logger, metrics *prometheus.Registry) (okgroup.Group, error) {
		var g okgroup.Group

		storeURL, err := url.Parse(fmt.Sprintf("tcp://%s", *storeAddress))
		if err != nil {
			return g, err
		}

		queryGroup, err := runQuery(logger, metrics, *apiAddr, nil, query.Config{
			QueryTimeout:         *queryTimeout,
			MaxConcurrentQueries: *maxConcurrentQueries,
		}, nil, storeURL)
		if err != nil {
			return g, errors.Wrap(err, "query setup")
		}
		g.AddGroup(queryGroup)

		sidecarGroup, err := runSidecar(logger, metrics, *storeAddress, *metricsAddr, *promURL, *dataDir, nil, *gcsBucket)
		if err != nil {
			return g, errors.Wrap(err, "sidecar setup")
		}
		g.AddGroup(sidecarGroup)

		storeGroup, err := runStore(logger, metrics, *gcsBucket, *maxDiskCacheSize, *maxMemCacheSize)
		if err != nil {
			return g, errors.Wrap(err, "store setup")
		}
		g.AddGroup(storeGroup)

		return g, nil
	}
}
