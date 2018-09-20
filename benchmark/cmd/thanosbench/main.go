package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"
	"k8s.io/client-go/tools/clientcmd"
)

type opts struct {
	cluster, kubeConfig, bucket, thanosImage                                                   *string
	queries                                                                                    *[]string
	numTimeseries, numPrometheus, numLoadgen                                                   *int
	gatherTime, queryTime, queryRangeOffsetStart, queryRangeOffsetEnd, tsdbLength, blockLength *time.Duration
}

func main() {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))

	app := kingpin.New(filepath.Base(os.Args[0]), "A benchmark for Thanos")
	app.Version(version.Print("thanosbench"))
	app.HelpFlag.Short('h')

	opts := &opts{
		cluster:               app.Flag("cluster", "The kubernetes cluster to run the loadtest in.").Required().String(),
		kubeConfig:            app.Flag("kube-config", "Path to kube config file.").Default(clientcmd.RecommendedHomeFile).String(),
		thanosImage:           app.Flag("thanos-image", "Image to use when running Thanos components.").Default("improbable/thanos:v0.1.0").String(),
		queryRangeOffsetStart: app.Flag("query-range-offset-start", "The offset to the start of the range to use in queries.").Default("1h").Duration(),
		queryRangeOffsetEnd:   app.Flag("query-range-offset-end", "The offset to the end of the range to use in queries.").Default("0").Duration(),
		queries:               app.Flag("queries", "Queries to run.").Strings(),
	}

	registerIngest(app, logger, opts)
	registerResponsiveness(app, logger, opts)
	registerLatency(app, logger, opts)

	kingpin.MustParse(app.Parse(os.Args[1:]))
}

func registerIngest(app *kingpin.Application, logger log.Logger, opts *opts) {
	cmd := app.Command("ingest", "Loadtest Thanos ingestion rate.")
	opts.numPrometheus = cmd.Flag("num-prometheus", "The number of prometheus/sidecar instances to create.").Default("10").Int()
	opts.numLoadgen = cmd.Flag("num-loadgen", "The number of load generation servers to deploy per prometheus instance.").Default("10").Int()
	cmd.Action(func(_ *kingpin.ParseContext) error {
		return testIngest(logger, opts)
	})
}

func registerResponsiveness(app *kingpin.Application, logger log.Logger, opts *opts) {
	cmd := app.Command("responsiveness", "Benchmark Thanos responsiveness.")
	opts.numTimeseries = cmd.Flag("num-timeseries", "The number of timeseries to generate historic data for.").Default("100").Int()
	opts.tsdbLength = cmd.Flag("tsdb-length", "The length of time to generate historic metrics (default 1 year).").Default("8760h").Duration()
	opts.blockLength = cmd.Flag("block-length", "The TSDB will be divided into blocks of this size (default 4 weeks).").Default("672h").Duration()
	opts.bucket = cmd.Flag("bucket", "The bucket containing Thanos TSDB data.").Required().String()
	cmd.Action(func(_ *kingpin.ParseContext) error {
		return testStoreResponsiveness(logger, opts)
	})
}

func registerLatency(app *kingpin.Application, logger log.Logger, opts *opts) {
	cmd := app.Command("latency", "Benchmark Thanos vs Prometheus latency.")
	opts.gatherTime = cmd.Flag("gather-time", "Time to gather metrics before testing latency.").Default("5m").Duration()
	opts.queryTime = cmd.Flag("query-time", "The amount of time each query will be run for.").Default("2m").Duration()
	cmd.Action(func(_ *kingpin.ParseContext) error {
		return testLatency(logger, opts)
	})
}
