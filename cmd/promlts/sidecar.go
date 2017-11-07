package main

import (
	"context"
	"net"
	"net/http"

	"cloud.google.com/go/storage"
	"github.com/improbable-eng/promlts/pkg/shipper"
	"github.com/improbable-eng/promlts/pkg/store"
	"github.com/improbable-eng/promlts/pkg/store/storepb"
	"google.golang.org/grpc"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/oklog/pkg/group"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

func registerSidecar(m map[string]runFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "sidecar for Prometheus server")

	apiAddr := cmd.Flag("api-address", "listen address for the store API").
		Default(":19090").String()

	metricsAddr := cmd.Flag("metrics-address", "metrics address for the sidecar").
		Default(":19091").String()

	promURL := cmd.Flag("prometheus.url", "URL at which to reach Prometheus's API").
		Default("http://localhost:9090").String()

	dataDir := cmd.Flag("tsdb.path", "data directory of TSDB").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks").
		PlaceHolder("<bucket>").Required().String()

	m[name] = func(logger log.Logger, reg prometheus.Registerer) error {
		return runSidecar(logger, reg, *apiAddr, *metricsAddr, *promURL, *dataDir, *gcsBucket)
	}
}

func runSidecar(
	logger log.Logger,
	reg prometheus.Registerer,
	apiAddr string,
	metricsAddr string,
	promURL string,
	dataDir string,
	gcsBucket string,
) error {
	level.Info(logger).Log("msg", "starting sidecar")

	var g group.Group
	{
		mux := http.NewServeMux()
		mux.Handle("/metrics", prometheus.Handler())

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
	{
		l, err := net.Listen("tcp", apiAddr)
		if err != nil {
			return errors.Wrap(err, "listen API address")
		}
		logger := log.With(logger, "component", "proxy")

		var client http.Client
		proxy, err := store.NewPrometheusProxy(logger, &client, promURL)
		if err != nil {
			return errors.Wrap(err, "create Prometheus proxy")
		}

		s := grpc.NewServer()

		storepb.RegisterStoreServer(s, proxy)

		g.Add(func() error {
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			s.Stop()
			l.Close()
		})

	}
	// The background shipper continously scans the data directory and uploads
	// new found blocks to Google Cloud Storage.
	{
		gcsClient, err := storage.NewClient(context.Background())
		if err != nil {
			return errors.Wrap(err, "create GCS client")
		}
		defer gcsClient.Close()

		remote := shipper.NewGCSRemote(logger, nil, gcsClient.Bucket(gcsBucket))
		s := shipper.New(logger, nil, dataDir, remote, shipper.IsULIDDir)

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return errors.Wrap(s.Run(ctx), "run block shipper")
		}, func(error) {
			cancel()
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
