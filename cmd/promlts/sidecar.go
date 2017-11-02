package main

import (
	"context"

	"github.com/improbable-eng/promlts/pkg/shipper"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/oklog/pkg/group"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

func registerSidecar(app *kingpin.Application, name string) runFunc {
	cmd := app.Command(name, "sidecar for Prometheus server")

	promAddr := cmd.Flag("prometheus.address", "listen address of Prometheus instance").
		Default("localhost:9090").String()

	dataDir := cmd.Flag("tsdb.path", "data directory of TSDB").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks").
		PlaceHolder("<bucket>").Required().String()

	return func(logger log.Logger, reg prometheus.Registerer) error {
		return runSidecar(logger, reg, *promAddr, *dataDir, *gcsBucket)
	}
}

func runSidecar(
	logger log.Logger,
	reg prometheus.Registerer,
	promAddr string,
	dataDir string,
	gcsBucket string,
) error {
	level.Info(logger).Log("msg", "I'm a sidecar", "promDir", dataDir, "promAddr", promAddr)

	gcsClient, err := storage.NewClient(context.Background())
	if err != nil {
		return errors.Wrap(err, "create GCS client")
	}
	defer gcsClient.Close()

	var g group.Group

	// The background shipper continously scans the data directory and uploads
	// new found blocks to Google Cloud Storage.
	{
		remote := shipper.NewGCSRemote(logger, nil, gcsClient.Bucket(gcsBucket))
		s := shipper.New(logger, nil, dataDir, remote, shipper.IsULIDDir)

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return s.Run(ctx)
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
