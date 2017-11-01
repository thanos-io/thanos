package main

import (
	"context"

	"github.com/alecthomas/units"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/oklog/pkg/group"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

// registerStore registers a store command.
func registerStore(app *kingpin.Application, name string) runFunc {
	cmd := app.Command(name, "sidecar for Prometheus server")

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks").
		PlaceHolder("<bucket>").Required().String()

	peers := cmd.Flag("peers", "Peering store nodes to connect to").
		PlaceHolder("<peers>").Strings()

	maxDiskCacheSize := cmd.Flag("disk-cache-size", "maximum size of on-disk cache").
		Default("100GB").Bytes()

	maxMemCacheSize := cmd.Flag("mem-cache-size", "maximum size of in-memory cache").
		Default("4GB").Bytes()

	return func(logger log.Logger, metrics prometheus.Registerer) error {
		return runStore(logger, metrics, *gcsBucket, *peers, *maxDiskCacheSize, *maxMemCacheSize)
	}
}

// runStore starts a daemon that connects to a cluster of other store nodes through gossip.
// It also connects to a Google Cloud Storage bucket and serves data queries to a subset of its contents.
// The served subset is determined through HRW hashing against the block's ULIDs and the known peers.
func runStore(
	logger log.Logger,
	reg prometheus.Registerer,
	gcsBucket string,
	peers []string,
	diskCacheSize units.Base2Bytes,
	memCacheSize units.Base2Bytes,
) error {
	level.Info(logger).Log("msg", "I'm a store node", "diskCacheSize", diskCacheSize, "memCacheSize", memCacheSize)

	gcsClient, err := storage.NewClient(context.Background())
	if err != nil {
		return errors.Wrap(err, "create GCS client")
	}
	defer gcsClient.Close()

	var g group.Group

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
