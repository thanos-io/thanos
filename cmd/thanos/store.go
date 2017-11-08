package main

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/alecthomas/units"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/okgroup"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

// registerStore registers a store command.
func registerStore(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "store node giving access to blocks in a GCS bucket")

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks").
		PlaceHolder("<bucket>").Required().String()

	maxDiskCacheSize := cmd.Flag("store.disk-cache-size", "maximum size of on-disk cache").
		Default("100GB").Bytes()

	maxMemCacheSize := cmd.Flag("store.mem-cache-size", "maximum size of in-memory cache").
		Default("4GB").Bytes()

	m[name] = func(logger log.Logger, metrics *prometheus.Registry) (okgroup.Group, error) {
		return runStore(logger, metrics, *gcsBucket, *maxDiskCacheSize, *maxMemCacheSize)
	}
}

// runStore starts a daemon that connects to a cluster of other store nodes through gossip.
// It also connects to a Google Cloud Storage bucket and serves data queries to a subset of its contents.
// The served subset is determined through HRW hashing against the block's ULIDs and the known peers.
func runStore(
	logger log.Logger,
	reg *prometheus.Registry,
	gcsBucket string,
	diskCacheSize units.Base2Bytes,
	memCacheSize units.Base2Bytes,
) (okgroup.Group, error) {
	var g okgroup.Group

	gcsClient, err := storage.NewClient(context.Background())
	if err != nil {
		return g, errors.Wrap(err, "create GCS client")
	}
	defer gcsClient.Close()

	// TODO(bplotka): Add store node logic for fetching blocks and exposing store API.

	level.Info(logger).Log("msg", "I'm a store node", "diskCacheSize", diskCacheSize, "memCacheSize", memCacheSize)
	return g, nil
}
