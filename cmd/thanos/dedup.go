package main

import (
	"context"
	"path/filepath"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/compact/dedup"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/runutil"
	"gopkg.in/alecthomas/kingpin.v2"
)

func registerDedup(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "continuously dedup blocks in an object store bucket")

	dataDir := cmd.Flag("data-dir", "Data directory in which to cache blocks and process deduplication.").
		Default("./data").String()

	replicaLabel := cmd.Flag("dedup.replica-label", "Label to treat as a replica indicator along which data is deduplicated.").Required().
		String()

	consistencyDelay := modelDuration(cmd.Flag("consistency-delay", "Minimum age of fresh (non-dedup) blocks before they are being processed.").
		Default("30m"))

	blockSyncConcurrency := cmd.Flag("block-sync-concurrency", "Number of goroutines to use when syncing block metadata from object storage.").
		Default("20").Int()

	objStoreConfig := regCommonObjStoreFlags(cmd, "", true)

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		return runDedup(g, logger, reg, *dataDir, *replicaLabel, time.Duration(*consistencyDelay), *blockSyncConcurrency, objStoreConfig, name)
	}
}

func runDedup(g *run.Group, logger log.Logger, reg *prometheus.Registry, dataDir string, replicaLabel string,
	consistencyDelay time.Duration, blockSyncConcurrency int, objStoreConfig *extflag.PathOrContent, component string) error {
	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}

	bkt, err := client.NewBucket(logger, confContentYaml, reg, component)
	if err != nil {
		if bkt != nil {
			runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
		}
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	dedupDir := filepath.Join(dataDir, "dedup")
	g.Add(func() error {
		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		deduper := dedup.NewBucketDeduper(logger, reg, bkt, dedupDir, replicaLabel, consistencyDelay, blockSyncConcurrency)
		return deduper.Dedup(ctx)
	}, func(error) {
		cancel()
	})
	return nil
}
