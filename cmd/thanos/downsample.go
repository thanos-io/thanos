// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	objstoretracing "github.com/thanos-io/objstore/tracing/opentracing"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
)

type DownsampleMetrics struct {
	downsamples        *prometheus.CounterVec
	downsampleFailures *prometheus.CounterVec
	downsampleDuration *prometheus.HistogramVec
}

func newDownsampleMetrics(reg *prometheus.Registry) *DownsampleMetrics {
	m := new(DownsampleMetrics)

	m.downsamples = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_downsample_total",
		Help: "Total number of downsampling attempts.",
	}, []string{"group"})
	m.downsampleFailures = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_downsample_failures_total",
		Help: "Total number of failed downsampling attempts.",
	}, []string{"group"})
	m.downsampleDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_compact_downsample_duration_seconds",
		Help:    "Duration of downsample runs",
		Buckets: []float64{60, 300, 900, 1800, 3600, 7200, 14400}, // 1m, 5m, 15m, 30m, 60m, 120m, 240m
	}, []string{"group"})

	return m
}

func RunDownsample(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	httpBindAddr string,
	httpTLSConfig string,
	httpGracePeriod time.Duration,
	dataDir string,
	waitInterval time.Duration,
	downsampleConcurrency int,
	blockFilesConcurrency int,
	objStoreConfig *extflag.PathOrContent,
	comp component.Component,
	hashFunc metadata.HashFunc,
) error {
	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}

	bkt, err := client.NewBucket(logger, confContentYaml, component.Downsample.String())
	if err != nil {
		return err
	}
	insBkt := objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))

	// While fetching blocks, filter out blocks that were marked for no downsample.
	metaFetcher, err := block.NewMetaFetcher(logger, block.FetcherConcurrency, insBkt, "", extprom.WrapRegistererWithPrefix("thanos_", reg), []block.MetadataFilter{
		block.NewDeduplicateFilter(block.FetcherConcurrency),
		downsample.NewGatherNoDownsampleMarkFilter(logger, insBkt),
	})
	if err != nil {
		return errors.Wrap(err, "create meta fetcher")
	}

	// Ensure we close up everything properly.
	defer func() {
		if err != nil {
			runutil.CloseWithLogOnErr(logger, insBkt, "bucket client")
		}
	}()

	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	metrics := newDownsampleMetrics(reg)
	// Start cycle of syncing blocks from the bucket and garbage collecting the bucket.
	{
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, insBkt, "bucket client")
			statusProber.Ready()

			return runutil.Repeat(waitInterval, ctx.Done(), func() error {
				level.Info(logger).Log("msg", "start first pass of downsampling")
				metas, _, err := metaFetcher.Fetch(ctx)
				if err != nil {
					return errors.Wrap(err, "sync before first pass of downsampling")
				}

				for _, meta := range metas {
					groupKey := meta.Thanos.GroupKey()
					metrics.downsamples.WithLabelValues(groupKey)
					metrics.downsampleFailures.WithLabelValues(groupKey)
				}
				if err := downsampleBucket(ctx, logger, metrics, insBkt, metas, dataDir, downsampleConcurrency, blockFilesConcurrency, hashFunc, false); err != nil {
					return errors.Wrap(err, "downsampling failed")
				}

				level.Info(logger).Log("msg", "start second pass of downsampling")
				metas, _, err = metaFetcher.Fetch(ctx)
				if err != nil {
					return errors.Wrap(err, "sync before second pass of downsampling")
				}
				if err := downsampleBucket(ctx, logger, metrics, insBkt, metas, dataDir, downsampleConcurrency, blockFilesConcurrency, hashFunc, false); err != nil {
					return errors.Wrap(err, "downsampling failed")
				}
				return nil
			})
		}, func(error) {
			cancel()
		})
	}

	srv := httpserver.New(logger, reg, comp, httpProbe,
		httpserver.WithListen(httpBindAddr),
		httpserver.WithGracePeriod(httpGracePeriod),
		httpserver.WithTLSConfig(httpTLSConfig),
	)

	g.Add(func() error {
		statusProber.Healthy()

		return srv.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		srv.Shutdown(err)
	})

	level.Info(logger).Log("msg", "starting downsample node")
	return nil
}

func downsampleBucket(
	ctx context.Context,
	logger log.Logger,
	metrics *DownsampleMetrics,
	bkt objstore.Bucket,
	metas map[ulid.ULID]*metadata.Meta,
	dir string,
	downsampleConcurrency int,
	blockFilesConcurrency int,
	hashFunc metadata.HashFunc,
	acceptMalformedIndex bool,
) (rerr error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return errors.Wrap(err, "create dir")
	}

	defer func() {
		// Leave the downsample directory for inspection if it is a halt error
		// or if it is not then so that possibly we would not have to download everything again.
		if rerr != nil {
			return
		}
		if err := os.RemoveAll(dir); err != nil {
			level.Error(logger).Log("msg", "failed to remove downsample cache directory", "path", dir, "err", err)
		}
	}()

	// mapping from a hash over all source IDs to blocks. We don't need to downsample a block
	// if a downsampled version with the same hash already exists.
	sources5m := map[ulid.ULID]struct{}{}
	sources1h := map[ulid.ULID]struct{}{}

	for _, m := range metas {
		switch m.Thanos.Downsample.Resolution {
		case downsample.ResLevel0:
			continue
		case downsample.ResLevel1:
			for _, id := range m.Compaction.Sources {
				sources5m[id] = struct{}{}
			}
		case downsample.ResLevel2:
			for _, id := range m.Compaction.Sources {
				sources1h[id] = struct{}{}
			}
		default:
			return errors.Errorf("unexpected downsampling resolution %d", m.Thanos.Downsample.Resolution)
		}
	}

	ignoreDirs := []string{}
	for ulid := range metas {
		ignoreDirs = append(ignoreDirs, ulid.String())
	}

	if err := runutil.DeleteAll(dir, ignoreDirs...); err != nil {
		level.Warn(logger).Log("msg", "failed deleting potentially outdated directories/files, some disk space usage might have leaked. Continuing", "err", err, "dir", dir)
	}

	metasULIDS := make([]ulid.ULID, 0, len(metas))
	for k := range metas {
		metasULIDS = append(metasULIDS, k)
	}
	sort.Slice(metasULIDS, func(i, j int) bool {
		return metasULIDS[i].Compare(metasULIDS[j]) < 0
	})

	var (
		wg                      sync.WaitGroup
		metaCh                  = make(chan *metadata.Meta)
		downsampleErrs          errutil.MultiError
		errCh                   = make(chan error, downsampleConcurrency)
		workerCtx, workerCancel = context.WithCancel(ctx)
	)

	defer workerCancel()

	level.Debug(logger).Log("msg", "downsampling bucket", "concurrency", downsampleConcurrency)
	for i := 0; i < downsampleConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := range metaCh {
				resolution := downsample.ResLevel1
				errMsg := "downsampling to 5 min"
				if m.Thanos.Downsample.Resolution == downsample.ResLevel1 {
					resolution = downsample.ResLevel2
					errMsg = "downsampling to 60 min"
				}
				if err := processDownsampling(workerCtx, logger, bkt, m, dir, resolution, hashFunc, metrics, acceptMalformedIndex, blockFilesConcurrency); err != nil {
					metrics.downsampleFailures.WithLabelValues(m.Thanos.GroupKey()).Inc()
					errCh <- errors.Wrap(err, errMsg)

				}
				metrics.downsamples.WithLabelValues(m.Thanos.GroupKey()).Inc()
			}
		}()
	}

	// Workers scheduled, distribute blocks.
metaSendLoop:
	for _, mk := range metasULIDS {
		m := metas[mk]

		switch m.Thanos.Downsample.Resolution {
		case downsample.ResLevel2:
			continue

		case downsample.ResLevel0:
			missing := false
			for _, id := range m.Compaction.Sources {
				if _, ok := sources5m[id]; !ok {
					missing = true
					break
				}
			}
			if !missing {
				continue
			}
			// Only downsample blocks once we are sure to get roughly 2 chunks out of it.
			// NOTE(fabxc): this must match with at which block size the compactor creates downsampled
			// blocks. Otherwise we may never downsample some data.
			if m.MaxTime-m.MinTime < downsample.ResLevel1DownsampleRange {
				continue
			}

		case downsample.ResLevel1:
			missing := false
			for _, id := range m.Compaction.Sources {
				if _, ok := sources1h[id]; !ok {
					missing = true
					break
				}
			}
			if !missing {
				continue
			}
			// Only downsample blocks once we are sure to get roughly 2 chunks out of it.
			// NOTE(fabxc): this must match with at which block size the compactor creates downsampled
			// blocks. Otherwise we may never downsample some data.
			if m.MaxTime-m.MinTime < downsample.ResLevel2DownsampleRange {
				continue
			}
		}

		select {
		case <-workerCtx.Done():
			downsampleErrs.Add(workerCtx.Err())
			break metaSendLoop
		case metaCh <- m:
		case downsampleErr := <-errCh:
			downsampleErrs.Add(downsampleErr)
			break metaSendLoop
		}
	}

	close(metaCh)
	wg.Wait()
	workerCancel()
	close(errCh)

	// Collect any other error reported by the workers.
	for downsampleErr := range errCh {
		downsampleErrs.Add(downsampleErr)
	}

	return downsampleErrs.Err()
}

func processDownsampling(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.Bucket,
	m *metadata.Meta,
	dir string,
	resolution int64,
	hashFunc metadata.HashFunc,
	metrics *DownsampleMetrics,
	acceptMalformedIndex bool,
	blockFilesConcurrency int,
) error {
	begin := time.Now()
	bdir := filepath.Join(dir, m.ULID.String())

	err := block.Download(ctx, logger, bkt, m.ULID, bdir, objstore.WithFetchConcurrency(blockFilesConcurrency))
	if err != nil {
		return errors.Wrapf(err, "download block %s", m.ULID)
	}
	level.Info(logger).Log("msg", "downloaded block", "id", m.ULID, "duration", time.Since(begin), "duration_ms", time.Since(begin).Milliseconds())

	if err := block.VerifyIndex(ctx, logger, filepath.Join(bdir, block.IndexFilename), m.MinTime, m.MaxTime); err != nil && !acceptMalformedIndex {
		return errors.Wrap(err, "input block index not valid")
	}

	begin = time.Now()

	var pool chunkenc.Pool
	if m.Thanos.Downsample.Resolution == 0 {
		pool = chunkenc.NewPool()
	} else {
		pool = downsample.NewPool()
	}

	b, err := tsdb.OpenBlock(logger, bdir, pool)
	if err != nil {
		return errors.Wrapf(err, "open block %s", m.ULID)
	}
	defer runutil.CloseWithLogOnErr(log.With(logger, "outcome", "potential left mmap file handlers left"), b, "tsdb reader")

	id, err := downsample.Downsample(ctx, logger, m, b, dir, resolution)
	if err != nil {
		return errors.Wrapf(err, "downsample block %s to window %d", m.ULID, resolution)
	}
	resdir := filepath.Join(dir, id.String())

	downsampleDuration := time.Since(begin)
	level.Info(logger).Log("msg", "downsampled block",
		"from", m.ULID, "to", id, "duration", downsampleDuration, "duration_ms", downsampleDuration.Milliseconds())
	metrics.downsampleDuration.WithLabelValues(m.Thanos.GroupKey()).Observe(downsampleDuration.Seconds())

	stats, err := block.GatherIndexHealthStats(ctx, logger, filepath.Join(resdir, block.IndexFilename), m.MinTime, m.MaxTime)
	if err == nil {
		err = stats.AnyErr()
	}
	if err != nil && !acceptMalformedIndex {
		return errors.Wrap(err, "output block index not valid")
	}

	meta, err := metadata.ReadFromDir(resdir)
	if err != nil {
		return errors.Wrap(err, "read meta")
	}

	if stats.ChunkMaxSize > 0 {
		meta.Thanos.IndexStats.ChunkMaxSize = stats.ChunkMaxSize
	}
	if stats.SeriesMaxSize > 0 {
		meta.Thanos.IndexStats.SeriesMaxSize = stats.SeriesMaxSize
	}
	if err := meta.WriteToDir(logger, resdir); err != nil {
		return errors.Wrap(err, "write meta")
	}

	begin = time.Now()

	err = block.Upload(ctx, logger, bkt, resdir, hashFunc)
	if err != nil {
		return errors.Wrapf(err, "upload downsampled block %s", id)
	}

	level.Info(logger).Log("msg", "uploaded block", "id", id, "duration", time.Since(begin), "duration_ms", time.Since(begin).Milliseconds())

	// It is not harmful if these fails.
	if err := os.RemoveAll(bdir); err != nil {
		level.Warn(logger).Log("msg", "failed to clean directory", "dir", bdir, "err", err)
	}
	if err := os.RemoveAll(resdir); err != nil {
		level.Warn(logger).Log("msg", "failed to clean directory", "resdir", bdir, "err", err)
	}

	return nil
}
