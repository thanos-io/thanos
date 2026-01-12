// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/units"
	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	objstoretracing "github.com/thanos-io/objstore/tracing/opentracing"

	blocksAPI "github.com/thanos-io/thanos/pkg/api/blocks"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/strutil"
	"github.com/thanos-io/thanos/pkg/tracing"
	"github.com/thanos-io/thanos/pkg/ui"
)

// idempotentRegisterer wraps a prometheus.Registerer and ignores duplicate registration errors.
// This allows running runCompactForTenant multiple times with the same registry without panicking.
type idempotentRegisterer struct {
	prometheus.Registerer
}

func (r *idempotentRegisterer) Register(c prometheus.Collector) error {
	err := r.Registerer.Register(c)
	// Ignore duplicate registration errors - expected in multi-tenant mode
	if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
		return nil
	}
	return err
}

func (r *idempotentRegisterer) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		_ = r.Register(c) // Ignores duplicates
	}
}

var (
	compactions = compactionSet{
		1 * time.Hour,
		2 * time.Hour,
		8 * time.Hour,
		2 * 24 * time.Hour,
		14 * 24 * time.Hour,
	}
)

type compactionSet []time.Duration

func (cs compactionSet) String() string {
	result := make([]string, len(cs))
	for i, c := range cs {
		result[i] = fmt.Sprintf("%d=%dh", i, int(c.Hours()))
	}
	return strings.Join(result, ", ")
}

// levels returns set of compaction levels not higher than specified max compaction level.
func (cs compactionSet) levels(maxLevel int) ([]int64, error) {
	if maxLevel >= len(cs) {
		return nil, errors.Errorf("level is bigger then default set of %d", len(cs))
	}

	levels := make([]int64, maxLevel+1)
	for i, c := range cs[:maxLevel+1] {
		levels[i] = int64(c / time.Millisecond)
	}
	return levels, nil
}

// maxLevel returns max available compaction level.
func (cs compactionSet) maxLevel() int {
	return len(cs) - 1
}

func registerCompact(app *extkingpin.App) {
	cmd := app.Command(component.Compact.String(), "Continuously compacts blocks in an object store bucket.")
	conf := &compactConfig{}
	conf.registerFlag(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		return runCompact(g, logger, tracer, reg, component.Compact, *conf, getFlagsMap(cmd.Flags()))
	})
}

type compactMetrics struct {
	halted                      prometheus.Gauge
	retried                     prometheus.Counter
	iterations                  prometheus.Counter
	cleanups                    prometheus.Counter
	partialUploadDeleteAttempts prometheus.Counter
	blocksCleaned               prometheus.Counter
	blockCleanupFailures        prometheus.Counter
	blocksMarked                *prometheus.CounterVec
	garbageCollectedBlocks      prometheus.Counter
}

func newCompactMetrics(reg *prometheus.Registry, deleteDelay time.Duration) *compactMetrics {
	_ = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_delete_delay_seconds",
		Help: "Configured delete delay in seconds.",
	}, func() float64 {
		return deleteDelay.Seconds()
	})

	m := &compactMetrics{}

	m.halted = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "thanos_compact_halted",
		Help: "Set to 1 if the compactor halted due to an unexpected error.",
	})
	m.halted.Set(0)
	m.retried = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_retries_total",
		Help: "Total number of retries after retriable compactor error.",
	})
	m.iterations = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_iterations_total",
		Help: "Total number of iterations that were executed successfully.",
	})
	m.cleanups = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_block_cleanup_loops_total",
		Help: "Total number of concurrent cleanup loops of partially uploaded blocks and marked blocks that were executed successfully.",
	})
	m.partialUploadDeleteAttempts = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_aborted_partial_uploads_deletion_attempts_total",
		Help: "Total number of started deletions of blocks that are assumed aborted and only partially uploaded.",
	})
	m.blocksCleaned = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_blocks_cleaned_total",
		Help: "Total number of blocks deleted in compactor.",
	})
	m.blockCleanupFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_block_cleanup_failures_total",
		Help: "Failures encountered while deleting blocks in compactor.",
	})
	m.blocksMarked = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_blocks_marked_total",
		Help: "Total number of blocks marked in compactor.",
	}, []string{"marker", "reason"})
	m.blocksMarked.WithLabelValues(metadata.NoCompactMarkFilename, metadata.OutOfOrderChunksNoCompactReason)
	m.blocksMarked.WithLabelValues(metadata.NoCompactMarkFilename, metadata.IndexSizeExceedingNoCompactReason)
	m.blocksMarked.WithLabelValues(metadata.DeletionMarkFilename, "")

	m.garbageCollectedBlocks = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collected_blocks_total",
		Help: "Total number of blocks marked for deletion by compactor.",
	})
	return m
}

func runCompact(
	g *run.Group,
	logger log.Logger,
	tracer opentracing.Tracer,
	reg *prometheus.Registry,
	component component.Component,
	conf compactConfig,
	flagsMap map[string]string,
) (rerr error) {
	deleteDelay := time.Duration(conf.deleteDelay)
	compactMetrics := newCompactMetrics(reg, deleteDelay)
	progressRegistry := compact.NewProgressRegistry(reg, logger)
	downsampleMetrics := newDownsampleMetrics(reg)

	ctx, cancel := context.WithCancel(context.Background())
	ctx = tracing.ContextWithTracer(ctx, tracer)
	ctx = objstoretracing.ContextWithTracer(ctx, tracer) // objstore tracing uses a different tracer key in context.

	defer func() {
		if rerr != nil {
			cancel()
		}
	}()

	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		prober.NewInstrumentation(component, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	srv := httpserver.New(logger, reg, component, httpProbe,
		httpserver.WithListen(conf.http.bindAddress),
		httpserver.WithGracePeriod(time.Duration(conf.http.gracePeriod)),
		httpserver.WithTLSConfig(conf.http.tlsConfig),
	)

	g.Add(func() error {
		statusProber.Healthy()

		return srv.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		srv.Shutdown(err)
	})

	confContentYaml, err := conf.objStore.Content()
	if err != nil {
		return err
	}

	var initialBucketConf client.BucketConfig
	if err := yaml.Unmarshal(confContentYaml, &initialBucketConf); err != nil {
		return errors.Wrap(err, "failed to parse initial bucket configuration")
	}

	tenantPrefixes, isMultiTenant, err := getTenantsForCompactor(ctx, logger, conf, confContentYaml, component)
	if err != nil {
		return errors.Wrap(err, "failed to get tenants for compactor")
	}

	relabelContentYaml, err := conf.selectorRelabelConf.Content()
	if err != nil {
		return errors.Wrap(err, "get content of relabel configuration")
	}

	relabelConfig, err := block.ParseRelabelConfig(relabelContentYaml, block.SelectorSupportedRelabelActions)
	if err != nil {
		return err
	}

	globalBkt, err := client.NewBucket(logger, confContentYaml, component.String(), nil)
	if err != nil {
		return errors.Wrap(err, "failed to create global bucket")
	}
	if conf.enableFolderDeletion {
		globalBkt, err = block.WrapWithAzDataLakeSdk(logger, confContentYaml, globalBkt)
		if err != nil {
			return errors.Wrap(err, "failed to wrap global bucket with Azure SDK")
		}
		level.Info(logger).Log("msg", "azdatalake sdk wrapper enabled for global bucket", "name", globalBkt.Name())
	}
	globalInsBkt := objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(globalBkt, extprom.WrapRegistererWithPrefix("thanos_", reg), globalBkt.Name()))

	// Ensure we close up everything properly.
	defer func() {
		if rerr != nil {
			runutil.CloseWithLogOnErr(logger, globalInsBkt, "global bucket client")
		}
	}()

	globalBlockLister, err := getBlockLister(logger, &conf, globalInsBkt)
	if err != nil {
		return errors.Wrap(err, "create global block lister")
	}
	globalBaseMetaFetcher, err := block.NewBaseFetcher(logger, conf.blockMetaFetchConcurrency, globalInsBkt, globalBlockLister, conf.dataDir, extprom.WrapRegistererWithPrefix("thanos_", reg))
	if err != nil {
		return errors.Wrap(err, "create global meta fetcher")
	}

	api := blocksAPI.NewBlocksAPI(logger, conf.webConf.disableCORS, conf.label, flagsMap, globalInsBkt)

	runWebServer(g, ctx, logger, cancel, reg, &conf, component, tracer, progressRegistry, globalBaseMetaFetcher, api, srv)

	for _, tenantPrefix := range tenantPrefixes {
		bucketConf := &client.BucketConfig{
			Type:   initialBucketConf.Type,
			Config: initialBucketConf.Config,
			Prefix: path.Join(initialBucketConf.Prefix, tenantPrefix),
		}
		level.Info(logger).Log("msg", "starting compaction loop for prefix", "prefix", bucketConf.Prefix)

		tenantConfYaml, err := yaml.Marshal(bucketConf)
		if err != nil {
			return errors.Wrap(err, "failed to marshal tenant bucket configuration")
		}

		bkt, err := getBucketForTenant(logger, isMultiTenant, tenantConfYaml, component, conf, bucketConf, globalBkt)
		if err != nil {
			return errors.Wrap(err, "failed to get bucket for tenant")
		}

		tenantReg, insBkt, tenantLogger, baseMetaFetcher, err := getTenantResources(logger, isMultiTenant, conf, reg, tenantPrefix, bkt, globalInsBkt, globalBaseMetaFetcher)
		if err != nil {
			return errors.Wrap(err, "failed to get tenant resources")
		}

		err = runCompactForTenant(g, ctx, tenantLogger, cancel, tenantReg, insBkt, deleteDelay, conf, relabelConfig, compactMetrics, progressRegistry, downsampleMetrics, baseMetaFetcher, tenantPrefix, api)

		if err != nil {
			return err
		}

		level.Info(tenantLogger).Log("msg", "compact node for tenant finished")
	}

	statusProber.Ready()
	return nil
}

func runCompactForTenant(
	g *run.Group,
	ctx context.Context,
	logger log.Logger,
	cancel context.CancelFunc,
	reg prometheus.Registerer,
	insBkt objstore.InstrumentedBucket,
	deleteDelay time.Duration,
	conf compactConfig,
	relabelConfig []*relabel.Config,
	compactMetrics *compactMetrics,
	progressRegistry *compact.ProgressRegistry,
	downsampleMetrics *DownsampleMetrics,
	baseMetaFetcher *block.BaseFetcher,
	tenant string,
	api *blocksAPI.BlocksAPI,
) error {
	// While fetching blocks, we filter out blocks that were marked for deletion by using IgnoreDeletionMarkFilter.
	// The delay of deleteDelay/2 is added to ensure we fetch blocks that are meant to be deleted but do not have a replacement yet.
	// This is to make sure compactor will not accidentally perform compactions with gap instead.
	ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(logger, insBkt, deleteDelay/2, conf.blockMetaFetchConcurrency)
	duplicateBlocksFilter := block.NewDeduplicateFilter(conf.blockMetaFetchConcurrency)
	noCompactMarkerFilter := compact.NewGatherNoCompactionMarkFilter(logger, insBkt, conf.blockMetaFetchConcurrency)
	noDownsampleMarkerFilter := downsample.NewGatherNoDownsampleMarkFilter(logger, insBkt, conf.blockMetaFetchConcurrency)
	labelShardedMetaFilter := block.NewLabelShardedMetaFilter(relabelConfig)
	consistencyDelayMetaFilter := block.NewConsistencyDelayMetaFilter(logger, conf.consistencyDelay, extprom.WrapRegistererWithPrefix("thanos_", reg))
	timePartitionMetaFilter := block.NewTimePartitionMetaFilter(conf.filterConf.MinTime, conf.filterConf.MaxTime)

	enableVerticalCompaction, dedupReplicaLabels := checkVerticalCompaction(logger, &conf)

	var sy *compact.Syncer
	{
		filters := []block.MetadataFilter{
			timePartitionMetaFilter,
			labelShardedMetaFilter,
			consistencyDelayMetaFilter,
			ignoreDeletionMarkFilter,
			block.NewReplicaLabelRemover(logger, dedupReplicaLabels),
			duplicateBlocksFilter,
			noCompactMarkerFilter,
		}
		if !conf.disableDownsampling {
			filters = append(filters, noDownsampleMarkerFilter)
		}
		// Make sure all compactor meta syncs are done through Syncer.SyncMeta for readability.
		cf := baseMetaFetcher.NewMetaFetcher(
			extprom.WrapRegistererWithPrefix("thanos_", reg), filters)
		cf.UpdateOnChange(func(blocks []metadata.Meta, err error) {
			api.SetLoaded(blocks, err)
		})

		// Still use blockViewerSyncBlockTimeout to retain original behavior before this upstream change:
		// https://github.com/databricks/thanos/commit/ab43b2b20cb42eca2668824a4084307216c6da2e#diff-6c2257b871fd1196514f664bc7e44cb21681215e0929710d0ad5ceea90b8e122R294
		// Otherwise Azure won't work due to its high latency
		var syncMetasTimeout = conf.blockViewerSyncBlockTimeout
		if !conf.wait {
			syncMetasTimeout = 0
		}
		var err error
		sy, err = compact.NewMetaSyncer(
			logger,
			reg,
			insBkt,
			cf,
			duplicateBlocksFilter,
			ignoreDeletionMarkFilter,
			compactMetrics.blocksMarked.WithLabelValues(metadata.DeletionMarkFilename, ""),
			compactMetrics.garbageCollectedBlocks,
			syncMetasTimeout,
		)
		if err != nil {
			return errors.Wrap(err, "create syncer")
		}
	}

	levels, err := getCompactionLevels(logger, &conf)
	if err != nil {
		return err
	}

	mergeFunc, err := getMergeFunc(&conf, dedupReplicaLabels)
	if err != nil {
		return errors.Wrap(err, "get merge func")
	}

	// Instantiate the compactor with different time slices. Timestamps in TSDB
	// are in milliseconds.
	comp, err := tsdb.NewLeveledCompactor(ctx, reg, logger, levels, downsample.NewPool(), mergeFunc)
	if err != nil {
		return errors.Wrap(err, "create compactor")
	}

	var compactDir, downsamplingDir string
	if tenant != "" {
		// In multi-tenant mode, each tenant gets its own working directory to avoid conflicts
		compactDir = path.Join(conf.dataDir, "compact", tenant)
		downsamplingDir = path.Join(conf.dataDir, "downsample", tenant)
	} else {
		// Single-tenant mode uses the base directories
		compactDir = path.Join(conf.dataDir, "compact")
		downsamplingDir = path.Join(conf.dataDir, "downsample")
	}

	if err := os.MkdirAll(compactDir, os.ModePerm); err != nil {
		return errors.Wrap(err, "create working compact directory")
	}

	if err := os.MkdirAll(downsamplingDir, os.ModePerm); err != nil {
		return errors.Wrap(err, "create working downsample directory")
	}

	grouper := compact.NewDefaultGrouper(
		logger,
		insBkt,
		conf.acceptMalformedIndex,
		enableVerticalCompaction,
		reg,
		compactMetrics.blocksMarked.WithLabelValues(metadata.DeletionMarkFilename, ""),
		compactMetrics.garbageCollectedBlocks,
		compactMetrics.blocksMarked.WithLabelValues(metadata.NoCompactMarkFilename, metadata.OutOfOrderChunksNoCompactReason),
		metadata.HashFunc(conf.hashFunc),
		conf.blockFilesConcurrency,
		conf.compactBlocksFetchConcurrency,
	)
	var planner compact.Planner

	tsdbPlanner := compact.NewPlanner(logger, levels, noCompactMarkerFilter)
	largeIndexFilterPlanner := compact.WithLargeTotalIndexSizeFilter(
		tsdbPlanner,
		insBkt,
		int64(conf.maxBlockIndexSize),
		compactMetrics.blocksMarked.WithLabelValues(metadata.NoCompactMarkFilename, metadata.IndexSizeExceedingNoCompactReason),
	)
	if enableVerticalCompaction {
		planner = compact.WithVerticalCompactionDownsampleFilter(largeIndexFilterPlanner, insBkt, compactMetrics.blocksMarked.WithLabelValues(metadata.NoCompactMarkFilename, metadata.DownsampleVerticalCompactionNoCompactReason))
	} else {
		planner = largeIndexFilterPlanner
	}
	blocksCleaner := compact.NewBlocksCleaner(logger, insBkt, ignoreDeletionMarkFilter, deleteDelay, compactMetrics.blocksCleaned, compactMetrics.blockCleanupFailures)
	compactor, err := compact.NewBucketCompactorWithCheckerAndCallback(
		logger,
		sy,
		grouper,
		planner,
		comp,
		compact.DefaultBlockDeletableChecker{},
		compact.NewOverlappingCompactionLifecycleCallback(reg, logger, conf.enableOverlappingRemoval),
		compactDir,
		insBkt,
		conf.compactionConcurrency,
		conf.skipBlockWithOutOfOrderChunks,
	)
	if err != nil {
		return errors.Wrap(err, "create bucket compactor")
	}

	retentionByResolution, retentionByTenant, err := getRetentionPolicies(logger, &conf)
	if err != nil {
		return errors.Wrap(err, "get retention policies")
	}

	var cleanMtx sync.Mutex
	// TODO(GiedriusS): we could also apply retention policies here but the logic would be a bit more complex.
	cleanPartialMarked := func(progress *compact.Progress) error {
		cleanMtx.Lock()
		defer cleanMtx.Unlock()
		defer progress.Idle()
		progress.Set(compact.SyncMeta)
		if err := sy.SyncMetas(ctx); err != nil {
			return errors.Wrap(err, "syncing metas")
		}

		progress.Set(compact.CleanBlocks)
		compact.BestEffortCleanAbortedPartialUploads(ctx, logger, sy.Partial(), insBkt, compactMetrics.partialUploadDeleteAttempts, compactMetrics.blocksCleaned, compactMetrics.blockCleanupFailures)
		if err := blocksCleaner.DeleteMarkedBlocks(ctx); err != nil {
			return errors.Wrap(err, "cleaning marked blocks")
		}
		compactMetrics.cleanups.Inc()

		return nil
	}

	compactMainFn := func(progress *compact.Progress) error {
		defer progress.Idle()
		// this should happen before any compaction to remove unnecessary process on backlogs beyond retention.
		if len(retentionByTenant) != 0 && len(sy.Metas()) == 0 {
			level.Info(logger).Log("msg", "sync before tenant retention due to no blocks")
			progress.Set(compact.SyncMeta)
			if err := sy.SyncMetas(ctx); err != nil {
				return errors.Wrap(err, "sync before tenant retention")
			}
		}

		progress.Set(compact.ApplyRetention)
		if err := compact.ApplyRetentionPolicyByTenant(ctx, logger, insBkt, sy.Metas(), retentionByTenant, compactMetrics.blocksMarked.WithLabelValues(metadata.DeletionMarkFilename, metadata.TenantRetentionExpired)); err != nil {
			return errors.Wrap(err, "retention by tenant failed")
		}

		if err := compactor.Compact(ctx, progress); err != nil {
			return errors.Wrap(err, "whole compaction error")
		}

		if !conf.disableDownsampling {
			// After all compactions are done, work down the downsampling backlog.
			// We run two passes of this to ensure that the 1h downsampling is generated
			// for 5m downsamplings created in the first run.
			level.Info(logger).Log("msg", "start first pass of downsampling")
			progress.Set(compact.SyncMeta)
			if err := sy.SyncMetas(ctx); err != nil {
				return errors.Wrap(err, "sync before first pass of downsampling")
			}
			progress.Set(compact.DownSampling)
			filteredMetas := sy.Metas()
			noDownsampleBlocks := noDownsampleMarkerFilter.NoDownsampleMarkedBlocks()
			for ul := range noDownsampleBlocks {
				delete(filteredMetas, ul)
			}

			for _, meta := range filteredMetas {
				resolutionLabel := meta.Thanos.ResolutionString()
				downsampleMetrics.downsamples.WithLabelValues(resolutionLabel)
				downsampleMetrics.downsampleFailures.WithLabelValues(resolutionLabel)
			}

			if err := downsampleBucket(
				ctx,
				logger,
				downsampleMetrics,
				insBkt,
				filteredMetas,
				downsamplingDir,
				conf.downsampleConcurrency,
				conf.blockFilesConcurrency,
				metadata.HashFunc(conf.hashFunc),
				conf.acceptMalformedIndex,
			); err != nil {
				return errors.Wrap(err, "first pass of downsampling failed")
			}

			level.Info(logger).Log("msg", "start second pass of downsampling")
			progress.Set(compact.SyncMeta)
			if err := sy.SyncMetas(ctx); err != nil {
				return errors.Wrap(err, "sync before second pass of downsampling")
			}

			// Regenerate the filtered list of blocks after the sync,
			// to include the blocks created by the first pass.
			filteredMetas = sy.Metas()
			noDownsampleBlocks = noDownsampleMarkerFilter.NoDownsampleMarkedBlocks()
			for ul := range noDownsampleBlocks {
				delete(filteredMetas, ul)
			}

			if err := downsampleBucket(
				ctx,
				logger,
				downsampleMetrics,
				insBkt,
				filteredMetas,
				downsamplingDir,
				conf.downsampleConcurrency,
				conf.blockFilesConcurrency,
				metadata.HashFunc(conf.hashFunc),
				conf.acceptMalformedIndex,
			); err != nil {
				return errors.Wrap(err, "second pass of downsampling failed")
			}

			level.Info(logger).Log("msg", "downsampling iterations done")
		} else {
			level.Info(logger).Log("msg", "downsampling was explicitly disabled")
		}

		// TODO(bwplotka): Find a way to avoid syncing if no op was done.
		progress.Set(compact.SyncMeta)
		if err := sy.SyncMetas(ctx); err != nil {
			return errors.Wrap(err, "sync before retention")
		}

		progress.Set(compact.ApplyRetention)
		if err := compact.ApplyRetentionPolicyByResolution(ctx, logger, insBkt, sy.Metas(), retentionByResolution, compactMetrics.blocksMarked.WithLabelValues(metadata.DeletionMarkFilename, "")); err != nil {
			return errors.Wrap(err, "retention failed")
		}

		return cleanPartialMarked(progress)
	}

	g.Add(func() error {
		defer runutil.CloseWithLogOnErr(logger, insBkt, "bucket client")

		if !conf.wait {
			return compactMainFn(progressRegistry.Get(compact.Main))
		}

		// --wait=true is specified.
		return runutil.Repeat(conf.waitInterval, ctx.Done(), func() error {
			err := compactMainFn(progressRegistry.Get(compact.Main))
			if err == nil {
				compactMetrics.iterations.Inc()
				return nil
			}

			// The HaltError type signals that we hit a critical bug and should block
			// for investigation. You should alert on this being halted.
			if compact.IsHaltError(err) {
				if conf.haltOnError {
					level.Error(logger).Log("msg", "critical error detected; halting", "err", err)
					compactMetrics.halted.Set(1)
					select {}
				} else {
					return errors.Wrap(err, "critical error detected")
				}
			}

			// The RetryError signals that we hit an retriable error (transient error, no connection).
			// You should alert on this being triggered too frequently.
			if compact.IsRetryError(err) {
				level.Error(logger).Log("msg", "retriable error", "err", err)
				compactMetrics.retried.Inc()
				// TODO(bplotka): use actual "retry()" here instead of waiting 5 minutes?
				return nil
			}

			return errors.Wrap(err, "error executing compaction")
		})
	}, func(error) {
		cancel()
	})

	runCleanup(g, ctx, logger, cancel, reg, &conf, progressRegistry, compactMetrics, tsdbPlanner, sy, retentionByResolution, cleanPartialMarked, grouper, tenant)

	return nil
}

func runWebServer(
	g *run.Group,
	ctx context.Context,
	logger log.Logger,
	cancel context.CancelFunc,
	reg *prometheus.Registry,
	conf *compactConfig,
	component component.Component,
	tracer opentracing.Tracer,
	progressRegistry *compact.ProgressRegistry,
	baseMetaFetcher *block.BaseFetcher,
	api *blocksAPI.BlocksAPI,
	srv *httpserver.Server,
) {
	if conf.wait {
		if !conf.disableWeb {
			r := route.New()

			ins := extpromhttp.NewInstrumentationMiddleware(reg, nil)

			global := ui.NewBucketUI(logger, conf.webConf.externalPrefix, conf.webConf.prefixHeaderName, component)
			global.Register(r, ins)

			// Configure Request Logging for HTTP calls.
			opts := []logging.Option{logging.WithDecider(func(_ string, _ error) logging.Decision {
				return logging.NoLogCall
			})}
			logMiddleware := logging.NewHTTPServerMiddleware(logger, opts...)
			api.Register(r.WithPrefix("/api/v1"), tracer, logger, ins, logMiddleware)

			// Separate fetcher for global view.
			// TODO(bwplotka): Allow Bucket UI to visualize the state of the block as well.
			f := baseMetaFetcher.NewMetaFetcher(extprom.WrapRegistererWithPrefix("thanos_bucket_ui", reg), nil, "component", "globalBucketUI")
			f.UpdateOnChange(func(blocks []metadata.Meta, err error) {
				api.SetGlobal(blocks, err)
			})

			srv.Handle("/", r)

			g.Add(func() error {
				iterCtx, iterCancel := context.WithTimeout(ctx, conf.blockViewerSyncBlockTimeout)
				_, _, _ = f.Fetch(iterCtx)
				iterCancel()

				// For /global state make sure to fetch periodically.
				return runutil.Repeat(conf.blockViewerSyncBlockInterval, ctx.Done(), func() error {
					return runutil.RetryWithLog(logger, time.Minute, ctx.Done(), func() error {
						progress := progressRegistry.Get(compact.Web)
						defer progress.Idle()
						iterCtx, iterCancel := context.WithTimeout(ctx, conf.blockViewerSyncBlockTimeout)
						defer iterCancel()
						progress.Set(compact.SyncMeta)
						_, _, err := f.Fetch(iterCtx)
						return err
					})
				})
			}, func(error) {
				cancel()
			})
		}
	}
}

func runCleanup(
	g *run.Group,
	ctx context.Context,
	logger log.Logger,
	cancel context.CancelFunc,
	reg prometheus.Registerer,
	conf *compactConfig,
	progressRegistry *compact.ProgressRegistry,
	compactMetrics *compactMetrics,
	tsdbPlanner compact.Planner,
	sy *compact.Syncer,
	retentionByResolution map[compact.ResolutionLevel]time.Duration,
	cleanPartialMarked func(*compact.Progress) error,
	grouper *compact.DefaultGrouper,
	tenant string,
) {
	if conf.wait {
		// Periodically remove partial blocks and blocks marked for deletion
		// since one iteration potentially could take a long time.
		if conf.cleanupBlocksInterval > 0 {
			g.Add(func() error {
				return runutil.Repeat(conf.cleanupBlocksInterval, ctx.Done(), func() error {
					err := cleanPartialMarked(progressRegistry.Get(compact.Cleanup))
					if err != nil && compact.IsRetryError(err) {
						// The RetryError signals that we hit an retriable error (transient error, no connection).
						// You should alert on this being triggered too frequently.
						level.Error(logger).Log("msg", "retriable error", "err", err)
						compactMetrics.retried.Inc()

						return nil
					}

					return err
				})
			}, func(error) {
				cancel()
			})
		}

		// Periodically calculate the progress of compaction, downsampling and retention.
		if conf.progressCalculateInterval > 0 {
			g.Add(func() error {
				ps := compact.NewCompactionProgressCalculator(reg, tsdbPlanner, tenant)
				rs := compact.NewRetentionProgressCalculator(reg, retentionByResolution, tenant)
				var ds *compact.DownsampleProgressCalculator
				if !conf.disableDownsampling {
					ds = compact.NewDownsampleProgressCalculator(reg, tenant)
				}

				return runutil.Repeat(conf.progressCalculateInterval, ctx.Done(), func() error {
					progress := progressRegistry.Get(compact.Calculate)
					defer progress.Idle()
					progress.Set(compact.SyncMeta)
					if err := sy.SyncMetas(ctx); err != nil {
						// The RetryError signals that we hit an retriable error (transient error, no connection).
						// You should alert on this being triggered too frequently.
						if compact.IsRetryError(err) {
							level.Error(logger).Log("msg", "retriable error", "err", err)
							compactMetrics.retried.Inc()

							return nil
						}

						return errors.Wrapf(err, "could not sync metas")
					}

					metas := sy.Metas()
					progress.Set(compact.Grouping)
					groups, err := grouper.Groups(metas)
					if err != nil {
						return errors.Wrapf(err, "could not group metadata for compaction")
					}
					progress.Set(compact.CalculateProgress)
					if err = ps.ProgressCalculate(ctx, groups); err != nil {
						return errors.Wrapf(err, "could not calculate compaction progress")
					}

					progress.Set(compact.Grouping)
					retGroups, err := grouper.Groups(metas)
					if err != nil {
						return errors.Wrapf(err, "could not group metadata for retention")
					}

					progress.Set(compact.CalculateProgress)
					if err = rs.ProgressCalculate(ctx, retGroups); err != nil {
						return errors.Wrapf(err, "could not calculate retention progress")
					}

					if !conf.disableDownsampling {
						progress.Set(compact.Grouping)
						groups, err = grouper.Groups(metas)
						if err != nil {
							return errors.Wrapf(err, "could not group metadata into downsample groups")
						}
						progress.Set(compact.CalculateProgress)
						if err := ds.ProgressCalculate(ctx, groups); err != nil {
							return errors.Wrapf(err, "could not calculate downsampling progress")
						}
					}

					return nil
				})
			}, func(err error) {
				cancel()
			})
		}
	}
}

func getTenantsForCompactor(ctx context.Context, logger log.Logger, conf compactConfig, confContentYaml []byte, component component.Component) ([]string, bool, error) {
	var tenantPrefixes []string
	var isMultiTenant bool

	if conf.enableTenantPathPrefix {
		isMultiTenant = true

		hostname := os.Getenv("HOSTNAME")
		ordinal, err := extractOrdinalFromHostname(hostname)
		if err != nil {
			return nil, true, errors.Wrapf(err, "failed to extract ordinal from hostname %s", hostname)
		}

		totalShards := conf.replicas / conf.replicationFactor
		if conf.replicas%conf.replicationFactor != 0 || conf.replicationFactor <= 0 {
			return nil, true, errors.Errorf("replicas %d must be divisible by replication factor %d and total shards must be greater than 0", conf.replicas, conf.replicationFactor)
		}

		if ordinal >= totalShards {
			return nil, true, errors.Errorf("ordinal %d is greater than total shards %d", ordinal, totalShards)
		}

		tenantWeightsPath := conf.tenantWeights.Path()
		if tenantWeightsPath == "" {
			return nil, true, errors.New("tenant weights file is not set")
		}

		discoveryBkt, err := client.NewBucket(logger, confContentYaml, component.String(), nil)
		if err != nil {
			return nil, true, errors.Wrapf(err, "failed to create discovery bucket")
		}

		level.Info(logger).Log("msg", "setting up tenant partitioning", "ordinal", ordinal, "total_shards", totalShards)

		tenantAssignments, err := compact.SetupTenantPartitioning(ctx, discoveryBkt, logger, tenantWeightsPath, conf.commonPathPrefix, totalShards)
		runutil.CloseWithLogOnErr(logger, discoveryBkt, "discovery bucket")
		if err != nil {
			return nil, true, errors.Wrap(err, "failed to setup tenant partitioning")
		}

		assignedTenants := tenantAssignments[ordinal]
		if len(assignedTenants) == 0 {
			level.Warn(logger).Log("msg", "no tenants assigned to this shard", "ordinal", ordinal)
		}

		// Deduplicate tenants to avoid duplicate metric registration
		seenTenants := make(map[string]bool)
		for _, tenant := range assignedTenants {
			tenantPrefix := path.Join(conf.commonPathPrefix, tenant)
			if !seenTenants[tenantPrefix] {
				seenTenants[tenantPrefix] = true
				tenantPrefixes = append(tenantPrefixes, tenantPrefix)
			}
		}

		level.Info(logger).Log("msg", "tenant partitioning setup complete", "tenant_prefixes", strings.Join(tenantPrefixes, ","))
	} else {
		isMultiTenant = false
		tenantPrefixes = []string{""}
		level.Info(logger).Log("msg", "single tenant mode")
	}
	return tenantPrefixes, isMultiTenant, nil
}

func getBucketForTenant(logger log.Logger, isMultiTenant bool, tenantConfYaml []byte, component component.Component, conf compactConfig, bucketConf *client.BucketConfig, globalBkt objstore.Bucket) (objstore.Bucket, error) {
	if isMultiTenant {
		bkt, err := client.NewBucket(logger, tenantConfYaml, component.String(), nil)
		if conf.enableFolderDeletion {
			bkt, err = block.WrapWithAzDataLakeSdk(logger, tenantConfYaml, bkt)
			level.Info(logger).Log("msg", "azdatalake sdk wrapper enabled", "prefix", bucketConf.Prefix, "name", bkt.Name())
		}
		return bkt, err
	}
	return globalBkt, nil
}

func getTenantResources(
	logger log.Logger,
	isMultiTenant bool,
	conf compactConfig,
	reg prometheus.Registerer,
	tenantPrefix string,
	bkt objstore.Bucket,
	globalInsBkt objstore.InstrumentedBucket,
	globalBaseMetaFetcher *block.BaseFetcher,
) (prometheus.Registerer, objstore.InstrumentedBucket, log.Logger, *block.BaseFetcher, error) {
	var tenantReg prometheus.Registerer
	var insBkt objstore.InstrumentedBucket
	var tenantLogger log.Logger
	var baseMetaFetcher *block.BaseFetcher

	if isMultiTenant {
		// Use idempotent registerer to ignore duplicate metric registrations across tenants.
		// Components extract tenant from block metadata and use it as a variable label.
		tenantReg = &idempotentRegisterer{Registerer: reg}
		tenantLogger = log.With(logger, "tenant", tenantPrefix)
		// Only wrap with tracing, not metrics (to avoid duplicate bucket metric registration)
		insBkt = objstoretracing.WrapWithTraces(bkt)

		// Create tenant-scoped block lister and fetcher so each tenant only sees their own blocks
		tenantBlockLister, err := getBlockLister(tenantLogger, &conf, insBkt)
		if err != nil {
			return nil, nil, nil, nil, errors.Wrap(err, "create tenant block lister")
		}
		baseMetaFetcher, err = block.NewBaseFetcher(tenantLogger, conf.blockMetaFetchConcurrency, insBkt, tenantBlockLister, conf.dataDir, extprom.WrapRegistererWithPrefix("thanos_", tenantReg))
		if err != nil {
			return nil, nil, nil, nil, errors.Wrap(err, "create tenant meta fetcher")
		}
	} else {
		tenantReg = reg
		tenantLogger = logger
		insBkt = globalInsBkt
		baseMetaFetcher = globalBaseMetaFetcher
	}
	return tenantReg, insBkt, tenantLogger, baseMetaFetcher, nil
}

func getBlockLister(logger log.Logger, conf *compactConfig, insBkt objstore.InstrumentedBucketReader) (block.Lister, error) {
	switch syncStrategy(conf.blockListStrategy) {
	case concurrentDiscovery:
		return block.NewConcurrentLister(logger, insBkt), nil
	case recursiveDiscovery:
		return block.NewRecursiveLister(logger, insBkt), nil
	default:
		return nil, errors.Errorf("unknown sync strategy %s", conf.blockListStrategy)
	}
}

func checkVerticalCompaction(logger log.Logger, conf *compactConfig) (bool, []string) {
	enableVerticalCompaction := conf.enableVerticalCompaction
	dedupReplicaLabels := strutil.ParseFlagLabels(conf.dedupReplicaLabels)

	if len(dedupReplicaLabels) > 0 {
		enableVerticalCompaction = true
		level.Info(logger).Log(
			"msg", "deduplication.replica-label specified, enabling vertical compaction", "dedupReplicaLabels", strings.Join(dedupReplicaLabels, ","),
		)
	}
	if enableVerticalCompaction {
		level.Info(logger).Log(
			"msg", "vertical compaction is enabled", "compact.enable-vertical-compaction", fmt.Sprintf("%v", conf.enableVerticalCompaction),
		)
	}
	return enableVerticalCompaction, dedupReplicaLabels
}

func getCompactionLevels(logger log.Logger, conf *compactConfig) ([]int64, error) {
	levels, err := compactions.levels(conf.maxCompactionLevel)
	if err != nil {
		return nil, errors.Wrap(err, "get compaction levels")
	}

	if conf.maxCompactionLevel < compactions.maxLevel() {
		level.Warn(logger).Log("msg", "Max compaction level is lower than should be", "current", conf.maxCompactionLevel, "default", compactions.maxLevel())
	}

	return levels, nil
}

func getMergeFunc(conf *compactConfig, dedupReplicaLabels []string) (storage.VerticalChunkSeriesMergeFunc, error) {
	var mergeFunc storage.VerticalChunkSeriesMergeFunc
	switch conf.dedupFunc {
	case compact.DedupAlgorithmPenalty:
		mergeFunc = dedup.NewChunkSeriesMerger()

		if len(dedupReplicaLabels) == 0 {
			return nil, errors.New("penalty based deduplication needs at least one replica label specified")
		}
	case "":
		mergeFunc = storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge)

	default:
		return nil, errors.Errorf("unsupported deduplication func, got %s", conf.dedupFunc)
	}

	return mergeFunc, nil
}

func getRetentionPolicies(logger log.Logger, conf *compactConfig) (map[compact.ResolutionLevel]time.Duration, map[string]compact.RetentionPolicy, error) {
	retentionByResolution := map[compact.ResolutionLevel]time.Duration{
		compact.ResolutionLevelRaw: time.Duration(conf.retentionRaw),
		compact.ResolutionLevel5m:  time.Duration(conf.retentionFiveMin),
		compact.ResolutionLevel1h:  time.Duration(conf.retentionOneHr),
	}

	if retentionByResolution[compact.ResolutionLevelRaw].Milliseconds() != 0 {
		// If downsampling is enabled, error if raw retention is not sufficient for downsampling to occur (upper bound 10 days for 1h resolution)
		if !conf.disableDownsampling && retentionByResolution[compact.ResolutionLevelRaw].Milliseconds() < downsample.ResLevel1DownsampleRange {
			return nil, nil, errors.New("raw resolution must be higher than the minimum block size after which 5m resolution downsampling will occur (40 hours)")
		}
		level.Info(logger).Log("msg", "retention policy of raw samples is enabled", "duration", retentionByResolution[compact.ResolutionLevelRaw])
	}
	if retentionByResolution[compact.ResolutionLevel5m].Milliseconds() != 0 {
		// If retention is lower than minimum downsample range, then no downsampling at this resolution will be persisted
		if !conf.disableDownsampling && retentionByResolution[compact.ResolutionLevel5m].Milliseconds() < downsample.ResLevel2DownsampleRange {
			return nil, nil, errors.New("5m resolution retention must be higher than the minimum block size after which 1h resolution downsampling will occur (10 days)")
		}
		level.Info(logger).Log("msg", "retention policy of 5 min aggregated samples is enabled", "duration", retentionByResolution[compact.ResolutionLevel5m])
	}
	if retentionByResolution[compact.ResolutionLevel1h].Milliseconds() != 0 {
		level.Info(logger).Log("msg", "retention policy of 1 hour aggregated samples is enabled", "duration", retentionByResolution[compact.ResolutionLevel1h])
	}

	retentionByTenant, err := compact.ParesRetentionPolicyByTenant(logger, *conf.retentionTenants)
	if err != nil {
		level.Error(logger).Log("msg", "failed to parse retention policy by tenant", "err", err)
		return nil, nil, err
	}

	return retentionByResolution, retentionByTenant, nil
}

func extractOrdinalFromHostname(hostname string) (int, error) {
	parts := strings.Split(hostname, "-")
	return strconv.Atoi(parts[len(parts)-1])
}

type compactConfig struct {
	haltOnError                                    bool
	acceptMalformedIndex                           bool
	maxCompactionLevel                             int
	http                                           httpConfig
	dataDir                                        string
	objStore                                       extflag.PathOrContent
	consistencyDelay                               time.Duration
	retentionRaw, retentionFiveMin, retentionOneHr model.Duration
	retentionTenants                               *[]string
	wait                                           bool
	waitInterval                                   time.Duration
	disableDownsampling                            bool
	blockListStrategy                              string
	blockMetaFetchConcurrency                      int
	blockFilesConcurrency                          int
	blockViewerSyncBlockInterval                   time.Duration
	blockViewerSyncBlockTimeout                    time.Duration
	cleanupBlocksInterval                          time.Duration
	compactionConcurrency                          int
	downsampleConcurrency                          int
	compactBlocksFetchConcurrency                  int
	deleteDelay                                    model.Duration
	dedupReplicaLabels                             []string
	selectorRelabelConf                            extflag.PathOrContent
	disableWeb                                     bool
	webConf                                        webConfig
	label                                          string
	maxBlockIndexSize                              units.Base2Bytes
	hashFunc                                       string
	enableVerticalCompaction                       bool
	enableOverlappingRemoval                       bool
	enableFolderDeletion                           bool
	dedupFunc                                      string
	skipBlockWithOutOfOrderChunks                  bool
	progressCalculateInterval                      time.Duration
	filterConf                                     *store.FilterConfig
	disableAdminOperations                         bool
	tenantWeights                                  extflag.PathOrContent
	replicas                                       int
	replicationFactor                              int
	commonPathPrefix                               string
	enableTenantPathPrefix                         bool
}

func (cc *compactConfig) registerFlag(cmd extkingpin.FlagClause) {
	cmd.Flag("debug.halt-on-error", "Halt the process if a critical compaction error is detected.").
		Hidden().Default("true").BoolVar(&cc.haltOnError)
	cmd.Flag("debug.accept-malformed-index",
		"Compaction and downsampling index verification will ignore out of order label names.").
		Hidden().Default("false").BoolVar(&cc.acceptMalformedIndex)
	cmd.Flag("debug.max-compaction-level", fmt.Sprintf("Maximum compaction level, default is %d: %s", compactions.maxLevel(), compactions.String())).
		Hidden().Default(strconv.Itoa(compactions.maxLevel())).IntVar(&cc.maxCompactionLevel)

	cc.http.registerFlag(cmd)

	cmd.Flag("data-dir", "Data directory in which to cache blocks and process compactions.").
		Default("./data").StringVar(&cc.dataDir)

	cc.objStore = *extkingpin.RegisterCommonObjStoreFlags(cmd, "", false)

	cmd.Flag("consistency-delay", fmt.Sprintf("Minimum age of fresh (non-compacted) blocks before they are being processed. Malformed blocks older than the maximum of consistency-delay and %v will be removed.", compact.PartialUploadThresholdAge)).
		Default("30m").DurationVar(&cc.consistencyDelay)

	cmd.Flag("retention.resolution-raw",
		"How long to retain raw samples in bucket. Setting this to 0d will retain samples of this resolution forever").
		Default("0d").SetValue(&cc.retentionRaw)
	cmd.Flag("retention.resolution-5m", "How long to retain samples of resolution 1 (5 minutes) in bucket. Setting this to 0d will retain samples of this resolution forever").
		Default("0d").SetValue(&cc.retentionFiveMin)
	cmd.Flag("retention.resolution-1h", "How long to retain samples of resolution 2 (1 hour) in bucket. Setting this to 0d will retain samples of this resolution forever").
		Default("0d").SetValue(&cc.retentionOneHr)
	cc.retentionTenants = cmd.Flag("retention.tenant", "How long to retain samples in bucket per tenant. Setting this to 0d will retain samples of this resolution forever").Strings()

	// TODO(kakkoyun, pgough): https://github.com/thanos-io/thanos/issues/2266.
	cmd.Flag("wait", "Do not exit after all compactions have been processed and wait for new work.").
		Short('w').BoolVar(&cc.wait)
	cmd.Flag("wait-interval", "Wait interval between consecutive compaction runs and bucket refreshes. Only works when --wait flag specified.").
		Default("5m").DurationVar(&cc.waitInterval)

	cmd.Flag("downsampling.disable", "Disables downsampling. This is not recommended "+
		"as querying long time ranges without non-downsampled data is not efficient and useful e.g it is not possible to render all samples for a human eye anyway").
		Default("false").BoolVar(&cc.disableDownsampling)

	strategies := strings.Join([]string{string(concurrentDiscovery), string(recursiveDiscovery)}, ", ")
	cmd.Flag("block-discovery-strategy", "One of "+strategies+". When set to concurrent, stores will concurrently issue one call per directory to discover active blocks in the bucket. The recursive strategy iterates through all objects in the bucket, recursively traversing into each directory. This avoids N+1 calls at the expense of having slower bucket iterations.").
		Default(string(concurrentDiscovery)).StringVar(&cc.blockListStrategy)
	cmd.Flag("block-meta-fetch-concurrency", "Number of goroutines to use when fetching block metadata from object storage.").
		Default("32").IntVar(&cc.blockMetaFetchConcurrency)
	cmd.Flag("block-files-concurrency", "Number of goroutines to use when fetching/uploading block files from object storage.").
		Default("1").IntVar(&cc.blockFilesConcurrency)
	cmd.Flag("block-viewer.global.sync-block-interval", "Repeat interval for syncing the blocks between local and remote view for /global Block Viewer UI.").
		Default("1m").DurationVar(&cc.blockViewerSyncBlockInterval)
	cmd.Flag("block-viewer.global.sync-block-timeout", "Maximum time for syncing the blocks between local and remote view for /global Block Viewer UI.").
		Default("5m").DurationVar(&cc.blockViewerSyncBlockTimeout)
	cmd.Flag("compact.cleanup-interval", "How often we should clean up partially uploaded blocks and blocks with deletion mark in the background when --wait has been enabled. Setting it to \"0s\" disables it - the cleaning will only happen at the end of an iteration.").
		Default("5m").DurationVar(&cc.cleanupBlocksInterval)
	cmd.Flag("compact.progress-interval", "Frequency of calculating the compaction progress in the background when --wait has been enabled. Setting it to \"0s\" disables it. Now compaction, downsampling and retention progress are supported.").
		Default("5m").DurationVar(&cc.progressCalculateInterval)

	cmd.Flag("compact.concurrency", "Number of goroutines to use when compacting groups.").
		Default("1").IntVar(&cc.compactionConcurrency)
	cmd.Flag("compact.blocks-fetch-concurrency", "Number of goroutines to use when download block during compaction.").
		Default("1").IntVar(&cc.compactBlocksFetchConcurrency)
	cmd.Flag("downsample.concurrency", "Number of goroutines to use when downsampling blocks.").
		Default("1").IntVar(&cc.downsampleConcurrency)

	cmd.Flag("delete-delay", "Time before a block marked for deletion is deleted from bucket. "+
		"If delete-delay is non zero, blocks will be marked for deletion and compactor component will delete blocks marked for deletion from the bucket. "+
		"If delete-delay is 0, blocks will be deleted straight away. "+
		"Note that deleting blocks immediately can cause query failures, if store gateway still has the block loaded, "+
		"or compactor is ignoring the deletion because it's compacting the block at the same time.").
		Default("48h").SetValue(&cc.deleteDelay)

	cmd.Flag("compact.enable-vertical-compaction", "Experimental. When set to true, compactor will allow overlaps and perform **irreversible** vertical compaction. See https://thanos.io/tip/components/compact.md/#vertical-compactions to read more. "+
		"Please note that by default this uses a NAIVE algorithm for merging. If you need a different deduplication algorithm (e.g one that works well with Prometheus replicas), please set it via --deduplication.func."+
		"NOTE: This flag is ignored and (enabled) when --deduplication.replica-label flag is set.").
		Hidden().Default("false").BoolVar(&cc.enableVerticalCompaction)

	cmd.Flag("compact.enable-overlapping-removal", "In house flag to remove overlapping blocks. Turn this on to fix https://github.com/thanos-io/thanos/issues/6775.").
		Default("false").BoolVar(&cc.enableOverlappingRemoval)

	cmd.Flag("compact.enable-folder-deletion", "Support Azure Blob HNS folder deletion. This is a workaround for Azure Blob HNS folder deletion issue.").
		Default("false").BoolVar(&cc.enableFolderDeletion)

	cmd.Flag("deduplication.func", "Experimental. Deduplication algorithm for merging overlapping blocks. "+
		"Possible values are: \"\", \"penalty\". If no value is specified, the default compact deduplication merger is used, which performs 1:1 deduplication for samples. "+
		"When set to penalty, penalty based deduplication algorithm will be used. At least one replica label has to be set via --deduplication.replica-label flag.").
		Default("").EnumVar(&cc.dedupFunc, compact.DedupAlgorithmPenalty, "")

	cmd.Flag("deduplication.replica-label", "Experimental. Label to treat as a replica indicator of blocks that can be deduplicated (repeated flag). This will merge multiple replica blocks into one. This process is irreversible. "+
		"Flag may be specified multiple times as well as a comma separated list of labels. "+
		"When one or more labels are set, compactor will ignore the given labels so that vertical compaction can merge the blocks."+
		"Please note that by default this uses a NAIVE algorithm for merging which works well for deduplication of blocks with **precisely the same samples** like produced by Receiver replication."+
		"If you need a different deduplication algorithm (e.g one that works well with Prometheus replicas), please set it via --deduplication.func.").
		StringsVar(&cc.dedupReplicaLabels)

	// TODO(bwplotka): This is short term fix for https://github.com/thanos-io/thanos/issues/1424, replace with vertical block sharding https://github.com/thanos-io/thanos/pull/3390.
	cmd.Flag("compact.block-max-index-size", "Maximum index size for the resulted block during any compaction. Note that"+
		"total size is approximated in worst case. If the block that would be resulted from compaction is estimated to exceed this number, biggest source"+
		"block is marked for no compaction (no-compact-mark.json is uploaded) which causes this block to be excluded from any compaction. "+
		"Default is due to https://github.com/thanos-io/thanos/issues/1424, but it's overall recommended to keeps block size to some reasonable size.").
		Hidden().Default("64GB").BytesVar(&cc.maxBlockIndexSize)

	cmd.Flag("compact.skip-block-with-out-of-order-chunks", "When set to true, mark blocks containing index with out-of-order chunks for no compact instead of halting the compaction").
		Hidden().Default("false").BoolVar(&cc.skipBlockWithOutOfOrderChunks)

	cmd.Flag("hash-func", "Specify which hash function to use when calculating the hashes of produced files. If no function has been specified, it does not happen. This permits avoiding downloading some files twice albeit at some performance cost. Possible values are: \"\", \"SHA256\".").
		Default("").EnumVar(&cc.hashFunc, "SHA256", "")

	cc.filterConf = &store.FilterConfig{}
	cmd.Flag("min-time", "Start of time range limit to compact. Thanos Compactor will compact only blocks, which happened later than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z").SetValue(&cc.filterConf.MinTime)
	cmd.Flag("max-time", "End of time range limit to compact. Thanos Compactor will compact only blocks, which happened earlier than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("9999-12-31T23:59:59Z").SetValue(&cc.filterConf.MaxTime)

	cmd.Flag("web.disable", "Disable Block Viewer UI.").Default("false").BoolVar(&cc.disableWeb)

	cc.selectorRelabelConf = *extkingpin.RegisterSelectorRelabelFlags(cmd)

	cc.tenantWeights = *extflag.RegisterPathOrContent(cmd, "compact.tenant-weights", "YAML file that contains the tenant weights for tenant partitioning.", extflag.WithEnvSubstitution())

	cmd.Flag("compact.replicas", "Total replicas of the stateful set.").
		Default("1").IntVar(&cc.replicas)

	cmd.Flag("compact.replication-factor", "Replication factor of the stateful set.").
		Default("1").IntVar(&cc.replicationFactor)

	cmd.Flag("compact.common-path-prefix", "Common path prefix for tenant discovery when using tenant partitioning. This is the prefix before the tenant name in the object storage path.").
		Default("v1/raw/").StringVar(&cc.commonPathPrefix)

	cmd.Flag("compact.enable-tenant-path-prefix", "Enable tenant path prefix mode for backward compatibility. When disabled, compactor runs in single-tenant mode.").
		Default("false").BoolVar(&cc.enableTenantPathPrefix)

	cc.webConf.registerFlag(cmd)

	cmd.Flag("bucket-web-label", "External block label to use as group title in the bucket web UI").StringVar(&cc.label)

	cmd.Flag("disable-admin-operations", "Disable UI/API admin operations like marking blocks for deletion and no compaction.").Default("false").BoolVar(&cc.disableAdminOperations)
}
