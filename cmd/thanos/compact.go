package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/dedup"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"gopkg.in/alecthomas/kingpin.v2"
)

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

func registerCompact(m map[string]setupFunc, app *kingpin.Application) {
	cmd := app.Command(component.Compact.String(), "continuously compacts blocks in an object store bucket")

	haltOnError := cmd.Flag("debug.halt-on-error", "Halt the process if a critical compaction error is detected.").
		Hidden().Default("true").Bool()
	acceptMalformedIndex := cmd.Flag("debug.accept-malformed-index",
		"Compaction index verification will ignore out of order label names.").
		Hidden().Default("false").Bool()

	httpAddr, httpGracePeriod := regHTTPFlags(cmd)

	dataDir := cmd.Flag("data-dir", "Data directory in which to cache blocks and process compactions.").
		Default("./data").String()

	objStoreConfig := regCommonObjStoreFlags(cmd, "", true)

	consistencyDelay := modelDuration(cmd.Flag("consistency-delay", fmt.Sprintf("Minimum age of fresh (non-compacted) blocks before they are being processed. Malformed blocks older than the maximum of consistency-delay and %s will be removed.", compact.MinimumAgeForRemoval)).
		Default("30m"))

	retentionRaw := modelDuration(cmd.Flag("retention.resolution-raw", "How long to retain raw samples in bucket. 0d - disables this retention").Default("0d"))
	retention5m := modelDuration(cmd.Flag("retention.resolution-5m", "How long to retain samples of resolution 1 (5 minutes) in bucket. 0d - disables this retention").Default("0d"))
	retention1h := modelDuration(cmd.Flag("retention.resolution-1h", "How long to retain samples of resolution 2 (1 hour) in bucket. 0d - disables this retention").Default("0d"))

	wait := cmd.Flag("wait", "Do not exit after all compactions have been processed and wait for new work.").
		Short('w').Bool()

	generateMissingIndexCacheFiles := cmd.Flag("index.generate-missing-cache-file", "If enabled, on startup compactor runs an on-off job that scans all the blocks to find all blocks with missing index cache file. It generates those if needed and upload.").
		Hidden().Default("false").Bool()

	disableDownsampling := cmd.Flag("downsampling.disable", "Disables downsampling. This is not recommended "+
		"as querying long time ranges without non-downsampled data is not efficient and useful e.g it is not possible to render all samples for a human eye anyway").
		Default("false").Bool()

	maxCompactionLevel := cmd.Flag("debug.max-compaction-level", fmt.Sprintf("Maximum compaction level, default is %d: %s", compactions.maxLevel(), compactions.String())).
		Hidden().Default(strconv.Itoa(compactions.maxLevel())).Int()

	blockSyncConcurrency := cmd.Flag("block-sync-concurrency", "Number of goroutines to use when syncing block metadata from object storage.").
		Default("20").Int()

	compactionConcurrency := cmd.Flag("compact.concurrency", "Number of goroutines to use when compacting groups.").
		Default("1").Int()

	enableDedup := cmd.Flag("enable-dedup", "Enable dedup function, but effect depends on 'dedup.replica-label' config").Default("false").Bool()
	dedupReplicaLabel := cmd.Flag("dedup.replica-label", "Label to treat as a replica indicator along which data is deduplicated.").String()

	selectorRelabelConf := regSelectorRelabelFlags(cmd)

	m[component.Compact.String()] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		return runCompact(g, logger, reg,
			*httpAddr,
			time.Duration(*httpGracePeriod),
			*dataDir,
			objStoreConfig,
			time.Duration(*consistencyDelay),
			*haltOnError,
			*acceptMalformedIndex,
			*wait,
			*generateMissingIndexCacheFiles,
			map[compact.ResolutionLevel]time.Duration{
				compact.ResolutionLevelRaw: time.Duration(*retentionRaw),
				compact.ResolutionLevel5m:  time.Duration(*retention5m),
				compact.ResolutionLevel1h:  time.Duration(*retention1h),
			},
			component.Compact,
			*disableDownsampling,
			*maxCompactionLevel,
			*blockSyncConcurrency,
			*compactionConcurrency,
			selectorRelabelConf,
			*enableDedup,
			*dedupReplicaLabel,
		)
	}
}

func runCompact(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	httpBindAddr string,
	httpGracePeriod time.Duration,
	dataDir string,
	objStoreConfig *extflag.PathOrContent,
	consistencyDelay time.Duration,
	haltOnError bool,
	acceptMalformedIndex bool,
	wait bool,
	generateMissingIndexCacheFiles bool,
	retentionByResolution map[compact.ResolutionLevel]time.Duration,
	component component.Component,
	disableDownsampling bool,
	maxCompactionLevel int,
	blockSyncConcurrency int,
	concurrency int,
	selectorRelabelConf *extflag.PathOrContent,
	enableDedup bool,
	dedupReplicaLabel string,
) error {
	halted := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_compactor_halted",
		Help: "Set to 1 if the compactor halted due to an unexpected error",
	})
	retried := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compactor_retries_total",
		Help: "Total number of retries after retriable compactor error",
	})
	iterations := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compactor_iterations_total",
		Help: "Total number of iterations that were executed successfully",
	})
	halted.Set(0)

	reg.MustRegister(halted)
	reg.MustRegister(retried)
	reg.MustRegister(iterations)

	downsampleMetrics := newDownsampleMetrics(reg)

	statusProber := prober.New(component, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg))
	// Initiate HTTP listener providing metrics endpoint and readiness/liveness probes.
	srv := httpserver.New(logger, reg, component, statusProber,
		httpserver.WithListen(httpBindAddr),
		httpserver.WithGracePeriod(httpGracePeriod),
	)

	g.Add(func() error {
		statusProber.Healthy()

		return srv.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		srv.Shutdown(err)
	})

	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}

	bkt, err := client.NewBucket(logger, confContentYaml, reg, component.String())
	if err != nil {
		return err
	}

	relabelContentYaml, err := selectorRelabelConf.Content()
	if err != nil {
		return errors.Wrap(err, "get content of relabel configuration")
	}

	relabelConfig, err := parseRelabelConfig(relabelContentYaml)
	if err != nil {
		return err
	}

	// Ensure we close up everything properly.
	defer func() {
		if err != nil {
			runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
		}
	}()

	sy, err := compact.NewSyncer(logger, reg, bkt, consistencyDelay,
		blockSyncConcurrency, acceptMalformedIndex, relabelConfig)
	if err != nil {
		return errors.Wrap(err, "create syncer")
	}

	levels, err := compactions.levels(maxCompactionLevel)
	if err != nil {
		return errors.Wrap(err, "get compaction levels")
	}

	if maxCompactionLevel < compactions.maxLevel() {
		level.Warn(logger).Log("msg", "Max compaction level is lower than should be", "current", maxCompactionLevel, "default", compactions.maxLevel())
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Instantiate the compactor with different time slices. Timestamps in TSDB
	// are in milliseconds.
	comp, err := tsdb.NewLeveledCompactor(ctx, reg, logger, levels, downsample.NewPool())
	if err != nil {
		cancel()
		return errors.Wrap(err, "create compactor")
	}

	var (
		dedupDir        = path.Join(dataDir, "dedup")
		compactDir      = path.Join(dataDir, "compact")
		downsamplingDir = path.Join(dataDir, "downsample")
		indexCacheDir   = path.Join(dataDir, "index_cache")
	)

	if err := os.RemoveAll(downsamplingDir); err != nil {
		cancel()
		return errors.Wrap(err, "clean working downsample directory")
	}

	compactor, err := compact.NewBucketCompactor(logger, sy, comp, compactDir, bkt, concurrency)
	if err != nil {
		cancel()
		return errors.Wrap(err, "create bucket compactor")
	}

	if retentionByResolution[compact.ResolutionLevelRaw].Seconds() != 0 {
		level.Info(logger).Log("msg", "retention policy of raw samples is enabled", "duration", retentionByResolution[compact.ResolutionLevelRaw])
	}
	if retentionByResolution[compact.ResolutionLevel5m].Seconds() != 0 {
		level.Info(logger).Log("msg", "retention policy of 5 min aggregated samples is enabled", "duration", retentionByResolution[compact.ResolutionLevel5m])
	}
	if retentionByResolution[compact.ResolutionLevel1h].Seconds() != 0 {
		level.Info(logger).Log("msg", "retention policy of 1 hour aggregated samples is enabled", "duration", retentionByResolution[compact.ResolutionLevel1h])
	}

	deduper := dedup.NewBucketDeduper(logger, reg, bkt, dedupDir, dedupReplicaLabel, consistencyDelay, blockSyncConcurrency)

	f := func() error {
		if isEnableDedup(enableDedup, dedupReplicaLabel) {
			if err := deduper.Dedup(ctx); err != nil {
				return errors.Wrap(err, "dedup failed")
			}
		}
		if err := compactor.Compact(ctx); err != nil {
			return errors.Wrap(err, "compaction failed")
		}
		level.Info(logger).Log("msg", "compaction iterations done")

		// TODO(bplotka): Remove "disableDownsampling" once https://github.com/thanos-io/thanos/issues/297 is fixed.
		if !disableDownsampling {
			// After all compactions are done, work down the downsampling backlog.
			// We run two passes of this to ensure that the 1h downsampling is generated
			// for 5m downsamplings created in the first run.
			level.Info(logger).Log("msg", "start first pass of downsampling")

			if err := downsampleBucket(ctx, logger, downsampleMetrics, bkt, downsamplingDir); err != nil {
				return errors.Wrap(err, "first pass of downsampling failed")
			}

			level.Info(logger).Log("msg", "start second pass of downsampling")

			if err := downsampleBucket(ctx, logger, downsampleMetrics, bkt, downsamplingDir); err != nil {
				return errors.Wrap(err, "second pass of downsampling failed")
			}
			level.Info(logger).Log("msg", "downsampling iterations done")
		} else {
			level.Warn(logger).Log("msg", "downsampling was explicitly disabled")
		}

		if err := compact.ApplyRetentionPolicyByResolution(ctx, logger, bkt, retentionByResolution); err != nil {
			return errors.Wrap(err, fmt.Sprintf("retention failed"))
		}
		return nil
	}

	g.Add(func() error {
		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		// Generate index file.
		if generateMissingIndexCacheFiles {
			if err := genMissingIndexCacheFiles(ctx, logger, reg, bkt, indexCacheDir); err != nil {
				return err
			}
		}

		if !wait {
			return f()
		}

		// --wait=true is specified.
		return runutil.Repeat(5*time.Minute, ctx.Done(), func() error {
			err := f()
			if err == nil {
				iterations.Inc()
				return nil
			}

			// The HaltError type signals that we hit a critical bug and should block
			// for investigation. You should alert on this being halted.
			if compact.IsHaltError(err) {
				if haltOnError {
					level.Error(logger).Log("msg", "critical error detected; halting", "err", err)
					halted.Set(1)
					select {}
				} else {
					return errors.Wrap(err, "critical error detected")
				}
			}

			// The RetryError signals that we hit an retriable error (transient error, no connection).
			// You should alert on this being triggered too frequently.
			if compact.IsRetryError(err) {
				level.Error(logger).Log("msg", "retriable error", "err", err)
				retried.Inc()
				// TODO(bplotka): use actual "retry()" here instead of waiting 5 minutes?
				return nil
			}

			return errors.Wrap(err, "error executing compaction")
		})
	}, func(error) {
		cancel()
	})

	level.Info(logger).Log("msg", "starting compact node")
	statusProber.Ready()
	return nil
}

const (
	metricIndexGenerateName = "thanos_compact_generated_index_total"
	metricIndexGenerateHelp = "Total number of generated indexes."
)

// genMissingIndexCacheFiles scans over all blocks, generates missing index cache files and uploads them to object storage.
func genMissingIndexCacheFiles(ctx context.Context, logger log.Logger, reg *prometheus.Registry, bkt objstore.Bucket, dir string) error {
	genIndex := prometheus.NewCounter(prometheus.CounterOpts{
		Name: metricIndexGenerateName,
		Help: metricIndexGenerateHelp,
	})
	reg.MustRegister(genIndex)

	if err := os.RemoveAll(dir); err != nil {
		return errors.Wrap(err, "clean index cache directory")
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		return errors.Wrap(err, "create dir")
	}

	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			level.Error(logger).Log("msg", "failed to remove index cache directory", "path", dir, "err", err)
		}
	}()

	level.Info(logger).Log("msg", "start index cache processing")

	var metas []*metadata.Meta

	if err := bkt.Iter(ctx, "", func(name string) error {
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}

		meta, err := block.DownloadMeta(ctx, logger, bkt, id)
		if err != nil {
			// Probably not finished block, skip it.
			if bkt.IsObjNotFoundErr(errors.Cause(err)) {
				level.Warn(logger).Log("msg", "meta file wasn't found", "block", id.String())
				return nil
			}
			return errors.Wrap(err, "download metadata")
		}

		// New version of compactor pushes index cache along with data block.
		// Skip uncompacted blocks.
		if meta.Compaction.Level == 1 {
			return nil
		}

		metas = append(metas, &meta)

		return nil
	}); err != nil {
		return errors.Wrap(err, "retrieve bucket block metas")
	}

	for _, meta := range metas {
		if err := generateIndexCacheFile(ctx, bkt, logger, dir, meta); err != nil {
			return err
		}
		genIndex.Inc()
	}

	level.Info(logger).Log("msg", "generating index cache files is done, you can remove startup argument `index.generate-missing-cache-file`")
	return nil
}

func generateIndexCacheFile(
	ctx context.Context,
	bkt objstore.Bucket,
	logger log.Logger,
	indexCacheDir string,
	meta *metadata.Meta,
) error {
	id := meta.ULID

	bdir := filepath.Join(indexCacheDir, id.String())
	if err := os.MkdirAll(bdir, 0777); err != nil {
		return errors.Wrap(err, "create block dir")
	}

	defer func() {
		if err := os.RemoveAll(bdir); err != nil {
			level.Error(logger).Log("msg", "failed to remove index cache directory", "path", bdir, "err", err)
		}
	}()

	cachePath := filepath.Join(bdir, block.IndexCacheFilename)
	cache := path.Join(meta.ULID.String(), block.IndexCacheFilename)

	ok, err := objstore.Exists(ctx, bkt, cache)
	if ok {
		return nil
	}
	if err != nil {
		return errors.Wrapf(err, "attempt to check if a cached index file exists")
	}

	level.Debug(logger).Log("msg", "make index cache", "block", id)

	// Try to download index file from obj store.
	indexPath := filepath.Join(bdir, block.IndexFilename)
	index := path.Join(id.String(), block.IndexFilename)

	if err := objstore.DownloadFile(ctx, logger, bkt, index, indexPath); err != nil {
		return errors.Wrap(err, "download index file")
	}

	if err := block.WriteIndexCache(logger, indexPath, cachePath); err != nil {
		return errors.Wrap(err, "write index cache")
	}

	if err := objstore.UploadFile(ctx, logger, bkt, cachePath, cache); err != nil {
		return errors.Wrap(err, "upload index cache")
	}
	return nil
}

func isEnableDedup(enableDedup bool, dedupReplicaLabel string) bool {
	return enableDedup && len(dedupReplicaLabel) > 0
}
