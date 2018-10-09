package main

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/compact"
	"github.com/improbable-eng/thanos/pkg/compact/downsample"
	"github.com/improbable-eng/thanos/pkg/objstore/client"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"gopkg.in/alecthomas/kingpin.v2"
)

func registerCompact(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "continuously compacts blocks in an object store bucket")

	haltOnError := cmd.Flag("debug.halt-on-error", "Halt the process if a critical compaction error is detected.").
		Hidden().Default("true").Bool()

	httpAddr := regHTTPAddrFlag(cmd)

	dataDir := cmd.Flag("data-dir", "Data directory in which to cache blocks and process compactions.").
		Default("./data").String()

	objStoreConfig := regCommonObjStoreFlags(cmd, "")

	syncDelay := modelDuration(cmd.Flag("sync-delay", "Minimum age of fresh (non-compacted) blocks before they are being processed.").
		Default("30m"))

	retentionRaw := modelDuration(cmd.Flag("retention.resolution-raw", "How long to retain raw samples in bucket. 0d - disables this retention").Default("0d"))
	retention5m := modelDuration(cmd.Flag("retention.resolution-5m", "How long to retain samples of resolution 1 (5 minutes) in bucket. 0d - disables this retention").Default("0d"))
	retention1h := modelDuration(cmd.Flag("retention.resolution-1h", "How long to retain samples of resolution 2 (1 hour) in bucket. 0d - disables this retention").Default("0d"))

	wait := cmd.Flag("wait", "Do not exit after all compactions have been processed and wait for new work.").
		Short('w').Bool()

	// TODO(bplotka): Remove this flag once https://github.com/improbable-eng/thanos/issues/297 is fixed.
	disableDownsampling := cmd.Flag("debug.disable-downsampling", "Disables downsampling. This is not recommended "+
		"as querying long time ranges without non-downsampled data is not efficient and not useful (is not possible to render all for human eye).").
		Hidden().Default("false").Bool()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		return runCompact(g, logger, reg,
			*httpAddr,
			*dataDir,
			objStoreConfig,
			time.Duration(*syncDelay),
			*haltOnError,
			*wait,
			map[compact.ResolutionLevel]time.Duration{
				compact.ResolutionLevelRaw: time.Duration(*retentionRaw),
				compact.ResolutionLevel5m:  time.Duration(*retention5m),
				compact.ResolutionLevel1h:  time.Duration(*retention1h),
			},
			name,
			*disableDownsampling,
		)
	}
}

func runCompact(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	httpBindAddr string,
	dataDir string,
	objStoreConfig *pathOrContent,
	syncDelay time.Duration,
	haltOnError bool,
	wait bool,
	retentionByResolution map[compact.ResolutionLevel]time.Duration,
	component string,
	disableDownsampling bool,
) error {
	halted := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_compactor_halted",
		Help: "Set to 1 if the compactor halted due to an unexpected error",
	})
	retried := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compactor_retries_total",
		Help: "Total number of retries after retriable compactor error",
	})
	halted.Set(0)

	reg.MustRegister(halted)

	bucketConfig, err := objStoreConfig.Content()
	if err != nil {
		return err
	}

	bkt, err := client.NewBucket(logger, bucketConfig, reg, component)
	if err != nil {
		return err
	}

	// Ensure we close up everything properly.
	defer func() {
		if err != nil {
			runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
		}
	}()

	sy, err := compact.NewSyncer(logger, reg, bkt, syncDelay)
	if err != nil {
		return errors.Wrap(err, "create syncer")
	}

	// Instantiate the compactor with different time slices. Timestamps in TSDB
	// are in milliseconds.
	comp, err := tsdb.NewLeveledCompactor(reg, logger, []int64{
		int64(1 * time.Hour / time.Millisecond),
		int64(2 * time.Hour / time.Millisecond),
		int64(8 * time.Hour / time.Millisecond),
		int64(2 * 24 * time.Hour / time.Millisecond),  // 2 days
		int64(14 * 24 * time.Hour / time.Millisecond), // 2 weeks
	}, downsample.NewPool())
	if err != nil {
		return errors.Wrap(err, "create compactor")
	}

	var (
		compactDir      = path.Join(dataDir, "compact")
		downsamplingDir = path.Join(dataDir, "downsample")
	)

	compactor := compact.NewBucketCompactor(logger, sy, comp, compactDir, bkt)

	if retentionByResolution[compact.ResolutionLevelRaw].Seconds() != 0 {
		level.Info(logger).Log("msg", "retention policy of raw samples is enabled", "duration", retentionByResolution[compact.ResolutionLevelRaw])
	}
	if retentionByResolution[compact.ResolutionLevel5m].Seconds() != 0 {
		level.Info(logger).Log("msg", "retention policy of 5 min aggregated samples is enabled", "duration", retentionByResolution[compact.ResolutionLevel5m])
	}
	if retentionByResolution[compact.ResolutionLevel1h].Seconds() != 0 {
		level.Info(logger).Log("msg", "retention policy of 1 hour aggregated samples is enabled", "duration", retentionByResolution[compact.ResolutionLevel1h])
	}

	ctx, cancel := context.WithCancel(context.Background())
	f := func() error {
		if err := compactor.Compact(ctx); err != nil {
			return errors.Wrap(err, "compaction failed")
		}
		level.Info(logger).Log("msg", "compaction iterations done")

		// TODO(bplotka): Remove "disableDownsampling" once https://github.com/improbable-eng/thanos/issues/297 is fixed.
		if !disableDownsampling {
			// After all compactions are done, work down the downsampling backlog.
			// We run two passes of this to ensure that the 1h downsampling is generated
			// for 5m downsamplings created in the first run.
			level.Info(logger).Log("msg", "start first pass of downsampling")

			if err := downsampleBucket(ctx, logger, bkt, downsamplingDir); err != nil {
				return errors.Wrap(err, "first pass of downsampling failed")
			}

			level.Info(logger).Log("msg", "start second pass of downsampling")

			if err := downsampleBucket(ctx, logger, bkt, downsamplingDir); err != nil {
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

		if !wait {
			return f()
		}

		// --wait=true is specified.
		return runutil.Repeat(5*time.Minute, ctx.Done(), func() error {
			err := f()
			if err == nil {
				return nil
			}
			// The HaltError type signals that we hit a critical bug and should block
			// for investigation.
			// You should alert on this being halted.
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
			// You should alert on this being triggered to frequently.
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

	if err := metricHTTPListenGroup(g, logger, reg, httpBindAddr); err != nil {
		return err
	}

	level.Info(logger).Log("msg", "starting compact node")
	return nil
}
