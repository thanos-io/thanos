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
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
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

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks.").
		PlaceHolder("<bucket>").String()

	s3config := s3.RegisterS3Params(cmd)

	syncDelay := modelDuration(cmd.Flag("sync-delay", "Minimum age of fresh (non-compacted) blocks before they are being processed.").
		Default("30m"))

	retention := modelDuration(cmd.Flag("retention.default", "How long to retain samples in bucket. 0d - disables retention").Default("0d"))

	retentionRes0 := modelDuration(cmd.Flag("retention.res0", "How long to retain raw samples in bucket. 0d - disables this retention").Default("0d"))
	retentionRes1 := modelDuration(cmd.Flag("retention.res1", "How long to retain samples of resolution 1 (5 minutes) in bucket. 0d - disables this retention").Default("0d"))
	retentionRes2 := modelDuration(cmd.Flag("retention.res2", "How long to retain samples of resolution 2 (1 hour) in bucket. 0d - disables this retention").Default("0d"))

	wait := cmd.Flag("wait", "Do not exit after all compactions have been processed and wait for new work.").
		Short('w').Bool()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		return runCompact(g, logger, reg,
			*httpAddr,
			*dataDir,
			*gcsBucket,
			s3config,
			time.Duration(*syncDelay),
			*haltOnError,
			*wait,
			time.Duration(*retention),
			map[int64]time.Duration{
				downsample.ResLevel0: time.Duration(*retentionRes0),
				downsample.ResLevel1: time.Duration(*retentionRes1),
				downsample.ResLevel2: time.Duration(*retentionRes2),
			},
			name,
		)
	}
}

func runCompact(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	httpBindAddr string,
	dataDir string,
	gcsBucket string,
	s3Config *s3.Config,
	syncDelay time.Duration,
	haltOnError bool,
	wait bool,
	retention time.Duration,
	retentionByResolution map[int64]time.Duration,
	component string,
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

	bkt, err := client.NewBucket(logger, &gcsBucket, *s3Config, reg, component)
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

	if retention.Seconds() != 0 {
		level.Info(logger).Log("msg", "default retention policy is enabled", "duration", retention)
	}

	if retentionByResolution[downsample.ResLevel0].Seconds() != 0 {
		level.Info(logger).Log("msg", "retention policy of raw samples is enabled", "duration", retentionByResolution[downsample.ResLevel0])
	}
	if retentionByResolution[downsample.ResLevel1].Seconds() != 0 {
		level.Info(logger).Log("msg", "retention policy of 5 min aggregated samples is enabled", "duration", retentionByResolution[downsample.ResLevel0])
	}
	if retentionByResolution[downsample.ResLevel2].Seconds() != 0 {
		level.Info(logger).Log("msg", "retention policy of 1 hour aggregated samples is enabled", "duration", retentionByResolution[downsample.ResLevel0])
	}

	ctx, cancel := context.WithCancel(context.Background())
	f := func() error {
		if err := compactor.Compact(ctx); err != nil {
			return errors.Wrap(err, "compaction failed")
		}

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

		if retention.Seconds() != 0 {
			if err := compact.ApplyDefaultRetentionPolicy(ctx, logger, bkt, retention); err != nil {
				return errors.Wrap(err, "retention failed")
			}
		}

		for resolution, retentionDuration := range retentionByResolution {
			if retentionDuration != 0 {
				if err := compact.ApplyRetentionPolicyByResolution(ctx, logger, bkt, retentionDuration, resolution); err != nil {
					return errors.Wrap(err, fmt.Sprintf("retention for resolution %d failed", resolution))
				}
			}
		}

		level.Info(logger).Log("msg", "compaction, downsampling and optional retention apply iteration done")
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
