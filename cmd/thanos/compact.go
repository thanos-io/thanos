package main

import (
	"context"
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
	"github.com/oklog/ulid"
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

	syncDelay := cmd.Flag("sync-delay", "Minimum age of fresh (non-compacted) blocks before they are being processed.").
		Default("30m").Duration()

	wait := cmd.Flag("wait", "Do not exit after all compactions have been processed and wait for new work.").
		Short('w').Bool()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		return runCompact(g, logger, reg,
			*httpAddr,
			*dataDir,
			*gcsBucket,
			s3config,
			*syncDelay,
			*haltOnError,
			*wait,
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
		return err
	}
	// Start cycle of syncing blocks from the bucket and garbage collecting the bucket.
	{
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

		ctx, cancel := context.WithCancel(context.Background())

		f := func() error {
			var (
				compactDir      = path.Join(dataDir, "compact")
				downsamplingDir = path.Join(dataDir, "downsample")
			)

			// Loop over bucket and compact until there's no work left.
			for {
				level.Info(logger).Log("msg", "start sync of metas")

				if err := sy.SyncMetas(ctx); err != nil {
					return errors.Wrap(err, "sync")
				}

				level.Info(logger).Log("msg", "start of GC")

				if err := sy.GarbageCollect(ctx); err != nil {
					return errors.Wrap(err, "garbage")
				}

				groups, err := sy.Groups()
				if err != nil {
					return errors.Wrap(err, "build compaction groups")
				}
				done := true
				for _, g := range groups {
					id, err := g.Compact(ctx, compactDir, comp)
					if err == nil {
						// If the returned ID has a zero value, the group had no blocks to be compacted.
						// We keep going through the outer loop until no group has any work left.
						if id != (ulid.ULID{}) {
							done = false
						}
						continue
					}

					if compact.IsIssue347Error(err) {
						err = compact.RepairIssue347(ctx, logger, bkt, err)
						if err == nil {
							done = false
							continue
						}
					}
					return errors.Wrap(err, "compaction")
				}
				if done {
					break
				}
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

			level.Info(logger).Log("msg", "compaction iteration done")
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
				if err != nil {
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
				}

				return err
			})
		}, func(error) {
			cancel()
		})
	}
	if err := metricHTTPListenGroup(g, logger, reg, httpBindAddr); err != nil {
		return err
	}

	level.Info(logger).Log("msg", "starting compact node")
	return nil
}
