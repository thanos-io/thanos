package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	gmetrics "github.com/armon/go-metrics"
	gprom "github.com/armon/go-metrics/prometheus"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/internal/register"
	"github.com/improbable-eng/thanos/pkg/tracing/client"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	logFormatLogfmt = "logfmt"
	logFormatJSON   = "json"
)

func main() {
	if os.Getenv("DEBUG") != "" {
		runtime.SetMutexProfileFraction(10)
		runtime.SetBlockProfileRate(10)
	}

	app := kingpin.New(filepath.Base(os.Args[0]), "A block storage based long-term storage for Prometheus")

	app.Version(version.Print("thanos"))
	app.HelpFlag.Short('h')

	debugName := app.Flag("debug.name", "Name to add as prefix to log lines.").Hidden().String()

	logLevel := app.Flag("log.level", "Log filtering level.").
		Default("info").Enum("error", "warn", "info", "debug")
	logFormat := app.Flag("log.format", "Log format to use.").
		Default(logFormatLogfmt).Enum(logFormatLogfmt, logFormatJSON)

	tracingConfig := register.CommonTracingFlags(app)

	cmds := map[string]register.SetupFunc{}
	register.Sidecar(cmds, app, "sidecar")
	register.Store(cmds, app, "store")
	register.Query(cmds, app, "query")
	register.Rule(cmds, app, "rule")
	register.Compact(cmds, app, "compact")
	register.Bucket(cmds, app, "bucket")
	register.Downsample(cmds, app, "downsample")
	register.Receive(cmds, app, "receive")

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		app.Usage(os.Args[1:])
		os.Exit(2)
	}

	var logger log.Logger
	{
		var lvl level.Option
		switch *logLevel {
		case "error":
			lvl = level.AllowError()
		case "warn":
			lvl = level.AllowWarn()
		case "info":
			lvl = level.AllowInfo()
		case "debug":
			lvl = level.AllowDebug()
		default:
			panic("unexpected log level")
		}
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
		if *logFormat == logFormatJSON {
			logger = log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
		}
		logger = level.NewFilter(logger, lvl)

		if *debugName != "" {
			logger = log.With(logger, "name", *debugName)
		}

		logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	}

	metrics := prometheus.NewRegistry()
	metrics.MustRegister(
		version.NewCollector("thanos"),
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)

	prometheus.DefaultRegisterer = metrics
	// Memberlist uses go-metrics
	sink, err := gprom.NewPrometheusSink()
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "%s command failed", cmd))
		os.Exit(1)
	}
	gmetricsConfig := gmetrics.DefaultConfig("thanos_" + cmd)
	gmetricsConfig.EnableRuntimeMetrics = false
	if _, err = gmetrics.NewGlobal(gmetricsConfig, sink); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "%s command failed", cmd))
		os.Exit(1)
	}

	var g run.Group
	var tracer opentracing.Tracer

	// Setup optional tracing.
	{
		ctx := context.Background()

		var closer io.Closer
		var confContentYaml []byte
		confContentYaml, err = tracingConfig.Content()

		if len(confContentYaml) == 0 {
			level.Info(logger).Log("msg", "Tracing will be disabled")
			tracer = client.NoopTracer()
		} else {
			tracer, closer, err = client.NewTracer(ctx, logger, metrics, confContentYaml)
			if err != nil {
				fmt.Fprintln(os.Stderr, errors.Wrapf(err, "tracing failed"))
				os.Exit(1)
			}
		}

		// This is bad, but Prometheus does not support any other tracer injections than just global one.
		// TODO(bplotka): Work with basictracer to handle gracefully tracker mismatches, and also with Prometheus to allow
		// tracer injection.
		opentracing.SetGlobalTracer(tracer)

		ctx, cancel := context.WithCancel(ctx)
		g.Add(func() error {
			<-ctx.Done()
			return ctx.Err()
		}, func(error) {
			if closer != nil {
				if err := closer.Close(); err != nil {
					level.Warn(logger).Log("msg", "closing tracer failed", "err", err)
				}
			}
			cancel()
		})
	}

	if err := cmds[cmd](&g, logger, metrics, tracer, *logLevel == "debug"); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "%s command failed", cmd))
		os.Exit(1)
	}

	// Listen for termination signals.
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			return interrupt(logger, cancel)
		}, func(error) {
			close(cancel)
		})
	}

	if err := g.Run(); err != nil {
		level.Error(logger).Log("msg", "running command failed", "err", err)
		os.Exit(1)
	}
	level.Info(logger).Log("msg", "exiting")
}

func interrupt(logger log.Logger, cancel <-chan struct{}) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-c:
		level.Info(logger).Log("msg", "caught signal. Exiting.", "signal", s)
		return nil
	case <-cancel:
		return errors.New("canceled")
	}
}
