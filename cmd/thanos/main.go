// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

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
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/version"
	"github.com/thanos-io/thanos/pkg/tracing/client"
	"go.uber.org/automaxprocs/maxprocs"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	logFormatLogfmt = "logfmt"
	logFormatJson   = "json"
)

type setupFunc func(*run.Group, log.Logger, *prometheus.Registry, opentracing.Tracer, <-chan struct{}, bool) error

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
	logFormat := app.Flag("log.format", "Log format to use. Possible options: logfmt or json.").
		Default(logFormatLogfmt).Enum(logFormatLogfmt, logFormatJson)

	tracingConfig := regCommonTracingFlags(app)

	cmds := map[string]setupFunc{}
	registerSidecar(cmds, app)
	registerStore(cmds, app)
	registerQuery(cmds, app)
	registerRule(cmds, app)
	registerCompact(cmds, app)
	registerTools(cmds, app)
	registerReceive(cmds, app)
	registerQueryFrontend(cmds, app)

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments: %v", os.Args))
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
		if *logFormat == logFormatJson {
			logger = log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
		}
		logger = level.NewFilter(logger, lvl)

		if *debugName != "" {
			logger = log.With(logger, "name", *debugName)
		}

		logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	}

	loggerAdapter := func(template string, args ...interface{}) {
		level.Debug(logger).Log("msg", fmt.Sprintf(template, args))
	}

	// Running in container with limits but with empty/wrong value of GOMAXPROCS env var could lead to throttling by cpu
	// maxprocs will automate adjustment by using cgroups info about cpu limit if it set as value for runtime.GOMAXPROCS.
	undo, err := maxprocs.Set(maxprocs.Logger(loggerAdapter))
	defer undo()
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "failed to set GOMAXPROCS: %v", err))
	}

	metrics := prometheus.NewRegistry()
	metrics.MustRegister(
		version.NewCollector("thanos"),
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)

	// Some packages still use default Register. Replace to have those metrics.
	prometheus.DefaultRegisterer = metrics
	// Memberlist uses go-metrics.
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
		if err != nil {
			level.Error(logger).Log("msg", "getting tracing config failed", "err", err)
			os.Exit(1)
		}

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

	// Create a signal channel to dispatch reload events to sub-commands.
	reloadCh := make(chan struct{}, 1)

	if err := cmds[cmd](&g, logger, metrics, tracer, reloadCh, *logLevel == "debug"); err != nil {
		// Use %+v for github.com/pkg/errors error to print with stack.
		level.Error(logger).Log("err", fmt.Sprintf("%+v", errors.Wrapf(err, "preparing %s command failed", cmd)))
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

	// Listen for reload signals.
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			return reload(logger, cancel, reloadCh)
		}, func(error) {
			close(cancel)
		})
	}

	if err := g.Run(); err != nil {
		// Use %+v for github.com/pkg/errors error to print with stack.
		level.Error(logger).Log("err", fmt.Sprintf("%+v", errors.Wrapf(err, "%s command failed", cmd)))
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

func reload(logger log.Logger, cancel <-chan struct{}, r chan<- struct{}) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)
	for {
		select {
		case s := <-c:
			level.Info(logger).Log("msg", "caught signal. Reloading.", "signal", s)
			select {
			case r <- struct{}{}:
				level.Info(logger).Log("msg", "reload dispatched.")
			default:
			}
		case <-cancel:
			return errors.New("canceled")
		}
	}
}

func getFlagsMap(flags []*kingpin.FlagModel) map[string]string {
	flagsMap := map[string]string{}

	// Exclude kingpin default flags to expose only Thanos ones.
	boilerplateFlags := kingpin.New("", "").Version("")

	for _, f := range flags {
		if boilerplateFlags.GetFlag(f.Name) != nil {
			continue
		}
		flagsMap[f.Name] = f.Value.String()
	}

	return flagsMap
}
