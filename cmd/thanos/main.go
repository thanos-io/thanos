package main

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"context"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/version"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const (
	defaultClusterAddr = "0.0.0.0:10900"
	defaultGRPCAddr    = "0.0.0.0:10901"
	defaultHTTPAddr    = "0.0.0.0:10902"
)

type setupFunc func(*run.Group, log.Logger, *prometheus.Registry, opentracing.Tracer) error

func main() {
	app := kingpin.New(filepath.Base(os.Args[0]), "A block storage based long-term storage for Prometheus")

	app.Version(version.Print("thanos"))
	app.HelpFlag.Short('h')

	debugName := app.Flag("debug.name", "name to prefix to log lines").Hidden().String()

	logLevel := app.Flag("log.level", "log filtering level").
		Default("info").Enum("error", "warn", "info", "debug")

	gcloudTraceProject := app.Flag("gcloudtrace.project", "GCP project to send Google Cloud Trace tracings to. If empty, tracing will be disabled.").
		String()
	gcloudTraceSampleFactor := app.Flag("gcloudtrace.sample-factor", "How often we send traces (1/<sample-factor>).").
		Default("1").Uint64()

	cmds := map[string]setupFunc{}
	registerSidecar(cmds, app, "sidecar")
	registerStore(cmds, app, "store")
	registerQuery(cmds, app, "query")
	registerRule(cmds, app, "rule")
	registerExample(cmds, app, "example")

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
	)

	var g run.Group
	var tracer opentracing.Tracer

	// Setup optional tracing.
	{
		ctx := context.Background()

		var closeFn func() error
		tracer, closeFn = tracing.NewOptionalGCloudTracer(ctx, logger, *gcloudTraceProject, *gcloudTraceSampleFactor, *debugName)

		ctx, cancel := context.WithCancel(ctx)
		g.Add(func() error {
			<-ctx.Done()
			return ctx.Err()
		}, func(error) {
			closeFn()
			cancel()
		})
	}

	if err := cmds[cmd](&g, logger, metrics, tracer); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "%s command failed", cmd))
		os.Exit(1)
	}

	// Listen for termination signals.
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			return interrupt(cancel)
		}, func(error) {
			close(cancel)
		})
	}

	if err := g.Run(); err != nil {
		level.Error(logger).Log("msg", "running command failed", "err", err)
		os.Exit(1)
	}
}

func interrupt(cancel <-chan struct{}) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-c:
		return nil
	case <-cancel:
		return errors.New("canceled")
	}
}

func registerProfile(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
}

func registerMetrics(mux *http.ServeMux, g prometheus.Gatherer) {
	mux.Handle("/metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{}))
}
