// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/queryfrontend"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/server/http/middleware"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type config struct {
	http httpConfig
	queryfrontend.Config

	cachePathOrContent     extflag.PathOrContent
	requestLoggingDecision string
	// partialResponseStrategy is the default strategy used
	// when parsing thanos query request.
	partialResponseStrategy bool
}

func registerQueryFrontend(app *extkingpin.App) {
	comp := component.QueryFrontend
	cmd := app.Command(comp.String(), "query frontend")
	cfg := &config{}

	cfg.http.registerFlag(cmd)

	cmd.Flag("query-range.split-queries-by-interval", "Split queries by an interval and execute in parallel, 0 disables it.").
		Default("24h").DurationVar(&cfg.QueryRange.SplitQueriesByInterval)

	cmd.Flag("query-range.max-retries-per-request", "Maximum number of retries for a single request; beyond this, the downstream error is returned.").
		Default("5").IntVar(&cfg.QueryRange.MaxRetries)

	cmd.Flag("query-range.max-query-length", "Limit the query time range (end - start time) in the query-frontend, 0 disables it.").
		Default("0").DurationVar(&cfg.Limits.MaxQueryLength)

	cmd.Flag("query-range.max-query-parallelism", "Maximum number of queries will be scheduled in parallel by the Frontend.").
		Default("14").IntVar(&cfg.Limits.MaxQueryParallelism)

	cmd.Flag("query-range.max-cache-freshness", "Most recent allowed cacheable result, to prevent caching very recent results that might still be in flux.").
		Default("1m").DurationVar(&cfg.Limits.MaxCacheFreshness)

	cmd.Flag("query-range.partial-response", "Enable partial response for queries if no partial_response param is specified. --no-query-range.partial-response for disabling.").
		Default("true").BoolVar(&cfg.partialResponseStrategy)

	cfg.cachePathOrContent = *extflag.RegisterPathOrContent(cmd, "query-range.cache-config", "YAML file that contains response cache configuration.", false)

	cmd.Flag("query-frontend.downstream-url", "URL of downstream Prometheus Query compatible API.").
		Default("http://localhost:9090").StringVar(&cfg.Frontend.DownstreamURL)

	cmd.Flag("query-frontend.compress-http-responses", "Compress HTTP responses.").
		Default("false").BoolVar(&cfg.Frontend.CompressResponses)

	cmd.Flag("query-frontend.log-queries-longer-than", "Log queries that are slower than the specified duration. "+
		"Set to 0 to disable. Set to < 0 to enable on all queries.").Default("0").DurationVar(&cfg.Frontend.LogQueriesLongerThan)

	cmd.Flag("log.request.decision", "Request Logging for logging the start and end of requests. LogFinishCall is enabled by default. LogFinishCall : Logs the finish call of the requests. LogStartAndFinishCall : Logs the start and finish call of the requests. NoLogCall : Disable request logging.").Default("LogFinishCall").EnumVar(&cfg.requestLoggingDecision, "NoLogCall", "LogFinishCall", "LogStartAndFinishCall")

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		return runQueryFrontend(g, logger, reg, tracer, cfg, comp)
	})
}

func runQueryFrontend(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	cfg *config,
	comp component.Component,
) error {
	cacheConfContentYaml, err := cfg.cachePathOrContent.Content()
	if err != nil {
		return err
	}
	if len(cacheConfContentYaml) > 0 {
		if err := yaml.UnmarshalStrict(cacheConfContentYaml, &cfg.QueryRange.ResultsCacheConfig.CacheConfig); err != nil {
			return errors.Wrap(err, "parsing cache config YAML file")
		}
	}

	err = cfg.Validate()
	if err != nil {
		return errors.Wrap(err, "error validating the config")
	}

	fe, err := frontend.New(frontend.Config{
		DownstreamURL:        cfg.Frontend.DownstreamURL,
		CompressResponses:    cfg.Frontend.CompressResponses,
		LogQueriesLongerThan: cfg.Frontend.LogQueriesLongerThan,
	}, logger, reg)
	if err != nil {
		return errors.Wrap(err, "setup query frontend")
	}
	defer fe.Close()

	limits, err := validation.NewOverrides(validation.Limits{
		MaxQueryLength:      cfg.Limits.MaxQueryLength,
		MaxQueryParallelism: cfg.Limits.MaxQueryParallelism,
		MaxCacheFreshness:   cfg.Limits.MaxCacheFreshness,
	}, nil)
	if err != nil {
		return errors.Wrap(err, "initialiase limits")
	}

	codec := queryfrontend.NewThanosCodec(cfg.partialResponseStrategy)
	tripperWare, err := queryfrontend.NewTripperWare(
		cfg.QueryRange,
		limits,
		codec,
		queryrange.PrometheusResponseExtractor{},
		reg,
		logger,
	)
	if err != nil {
		return errors.Wrap(err, "setup query range middlewares")
	}

	fe.Wrap(tripperWare)

	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	// Configure Request Logging for HTTP calls.
	opts := []logging.Option{logging.WithDecider(func() logging.Decision {
		return logging.LogDecision[cfg.requestLoggingDecision]
	})}
	logMiddleware := logging.NewHTTPServerMiddleware(logger, opts...)
	ins := extpromhttp.NewInstrumentationMiddleware(reg)

	// Start metrics HTTP server.
	{
		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(cfg.http.bindAddress),
			httpserver.WithGracePeriod(time.Duration(cfg.http.gracePeriod)),
		)

		instr := func(f http.HandlerFunc) http.HandlerFunc {
			hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				name := "query-frontend"
				ins.NewHandler(
					name,
					logMiddleware.HTTPMiddleware(
						name,
						tracing.HTTPMiddleware(
							tracer,
							name,
							logger,
							gziphandler.GzipHandler(middleware.RequestID(f)),
						),
					),
					// Cortex frontend middlewares require orgID.
				).ServeHTTP(w, r.WithContext(user.InjectOrgID(r.Context(), "fake")))
			})
			return hf
		}
		srv.Handle("/", instr(fe.Handler().ServeHTTP))

		g.Add(func() error {
			statusProber.Healthy()

			return srv.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			defer statusProber.NotHealthy(err)

			srv.Shutdown(err)
		})
	}

	level.Info(logger).Log("msg", "starting query frontend")
	statusProber.Ready()
	return nil
}
