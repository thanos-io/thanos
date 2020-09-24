// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"
	cortexfrontend "github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	cortexvalidation "github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/user"

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

type queryFrontendConfig struct {
	http httpConfig
	queryfrontend.Config
}

func registerQueryFrontend(app *extkingpin.App) {
	comp := component.QueryFrontend
	cmd := app.Command(comp.String(), "query frontend")
	cfg := &queryFrontendConfig{
		Config: queryfrontend.Config{
			CortexFrontendConfig:     &cortexfrontend.Config{},
			CortexLimits:             &cortexvalidation.Limits{},
			CortexResultsCacheConfig: &queryrange.ResultsCacheConfig{},
		},
	}

	cfg.http.registerFlag(cmd)

	cmd.Flag("query-range.split-interval", "Split queries by an interval and execute in parallel, it should be greater than 0 when response-cache-config is configured.").
		Default("24h").DurationVar(&cfg.SplitQueriesByInterval)

	cmd.Flag("query-range.max-retries-per-request", "Maximum number of retries for a single request; beyond this, the downstream error is returned.").
		Default("5").IntVar(&cfg.MaxRetries)

	cmd.Flag("query-range.max-query-length", "Limit the query time range (end - start time) in the query-frontend, 0 disables it.").
		Default("0").DurationVar(&cfg.CortexLimits.MaxQueryLength)

	cmd.Flag("query-range.max-query-parallelism", "Maximum number of queries will be scheduled in parallel by the Frontend.").
		Default("14").IntVar(&cfg.CortexLimits.MaxQueryParallelism)

	cmd.Flag("query-range.response-cache-max-freshness", "Most recent allowed cacheable result, to prevent caching very recent results that might still be in flux.").
		Default("1m").DurationVar(&cfg.CortexLimits.MaxCacheFreshness)

	cmd.Flag("query-range.partial-response", "Enable partial response for queries if no partial_response param is specified. --no-query-range.partial-response for disabling.").
		Default("true").BoolVar(&cfg.PartialResponseStrategy)

	cfg.CachePathOrContent = *extflag.RegisterPathOrContent(cmd, "query-range.response-cache-config", "YAML file that contains response cache configuration.", false)

	cmd.Flag("cache-compression-type", "Use compression in results cache. Supported values are: 'snappy' and '' (disable compression).").
		Default("").StringVar(&cfg.CacheCompression)

	cmd.Flag("query-frontend.downstream-url", "URL of downstream Prometheus Query compatible API.").
		Default("http://localhost:9090").StringVar(&cfg.CortexFrontendConfig.DownstreamURL)

	cmd.Flag("query-frontend.compress-responses", "Compress HTTP responses.").
		Default("false").BoolVar(&cfg.CortexFrontendConfig.CompressResponses)

	cmd.Flag("query-frontend.log-queries-longer-than", "Log queries that are slower than the specified duration. "+
		"Set to 0 to disable. Set to < 0 to enable on all queries.").Default("0").DurationVar(&cfg.CortexFrontendConfig.LogQueriesLongerThan)

	cmd.Flag("log.request.decision", "Request Logging for logging the start and end of requests. LogFinishCall is enabled by default. LogFinishCall : Logs the finish call of the requests. LogStartAndFinishCall : Logs the start and finish call of the requests. NoLogCall : Disable request logging.").Default("LogFinishCall").EnumVar(&cfg.RequestLoggingDecision, "NoLogCall", "LogFinishCall", "LogStartAndFinishCall")

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		return runQueryFrontend(g, logger, reg, tracer, cfg, comp)
	})
}

func runQueryFrontend(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	cfg *queryFrontendConfig,
	comp component.Component,
) error {
	cacheConfContentYaml, err := cfg.CachePathOrContent.Content()
	if err != nil {
		return err
	}
	if len(cacheConfContentYaml) > 0 {
		cacheConfig, err := queryfrontend.NewCacheConfig(logger, cacheConfContentYaml)
		if err != nil {
			return errors.Wrap(err, "initializing the query frontend config")
		}
		if cfg.CortexResultsCacheConfig.CacheConfig.Memcache.Expiration == 0 {
			level.Warn(logger).Log("msg", "memcached cache valid time set to 0, so using a default of 24 hours expiration time")
			cfg.CortexResultsCacheConfig.CacheConfig.Memcache.Expiration = 24 * time.Hour
		}
		cfg.CortexResultsCacheConfig = &queryrange.ResultsCacheConfig{
			Compression: cfg.CacheCompression,
			CacheConfig: *cacheConfig,
		}
	}

	if err := cfg.Validate(); err != nil {
		return errors.Wrap(err, "error validating the config")
	}

	fe, err := cortexfrontend.New(*cfg.CortexFrontendConfig, nil, logger, reg)
	if err != nil {
		return errors.Wrap(err, "setup query frontend")
	}
	defer fe.Close()

	tripperWare, err := queryfrontend.NewTripperware(cfg.Config, reg, logger)
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
		return logging.LogDecision[cfg.RequestLoggingDecision]
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
