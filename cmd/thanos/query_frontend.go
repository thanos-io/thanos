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
			CortexFrontendConfig: &cortexfrontend.Config{},
			QueryRangeConfig: queryfrontend.QueryRangeConfig{
				Limits:             &cortexvalidation.Limits{},
				ResultsCacheConfig: &queryrange.ResultsCacheConfig{},
			},
			LabelsConfig: queryfrontend.LabelsConfig{
				Limits:             &cortexvalidation.Limits{},
				ResultsCacheConfig: &queryrange.ResultsCacheConfig{},
			},
		},
	}

	cfg.http.registerFlag(cmd)

	// Query range tripperware flags.
	cmd.Flag("query-range.split-interval", "Split query range requests by an interval and execute in parallel, it should be greater than 0 when query-range.response-cache-config is configured.").
		Default("24h").DurationVar(&cfg.QueryRangeConfig.SplitQueriesByInterval)

	cmd.Flag("query-range.max-retries-per-request", "Maximum number of retries for a single query range request; beyond this, the downstream error is returned.").
		Default("5").IntVar(&cfg.QueryRangeConfig.MaxRetries)

	cmd.Flag("query-range.max-query-length", "Limit the query time range (end - start time) in the query-frontend, 0 disables it.").
		Default("0").DurationVar(&cfg.QueryRangeConfig.Limits.MaxQueryLength)

	cmd.Flag("query-range.max-query-parallelism", "Maximum number of query range requests will be scheduled in parallel by the Frontend.").
		Default("14").IntVar(&cfg.QueryRangeConfig.Limits.MaxQueryParallelism)

	cmd.Flag("query-range.response-cache-max-freshness", "Most recent allowed cacheable result for query range requests, to prevent caching very recent results that might still be in flux.").
		Default("1m").DurationVar(&cfg.QueryRangeConfig.Limits.MaxCacheFreshness)

	cmd.Flag("query-range.partial-response", "Enable partial response for query range requests if no partial_response param is specified. --no-query-range.partial-response for disabling.").
		Default("true").BoolVar(&cfg.QueryRangeConfig.PartialResponseStrategy)

	cfg.QueryRangeConfig.CachePathOrContent = *extflag.RegisterPathOrContent(cmd, "query-range.response-cache-config", "YAML file that contains response cache configuration.", false)

	// Labels tripperware flags.
	cmd.Flag("labels.split-interval", "Split labels requests by an interval and execute in parallel, it should be greater than 0 when labels.response-cache-config is configured.").
		Default("24h").DurationVar(&cfg.LabelsConfig.SplitQueriesByInterval)

	cmd.Flag("labels.max-retries-per-request", "Maximum number of retries for a single label/series API request; beyond this, the downstream error is returned.").
		Default("5").IntVar(&cfg.LabelsConfig.MaxRetries)

	cmd.Flag("labels.max-query-parallelism", "Maximum number of labels requests will be scheduled in parallel by the Frontend.").
		Default("14").IntVar(&cfg.LabelsConfig.Limits.MaxQueryParallelism)

	cmd.Flag("labels.response-cache-max-freshness", "Most recent allowed cacheable result for labels requests, to prevent caching very recent results that might still be in flux.").
		Default("1m").DurationVar(&cfg.LabelsConfig.Limits.MaxCacheFreshness)

	cmd.Flag("labels.partial-response", "Enable partial response for labels requests if no partial_response param is specified. --no-labels.partial-response for disabling.").
		Default("true").BoolVar(&cfg.LabelsConfig.PartialResponseStrategy)

	cmd.Flag("labels.default-time-range", "The default metadata time range duration for retrieving labels through Labels and Series API when the range parameters are not specified.").
		Default("24h").DurationVar(&cfg.DefaultTimeRange)

	cfg.LabelsConfig.CachePathOrContent = *extflag.RegisterPathOrContent(cmd, "labels.response-cache-config", "YAML file that contains response cache configuration.", false)

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
	queryRangeCacheConfContentYaml, err := cfg.QueryRangeConfig.CachePathOrContent.Content()
	if err != nil {
		return err
	}
	if len(queryRangeCacheConfContentYaml) > 0 {
		cacheConfig, err := queryfrontend.NewCacheConfig(logger, queryRangeCacheConfContentYaml)
		if err != nil {
			return errors.Wrap(err, "initializing the query range cache config")
		}
		cfg.QueryRangeConfig.ResultsCacheConfig = &queryrange.ResultsCacheConfig{
			Compression: cfg.CacheCompression,
			CacheConfig: *cacheConfig,
		}
	}

	labelsCacheConfContentYaml, err := cfg.LabelsConfig.CachePathOrContent.Content()
	if err != nil {
		return err
	}
	if len(labelsCacheConfContentYaml) > 0 {
		cacheConfig, err := queryfrontend.NewCacheConfig(logger, queryRangeCacheConfContentYaml)
		if err != nil {
			return errors.Wrap(err, "initializing the labels cache config")
		}
		cfg.LabelsConfig.ResultsCacheConfig = &queryrange.ResultsCacheConfig{
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
		return errors.Wrap(err, "setup tripperwares")
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
