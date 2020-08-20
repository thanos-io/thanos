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
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/user"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/queryfrontend"
	"github.com/thanos-io/thanos/pkg/queryfrontend/cache"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/server/http/middleware"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type queryFrontendConfig struct {
	http             httpConfig
	queryRangeConfig queryRangeConfig

	downstreamURL        string
	compressResponses    bool
	LogQueriesLongerThan time.Duration

	requestLoggingDecision string
}

type queryRangeConfig struct {
	respCacheConfig     extflag.PathOrContent
	cacheMaxFreshness   time.Duration
	splitInterval       model.Duration
	maxRetries          int
	maxQueryParallelism int
	maxQueryLength      model.Duration

	// partialResponseStrategy is the default strategy used
	// when parsing thanos query request.
	partialResponseStrategy bool
}

func (c *queryRangeConfig) registerFlag(cmd *kingpin.CmdClause) {
	cmd.Flag("query-range.split-interval", "Split queries by an interval and execute in parallel, 0 disables it.").
		Default("24h").SetValue(&c.splitInterval)

	cmd.Flag("query-range.max-retries-per-request", "Maximum number of retries for a single request; beyond this, the downstream error is returned.").
		Default("5").IntVar(&c.maxRetries)

	cmd.Flag("query-range.max-query-length", "Limit the query time range (end - start time) in the query-frontend, 0 disables it.").
		Default("0").SetValue(&c.maxQueryLength)

	cmd.Flag("query-range.max-query-parallelism", "Maximum number of queries will be scheduled in parallel by the frontend.").
		Default("14").IntVar(&c.maxQueryParallelism)

	cmd.Flag("query-range.response-cache-max-freshness", "Most recent allowed cacheable result, to prevent caching very recent results that might still be in flux.").
		Default("1m").DurationVar(&c.cacheMaxFreshness)

	cmd.Flag("query-range.partial-response", "Enable partial response for queries if no partial_response param is specified. --no-query-range.partial-response for disabling.").
		Default("true").BoolVar(&c.partialResponseStrategy)

	c.respCacheConfig = *extflag.RegisterPathOrContent(cmd, "query-range.response-cache-config", "YAML file that contains response cache configuration.", false)
}

func (c *queryFrontendConfig) registerFlag(cmd *kingpin.CmdClause) {
	c.queryRangeConfig.registerFlag(cmd)
	c.http.registerFlag(cmd)

	cmd.Flag("query-frontend.downstream-url", "URL of downstream Prometheus Query compatible API.").
		Default("http://localhost:9090").StringVar(&c.downstreamURL)

	cmd.Flag("query-frontend.compress-responses", "Compress HTTP responses.").
		Default("false").BoolVar(&c.compressResponses)

	cmd.Flag("query-frontend.log_queries_longer_than", "Log queries that are slower than the specified duration. "+
		"Set to 0 to disable. Set to < 0 to enable on all queries.").Default("0").DurationVar(&c.LogQueriesLongerThan)

	cmd.Flag("log.request.decision", "Request Logging for logging the start and end of requests. LogFinishCall is enabled by default. LogFinishCall : Logs the finish call of the requests. LogStartAndFinishCall : Logs the start and finish call of the requests. NoLogCall : Disable request logging.").Default("LogFinishCall").EnumVar(&c.requestLoggingDecision, "NoLogCall", "LogFinishCall", "LogStartAndFinishCall")
}

func registerQueryFrontend(m map[string]setupFunc, app *kingpin.Application) {
	comp := component.QueryFrontend
	cmd := app.Command(comp.String(), "query frontend")
	conf := &queryFrontendConfig{}
	conf.registerFlag(cmd)

	m[comp.String()] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		return runQueryFrontend(g, logger, reg, tracer, conf, comp)
	}
}

func runQueryFrontend(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	conf *queryFrontendConfig,
	comp component.Component,
) error {

	if len(conf.downstreamURL) == 0 {
		return errors.New("downstream URL should be configured")
	}

	fe, err := frontend.New(frontend.Config{
		DownstreamURL:        conf.downstreamURL,
		CompressResponses:    conf.compressResponses,
		LogQueriesLongerThan: conf.LogQueriesLongerThan,
	}, logger, reg)
	if err != nil {
		return errors.Wrap(err, "setup query frontend")
	}
	defer fe.Close()

	limits := queryfrontend.NewLimits(
		conf.queryRangeConfig.maxQueryParallelism,
		time.Duration(conf.queryRangeConfig.maxQueryLength),
		conf.queryRangeConfig.cacheMaxFreshness,
	)

	respCacheContentYaml, err := conf.queryRangeConfig.respCacheConfig.Content()
	if err != nil {
		return errors.Wrap(err, "get content of response cache configuration")
	}

	var cacheConfig *queryrange.ResultsCacheConfig
	if len(respCacheContentYaml) > 0 {
		cacheConfig, err = cache.NewResponseCacheConfig(respCacheContentYaml)
		if err != nil {
			return errors.Wrap(err, "create response cache")
		}
	}

	codec := queryfrontend.NewThanosCodec(conf.queryRangeConfig.partialResponseStrategy)
	tripperWare, err := queryfrontend.NewTripperWare(
		limits,
		cacheConfig,
		codec,
		queryrange.PrometheusResponseExtractor{},
		time.Duration(conf.queryRangeConfig.splitInterval),
		conf.queryRangeConfig.maxRetries,
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
		return logging.LogDecision[conf.requestLoggingDecision]
	})}
	logMiddleware := logging.NewHTTPServerMiddleware(logger, opts...)
	ins := extpromhttp.NewInstrumentationMiddleware(reg)

	// Start metrics HTTP server.
	{
		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(conf.http.bindAddress),
			httpserver.WithGracePeriod(time.Duration(conf.http.gracePeriod)),
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
