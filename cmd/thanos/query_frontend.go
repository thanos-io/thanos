// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"net"
	"net/http"
	"time"

	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/klauspost/compress/gzhttp"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v2"

	cortexfrontend "github.com/thanos-io/thanos/internal/cortex/frontend"
	"github.com/thanos-io/thanos/internal/cortex/frontend/transport"
	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	cortexvalidation "github.com/thanos-io/thanos/internal/cortex/util/validation"
	"github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/component"
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
	http           httpConfig
	webDisableCORS bool
	queryfrontend.Config
	orgIdHeaders []string
}

func registerQueryFrontend(app *extkingpin.App) {
	comp := component.QueryFrontend
	cmd := app.Command(comp.String(), "Query frontend command implements a service deployed in front of queriers to improve query parallelization and caching.")
	cfg := &queryFrontendConfig{
		Config: queryfrontend.Config{
			// Max body size is 10 MiB.
			CortexHandlerConfig: &transport.HandlerConfig{
				MaxBodySize: 10 * 1024 * 1024,
			},
			QueryRangeConfig: queryfrontend.QueryRangeConfig{
				Limits: &cortexvalidation.Limits{},
			},
			LabelsConfig: queryfrontend.LabelsConfig{
				Limits: &cortexvalidation.Limits{},
			},
		},
	}

	cfg.http.registerFlag(cmd)

	cmd.Flag("web.disable-cors", "Whether to disable CORS headers to be set by Thanos. By default Thanos sets CORS headers to be allowed by all.").
		Default("false").BoolVar(&cfg.webDisableCORS)

	// Query range tripperware flags.
	cmd.Flag("query-range.align-range-with-step", "Mutate incoming queries to align their start and end with their step for better cache-ability. Note: Grafana dashboards do that by default.").
		Default("true").BoolVar(&cfg.QueryRangeConfig.AlignRangeWithStep)

	cmd.Flag("query-range.request-downsampled", "Make additional query for downsampled data in case of empty or incomplete response to range request.").
		Default("true").BoolVar(&cfg.QueryRangeConfig.RequestDownsampled)

	cmd.Flag("query-range.split-interval", "Split query range requests by an interval and execute in parallel, it should be greater than 0 when query-range.response-cache-config is configured.").
		Default("24h").DurationVar(&cfg.QueryRangeConfig.SplitQueriesByInterval)

	cmd.Flag("query-range.min-split-interval", "Split query range requests above this interval in query-range.horizontal-shards requests of equal range. "+
		"Using this parameter is not allowed with query-range.split-interval. "+
		"One should also set query-range.split-min-horizontal-shards to a value greater than 1 to enable splitting.").
		Default("0").DurationVar(&cfg.QueryRangeConfig.MinQuerySplitInterval)

	cmd.Flag("query-range.max-split-interval", "Split query range below this interval in query-range.horizontal-shards. Queries with a range longer than this value will be split in multiple requests of this length.").
		Default("0").DurationVar(&cfg.QueryRangeConfig.MaxQuerySplitInterval)

	cmd.Flag("query-range.horizontal-shards", "Split queries in this many requests when query duration is below query-range.max-split-interval.").
		Default("0").Int64Var(&cfg.QueryRangeConfig.HorizontalShards)

	cmd.Flag("query-range.max-retries-per-request", "Maximum number of retries for a single query range request; beyond this, the downstream error is returned.").
		Default("5").IntVar(&cfg.QueryRangeConfig.MaxRetries)

	cmd.Flag("query-range.max-query-length", "Limit the query time range (end - start time) in the query-frontend, 0 disables it.").
		Default("0").DurationVar((*time.Duration)(&cfg.QueryRangeConfig.Limits.MaxQueryLength))

	cmd.Flag("query-range.max-query-parallelism", "Maximum number of query range requests will be scheduled in parallel by the Frontend.").
		Default("14").IntVar(&cfg.QueryRangeConfig.Limits.MaxQueryParallelism)

	cmd.Flag("query-range.response-cache-max-freshness", "Most recent allowed cacheable result for query range requests, to prevent caching very recent results that might still be in flux.").
		Default("1m").DurationVar((*time.Duration)(&cfg.QueryRangeConfig.Limits.MaxCacheFreshness))

	cmd.Flag("query-range.partial-response", "Enable partial response for query range requests if no partial_response param is specified. --no-query-range.partial-response for disabling.").
		Default("true").BoolVar(&cfg.QueryRangeConfig.PartialResponseStrategy)

	cfg.QueryRangeConfig.CachePathOrContent = *extflag.RegisterPathOrContent(cmd, "query-range.response-cache-config", "YAML file that contains response cache configuration.", extflag.WithEnvSubstitution())

	// Labels tripperware flags.
	cmd.Flag("labels.split-interval", "Split labels requests by an interval and execute in parallel, it should be greater than 0 when labels.response-cache-config is configured.").
		Default("24h").DurationVar(&cfg.LabelsConfig.SplitQueriesByInterval)

	cmd.Flag("labels.max-retries-per-request", "Maximum number of retries for a single label/series API request; beyond this, the downstream error is returned.").
		Default("5").IntVar(&cfg.LabelsConfig.MaxRetries)

	cmd.Flag("labels.max-query-parallelism", "Maximum number of labels requests will be scheduled in parallel by the Frontend.").
		Default("14").IntVar(&cfg.LabelsConfig.Limits.MaxQueryParallelism)

	cmd.Flag("labels.response-cache-max-freshness", "Most recent allowed cacheable result for labels requests, to prevent caching very recent results that might still be in flux.").
		Default("1m").DurationVar((*time.Duration)(&cfg.LabelsConfig.Limits.MaxCacheFreshness))

	cmd.Flag("labels.partial-response", "Enable partial response for labels requests if no partial_response param is specified. --no-labels.partial-response for disabling.").
		Default("true").BoolVar(&cfg.LabelsConfig.PartialResponseStrategy)

	cmd.Flag("labels.default-time-range", "The default metadata time range duration for retrieving labels through Labels and Series API when the range parameters are not specified.").
		Default("24h").DurationVar(&cfg.DefaultTimeRange)

	cfg.LabelsConfig.CachePathOrContent = *extflag.RegisterPathOrContent(cmd, "labels.response-cache-config", "YAML file that contains response cache configuration.", extflag.WithEnvSubstitution())

	cmd.Flag("cache-compression-type", "Use compression in results cache. Supported values are: 'snappy' and '' (disable compression).").
		Default("").StringVar(&cfg.CacheCompression)

	cmd.Flag("query-frontend.downstream-url", "URL of downstream Prometheus Query compatible API.").
		Default("http://localhost:9090").StringVar(&cfg.DownstreamURL)

	cfg.DownstreamTripperConfig.CachePathOrContent = *extflag.RegisterPathOrContent(cmd, "query-frontend.downstream-tripper-config", "YAML file that contains downstream tripper configuration. If your downstream URL is localhost or 127.0.0.1 then it is highly recommended to increase max_idle_conns_per_host to at least 100.", extflag.WithEnvSubstitution())

	cmd.Flag("query-frontend.compress-responses", "Compress HTTP responses.").
		Default("false").BoolVar(&cfg.CompressResponses)

	cmd.Flag("query-frontend.log-queries-longer-than", "Log queries that are slower than the specified duration. "+
		"Set to 0 to disable. Set to < 0 to enable on all queries.").Default("0").DurationVar(&cfg.CortexHandlerConfig.LogQueriesLongerThan)

	cmd.Flag("query-frontend.org-id-header", "Request header names used to identify the source of slow queries (repeated flag). "+
		"The values of the header will be added to the org id field in the slow query log. "+
		"If multiple headers match the request, the first matching arg specified will take precedence. "+
		"If no headers match 'anonymous' will be used.").PlaceHolder("<http-header-name>").StringsVar(&cfg.orgIdHeaders)

	cmd.Flag("query-frontend.forward-header", "List of headers forwarded by the query-frontend to downstream queriers, default is empty").PlaceHolder("<http-header-name>").StringsVar(&cfg.ForwardHeaders)

	cmd.Flag("query-frontend.vertical-shards", "Number of shards to use when distributing shardable PromQL queries. For more details, you can refer to the Vertical query sharding proposal: https://thanos.io/tip/proposals-accepted/202205-vertical-query-sharding.md").IntVar(&cfg.NumShards)

	reqLogConfig := extkingpin.RegisterRequestLoggingFlags(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		httpLogOpts, err := logging.ParseHTTPOptions(reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		return runQueryFrontend(g, logger, reg, tracer, httpLogOpts, cfg, comp)
	})
}

func parseTransportConfiguration(downstreamTripperConfContentYaml []byte) (*http.Transport, error) {
	downstreamTripper := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	if len(downstreamTripperConfContentYaml) > 0 {
		tripperConfig := &queryfrontend.DownstreamTripperConfig{}
		if err := yaml.UnmarshalStrict(downstreamTripperConfContentYaml, tripperConfig); err != nil {
			return nil, errors.Wrap(err, "parsing downstream tripper config YAML file")
		}

		if tripperConfig.IdleConnTimeout > 0 {
			downstreamTripper.IdleConnTimeout = time.Duration(tripperConfig.IdleConnTimeout)
		}
		if tripperConfig.ResponseHeaderTimeout > 0 {
			downstreamTripper.ResponseHeaderTimeout = time.Duration(tripperConfig.ResponseHeaderTimeout)
		}
		if tripperConfig.TLSHandshakeTimeout > 0 {
			downstreamTripper.TLSHandshakeTimeout = time.Duration(tripperConfig.TLSHandshakeTimeout)
		}
		if tripperConfig.ExpectContinueTimeout > 0 {
			downstreamTripper.ExpectContinueTimeout = time.Duration(tripperConfig.ExpectContinueTimeout)
		}
		if tripperConfig.MaxIdleConns != nil {
			downstreamTripper.MaxIdleConns = *tripperConfig.MaxIdleConns
		}
		if tripperConfig.MaxIdleConnsPerHost != nil {
			downstreamTripper.MaxIdleConnsPerHost = *tripperConfig.MaxIdleConnsPerHost
		}
		if tripperConfig.MaxConnsPerHost != nil {
			downstreamTripper.MaxConnsPerHost = *tripperConfig.MaxConnsPerHost
		}
	}

	return downstreamTripper, nil
}

func runQueryFrontend(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	httpLogOpts []logging.Option,
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
		cacheConfig, err := queryfrontend.NewCacheConfig(logger, labelsCacheConfContentYaml)
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

	tripperWare, err := queryfrontend.NewTripperware(cfg.Config, reg, logger)
	if err != nil {
		return errors.Wrap(err, "setup tripperwares")
	}

	// Create a downstream roundtripper.
	downstreamTripperConfContentYaml, err := cfg.DownstreamTripperConfig.CachePathOrContent.Content()
	if err != nil {
		return err
	}
	downstreamTripper, err := parseTransportConfiguration(downstreamTripperConfContentYaml)
	if err != nil {
		return err
	}

	roundTripper, err := cortexfrontend.NewDownstreamRoundTripper(cfg.DownstreamURL, downstreamTripper)
	if err != nil {
		return errors.Wrap(err, "setup downstream roundtripper")
	}

	// Wrap the downstream RoundTripper into query frontend Tripperware.
	roundTripper = tripperWare(roundTripper)

	// Create the query frontend transport.
	handler := transport.NewHandler(*cfg.CortexHandlerConfig, roundTripper, logger, nil)
	if cfg.CompressResponses {
		handler = gzhttp.GzipHandler(handler)
	}

	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	// Configure Request Logging for HTTP calls.
	logMiddleware := logging.NewHTTPServerMiddleware(logger, httpLogOpts...)
	ins := extpromhttp.NewInstrumentationMiddleware(reg, nil)

	// Start metrics HTTP server.
	{
		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(cfg.http.bindAddress),
			httpserver.WithGracePeriod(time.Duration(cfg.http.gracePeriod)),
			httpserver.WithTLSConfig(cfg.http.tlsConfig),
		)

		instr := func(f http.HandlerFunc) http.HandlerFunc {
			hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				orgId := extractOrgId(cfg, r)
				name := "query-frontend"
				if !cfg.webDisableCORS {
					api.SetCORS(w)
				}
				tracing.HTTPMiddleware(
					tracer,
					name,
					logger,
					ins.NewHandler(
						name,
						gzhttp.GzipHandler(
							middleware.RequestID(
								logMiddleware.HTTPMiddleware(name, f),
							),
						),
					),
					// Cortex frontend middlewares require orgID.
				).ServeHTTP(w, r.WithContext(user.InjectOrgID(r.Context(), orgId)))
			})
			return hf
		}
		srv.Handle("/", instr(handler.ServeHTTP))

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

func extractOrgId(conf *queryFrontendConfig, r *http.Request) string {
	for _, header := range conf.orgIdHeaders {
		headerVal := r.Header.Get(header)
		if headerVal != "" {
			return headerVal
		}
	}
	return "anonymous"
}
