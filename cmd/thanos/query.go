// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tags"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	//"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/discovery/file"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/thanos-io/thanos/pkg/httpconfig"

	extflag "github.com/efficientgo/tools/extkingpin"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	v1 "github.com/thanos-io/thanos/pkg/api/query"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/exemplars"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/metadata"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/rules"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/targets"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/ui"
)

const (
	promqlNegativeOffset = "promql-negative-offset"
	promqlAtModifier     = "promql-at-modifier"
)

// registerQuery registers a query command.
func registerQuery(app *extkingpin.App) {
	comp := component.Query
	cmd := app.Command(comp.String(), "Query node exposing PromQL enabled Query API with data retrieved from multiple store nodes.")

	httpBindAddr, httpGracePeriod, httpTLSConfig := extkingpin.RegisterHTTPFlags(cmd)
	grpcBindAddr, grpcGracePeriod, grpcCert, grpcKey, grpcClientCA, grpcMaxConnAge := extkingpin.RegisterGRPCFlags(cmd)

	// TODO(bwplotka): Remove in 0.27.0.
	secure := cmd.Flag("grpc-client-tls-secure", "Deprecated: Use endpoint.config instead. Use TLS when talking to the gRPC server").Default("false").Bool()
	skipVerify := cmd.Flag("grpc-client-tls-skip-verify", "Deprecated: Use endpoint.config instead. Disable TLS certificate verification i.e self signed, signed by fake CA").Default("false").Bool()
	cert := cmd.Flag("grpc-client-tls-cert", "Deprecated: Use endpoint.config instead. TLS Certificates to use to identify this client to the server").Default("").String()
	key := cmd.Flag("grpc-client-tls-key", "Deprecated: Use endpoint.config instead. TLS Key for the client's certificate").Default("").String()
	caCert := cmd.Flag("grpc-client-tls-ca", "Deprecated: Use endpoint.config instead. TLS CA Certificates to use to verify gRPC servers").Default("").String()
	serverName := cmd.Flag("grpc-client-server-name", "Deprecated: Use endpoint.config instead. Server name to verify the hostname on the returned gRPC certificates. See https://tools.ietf.org/html/rfc4366#section-3.1").Default("").String()

	webRoutePrefix := cmd.Flag("web.route-prefix", "Prefix for API and UI endpoints. This allows thanos UI to be served on a sub-path. Defaults to the value of --web.external-prefix. This option is analogous to --web.route-prefix of Prometheus.").Default("").String()
	webExternalPrefix := cmd.Flag("web.external-prefix", "Static prefix for all HTML links and redirect URLs in the UI query web interface. Actual endpoints are still served on / or the web.route-prefix. This allows thanos UI to be served behind a reverse proxy that strips a URL sub-path.").Default("").String()
	webPrefixHeaderName := cmd.Flag("web.prefix-header", "Name of HTTP request header used for dynamic prefixing of UI links and redirects. This option is ignored if web.external-prefix argument is set. Security risk: enable this option only if a reverse proxy in front of thanos is resetting the header. The --web.prefix-header=X-Forwarded-Prefix option can be useful, for example, if Thanos UI is served via Traefik reverse proxy with PathPrefixStrip option enabled, which sends the stripped prefix value in X-Forwarded-Prefix header. This allows thanos UI to be served on a sub-path.").Default("").String()
	webDisableCORS := cmd.Flag("web.disable-cors", "Whether to disable CORS headers to be set by Thanos. By default Thanos sets CORS headers to be allowed by all.").Default("false").Bool()

	reqLogDecision := cmd.Flag("log.request.decision", "Deprecation Warning - This flag would be soon deprecated, and replaced with `request.logging-config`. Request Logging for logging the start and end of requests. By default this flag is disabled. LogFinishCall: Logs the finish call of the requests. LogStartAndFinishCall: Logs the start and finish call of the requests. NoLogCall: Disable request logging.").Default("").Enum("NoLogCall", "LogFinishCall", "LogStartAndFinishCall", "")

	queryTimeout := extkingpin.ModelDuration(cmd.Flag("query.timeout", "Maximum time to process query by query node.").
		Default("2m"))

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "Maximum number of queries processed concurrently by query node.").
		Default("20").Int()

	lookbackDelta := cmd.Flag("query.lookback-delta", "The maximum lookback duration for retrieving metrics during expression evaluations. PromQL always evaluates the query for the certain timestamp (query range timestamps are deduced by step). Since scrape intervals might be different, PromQL looks back for given amount of time to get latest sample. If it exceeds the maximum lookback delta it assumes series is stale and returns none (a gap). This is why lookback delta should be set to at least 2 times of the slowest scrape interval. If unset it will use the promql default of 5m.").Duration()
	dynamicLookbackDelta := cmd.Flag("query.dynamic-lookback-delta", "Allow for larger lookback duration for queries based on resolution.").Hidden().Default("true").Bool()

	maxConcurrentSelects := cmd.Flag("query.max-concurrent-select", "Maximum number of select requests made concurrently per a query.").
		Default("4").Int()

	queryReplicaLabels := cmd.Flag("query.replica-label", "Labels to treat as a replica indicator along which data is deduplicated. Still you will be able to query without deduplication using 'dedup=false' parameter. Data includes time series, recording rules, and alerting rules.").
		Strings()

	instantDefaultMaxSourceResolution := extkingpin.ModelDuration(cmd.Flag("query.instant.default.max_source_resolution", "default value for max_source_resolution for instant queries. If not set, defaults to 0s only taking raw resolution into account. 1h can be a good value if you use instant queries over time ranges that incorporate times outside of your raw-retention.").Default("0s").Hidden())

	defaultMetadataTimeRange := cmd.Flag("query.metadata.default-time-range", "The default metadata time range duration for retrieving labels through Labels and Series API when the range parameters are not specified. The zero value means range covers the time since the beginning.").Default("0s").Duration()

	selectorLabels := cmd.Flag("selector-label", "Query selector labels that will be exposed in info endpoint (repeated).").
		PlaceHolder("<name>=\"<value>\"").Strings()

	// TODO(bwplotka): Hidden because we plan to extract discovery to separate API: https://github.com/thanos-io/thanos/issues/2600.
	ruleEndpoints := cmd.Flag("rule", "Experimental: Addresses of statically configured rules API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect rule API servers through respective DNS lookups.").
		Hidden().PlaceHolder("<rule>").Strings()
	metadataEndpoints := cmd.Flag("metadata", "Experimental: Addresses of statically configured metadata API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect metadata API servers through respective DNS lookups.").
		Hidden().PlaceHolder("<metadata>").Strings()
	exemplarEndpoints := cmd.Flag("exemplar", "Experimental: Use endpoint or endpoint.config. Addresses of statically configured exemplars API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect exemplars API servers through respective DNS lookups.").
		Hidden().PlaceHolder("<exemplar>").Strings()
	targetEndpoints := cmd.Flag("target", "Experimental: Use endpoint or endpoint.config. Addresses of statically configured target API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect target API servers through respective DNS lookups.").
		Hidden().PlaceHolder("<target>").Strings()

	endpointConfig := extflag.RegisterPathOrContent(cmd, "endpoint.config", "YAML file that contains set of endpoints (e.g Store API) with optional TLS options. To enable TLS either use this option or deprecated ones --grpc-client-tls* .", extflag.WithEnvSubstitution())

	stores := cmd.Flag("store", "Addresses of statically configured store API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect store API servers through respective DNS lookups.").
		PlaceHolder("<store>").Strings()

	strictStores := cmd.Flag("store-strict", "Addresses of only statically configured store API servers that are always used, even if the health check fails. Useful if you have a caching layer on top.").
		PlaceHolder("<staticstore>").Strings()

	// TODO(bwplotka): Remove in 0.27.0.
	fileSDFiles := cmd.Flag("store.sd-files", "Deprecated: Use endpoint.config instead. Path to files that contain addresses of store API servers. The path can be a glob pattern (repeatable).").
		PlaceHolder("<path>").Strings()
	fileSDInterval := extkingpin.ModelDuration(cmd.Flag("store.sd-interval", "Deprecated: Use endpoint.config instead. Refresh interval to re-read file SD files. It is used as a resync fallback.").
		Default("5m"))
	// TODO(bwplotka): Grab this from TTL at some point.
	dnsSDInterval := extkingpin.ModelDuration(cmd.Flag("store.sd-dns-interval", "Deprecated: Use endpoint.config instead. Interval between DNS resolutions.").
		Default("30s"))
	dnsSDResolver := cmd.Flag("store.sd-dns-resolver", fmt.Sprintf("Deprecated: Use endpoint.config instead. Resolver to use. Possible options: [%s, %s]", dns.GolangResolverType, dns.MiekgdnsResolverType)).
		Default(string(dns.MiekgdnsResolverType)).Hidden().String()

	unhealthyStoreTimeout := extkingpin.ModelDuration(cmd.Flag("store.unhealthy-timeout", "Timeout before an unhealthy store is cleaned from the store UI page.").Default("5m"))

	enableAutodownsampling := cmd.Flag("query.auto-downsampling", "Enable automatic adjustment (step / 5) to what source of data should be used in store gateways if no max_source_resolution param is specified.").
		Default("false").Bool()

	enableQueryPartialResponse := cmd.Flag("query.partial-response", "Enable partial response for queries if no partial_response param is specified. --no-query.partial-response for disabling.").
		Default("true").Bool()

	enableRulePartialResponse := cmd.Flag("rule.partial-response", "Enable partial response for rules endpoint. --no-rule.partial-response for disabling.").
		Hidden().Default("true").Bool()

	enableTargetPartialResponse := cmd.Flag("target.partial-response", "Enable partial response for targets endpoint. --no-target.partial-response for disabling.").
		Hidden().Default("true").Bool()

	enableMetricMetadataPartialResponse := cmd.Flag("metric-metadata.partial-response", "Enable partial response for metric metadata endpoint. --no-metric-metadata.partial-response for disabling.").
		Hidden().Default("true").Bool()

	featureList := cmd.Flag("enable-feature", "Comma separated experimental feature names to enable.The current list of features is "+promqlNegativeOffset+" and "+promqlAtModifier+".").Default("").Strings()

	enableExemplarPartialResponse := cmd.Flag("exemplar.partial-response", "Enable partial response for exemplar endpoint. --no-exemplar.partial-response for disabling.").
		Hidden().Default("true").Bool()

	defaultEvaluationInterval := extkingpin.ModelDuration(cmd.Flag("query.default-evaluation-interval", "Set default evaluation interval for sub queries.").Default("1m"))

	defaultRangeQueryStep := extkingpin.ModelDuration(cmd.Flag("query.default-step", "Set default step for range queries. Default step is only used when step is not set in UI. In such cases, Thanos UI will use default step to calculate resolution (resolution = max(rangeSeconds / 250, defaultStep)). This will not work from Grafana, but Grafana has __step variable which can be used.").
		Default("1s"))

	storeResponseTimeout := extkingpin.ModelDuration(cmd.Flag("store.response-timeout", "If a Store doesn't send any data in this specified duration then a Store will be ignored and partial data will be returned if it's enabled. 0 disables timeout.").Default("0ms"))
	reqLogConfig := extkingpin.RegisterRequestLoggingFlags(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		selectorLset, err := parseFlagLabels(*selectorLabels)
		if err != nil {
			return errors.Wrap(err, "parse federation labels")
		}

		var enableNegativeOffset, enableAtModifier bool
		for _, feature := range *featureList {
			if feature == promqlNegativeOffset {
				enableNegativeOffset = true
			}
			if feature == promqlAtModifier {
				enableAtModifier = true
			}
		}

		if dup := firstDuplicate(*stores); dup != "" {
			return errors.Errorf("Address %s is duplicated for --store flag.", dup)
		}

		if dup := firstDuplicate(*ruleEndpoints); dup != "" {
			return errors.Errorf("Address %s is duplicated for --rule flag.", dup)
		}

		if dup := firstDuplicate(*metadataEndpoints); dup != "" {
			return errors.Errorf("Address %s is duplicated for --metadata flag.", dup)
		}

		if dup := firstDuplicate(*exemplarEndpoints); dup != "" {
			return errors.Errorf("Address %s is duplicated for --exemplar flag.", dup)
		}

		httpLogOpts, err := logging.ParseHTTPOptions(*reqLogDecision, reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		tagOpts, grpcLogOpts, err := logging.ParsegRPCOptions(*reqLogDecision, reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		if dup := firstDuplicate(*targetEndpoints); dup != "" {
			return errors.Errorf("Address %s is duplicated for --target flag.", dup)
		}

		endpointConfigYAML, err := endpointConfig.Content()
		if err != nil {
			return err
		}

		if *secure && len(endpointConfigYAML) != 0 {
			return errors.Errorf("deprecated flags --grpc-client-tls* and new --endpoint.config flag cannot be specified at the same time; use either of those")
		}

		var fileSDConfig *file.SDConfig
		if len(*fileSDFiles) > 0 {
			fileSDConfig = &file.SDConfig{
				Files:           *fileSDFiles,
				RefreshInterval: *fileSDInterval,
			}
		}

		if *webRoutePrefix == "" {
			*webRoutePrefix = *webExternalPrefix
		}

		if *webRoutePrefix != *webExternalPrefix {
			level.Warn(logger).Log("msg", "different values for --web.route-prefix and --web.external-prefix detected, web UI may not work without a reverse-proxy.")
		}

		return runQuery(
			g,
			logger,
			reg,
			tracer,
			httpLogOpts,
			grpcLogOpts,
			tagOpts,
			*grpcBindAddr,
			time.Duration(*grpcGracePeriod),
			*grpcCert,
			*grpcKey,
			*grpcClientCA,
			*grpcMaxConnAge,
			*secure,
			*skipVerify,
			*cert,
			*key,
			*caCert,
			*serverName,
			*httpBindAddr,
			*httpTLSConfig,
			time.Duration(*httpGracePeriod),
			*webRoutePrefix,
			*webExternalPrefix,
			*webPrefixHeaderName,
			*maxConcurrentQueries,
			*maxConcurrentSelects,
			time.Duration(*defaultRangeQueryStep),
			time.Duration(*queryTimeout),
			*lookbackDelta,
			*dynamicLookbackDelta,
			time.Duration(*defaultEvaluationInterval),
			time.Duration(*storeResponseTimeout),
			*queryReplicaLabels,
			selectorLset,
			getFlagsMap(cmd.Flags()),
			*stores,
			*ruleEndpoints,
			*targetEndpoints,
			*metadataEndpoints,
			*exemplarEndpoints,
			*enableAutodownsampling,
			*enableQueryPartialResponse,
			*enableRulePartialResponse,
			*enableTargetPartialResponse,
			*enableMetricMetadataPartialResponse,
			*enableExemplarPartialResponse,
			fileSDConfig,
			endpointConfigYAML,
			time.Duration(*dnsSDInterval),
			*dnsSDResolver,
			time.Duration(*unhealthyStoreTimeout),
			time.Duration(*instantDefaultMaxSourceResolution),
			*defaultMetadataTimeRange,
			*strictStores,
			*webDisableCORS,
			enableAtModifier,
			enableNegativeOffset,
			component.Query,
		)
	})
}

// runQuery starts a server that exposes PromQL Query API. It is responsible for querying configured
// store nodes, merging and duplicating the data to satisfy user query.
func runQuery(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	httpLogOpts []logging.Option,
	grpcLogOpts []grpc_logging.Option,
	tagOpts []tags.Option,
	grpcBindAddr string,
	grpcGracePeriod time.Duration,
	grpcCert string,
	grpcKey string,
	grpcClientCA string,
	grpcMaxConnAge time.Duration,
	secure bool,
	skipVerify bool,
	cert string,
	key string,
	caCert string,
	serverName string,
	httpBindAddr string,
	httpTLSConfig string,
	httpGracePeriod time.Duration,
	webRoutePrefix string,
	webExternalPrefix string,
	webPrefixHeaderName string,
	maxConcurrentQueries int,
	maxConcurrentSelects int,
	defaultRangeQueryStep time.Duration,
	queryTimeout time.Duration,
	lookbackDelta time.Duration,
	dynamicLookbackDelta bool,
	defaultEvaluationInterval time.Duration,
	storeResponseTimeout time.Duration,
	queryReplicaLabels []string,
	selectorLset labels.Labels,
	flagsMap map[string]string,
	storeAddrs []string,
	ruleAddrs []string,
	targetAddrs []string,
	metadataAddrs []string,
	exemplarAddrs []string,
	enableAutodownsampling bool,
	enableQueryPartialResponse bool,
	enableRulePartialResponse bool,
	enableTargetPartialResponse bool,
	enableMetricMetadataPartialResponse bool,
	enableExemplarPartialResponse bool,
	fileSDConfig *file.SDConfig,
	endpointConfigYAML []byte,
	dnsSDInterval time.Duration,
	dnsSDResolver string,
	unhealthyStoreTimeout time.Duration,
	instantDefaultMaxSourceResolution time.Duration,
	defaultMetadataTimeRange time.Duration,
	strictStores []string,
	disableCORS bool,
	enableAtModifier bool,
	enableNegativeOffset bool,
	comp component.Component,
) error {
	// TODO(bplotka in PR #513 review): Move arguments into struct.
	// duplicatedStores := promauto.With(reg).NewCounter(prometheus.CounterOpts{
	// 	Name: "thanos_query_duplicated_store_addresses_total",
	// 	Help: "The number of times a duplicated store addresses is detected from the different configs in query",
	// })

	// TLSConfig for endpoints provided in --store, --store.sd-files and --store-strict.
	var TLSConfig httpconfig.TLSConfig
	if secure {
		TLSConfig = httpconfig.TLSConfig{
			CertFile:   cert,
			KeyFile:    key,
			CAFile:     caCert,
			ServerName: serverName,
		}
	}

	// TODO(bwplotka): Allow filtering by API through config.
	combinedAddresses := storeAddrs
	combinedAddresses = append(combinedAddresses, ruleAddrs...)
	combinedAddresses = append(combinedAddresses, metadataAddrs...)
	combinedAddresses = append(combinedAddresses, exemplarAddrs...)
	combinedAddresses = append(combinedAddresses, targetAddrs...)

	// Create endpoint config combining flag-based options with --endpoint.config.
	endpointConfig, err := query.LoadConfig(endpointConfigYAML, combinedAddresses, strictStores, fileSDConfig, TLSConfig)
	if err != nil {
		return errors.Wrap(err, "loading endpoint config")
	}

	dnsProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_query_endpoints_", reg),
		dns.ResolverType(dnsSDResolver),
	)

	engineOpts := promql.EngineOpts{
		Logger: logger,
		Reg:    reg,
		// TODO(bwplotka): Expose this as a flag: https://github.com/thanos-io/thanos/issues/703.
		MaxSamples:    math.MaxInt32,
		Timeout:       queryTimeout,
		LookbackDelta: lookbackDelta,
		NoStepSubqueryIntervalFn: func(int64) int64 {
			return defaultEvaluationInterval.Milliseconds()
		},
		EnableAtModifier:     enableAtModifier,
		EnableNegativeOffset: enableNegativeOffset,
	}

	var groups []*query.EndpointGroup
	endpointSetGRPCMetrics := extgrpc.ClientGRPCMetrics(reg, "endpointset")
	for _, config := range endpointConfig {
		dialOpts, err := extgrpc.ClientGRPCOpts(logger, tracer, endpointSetGRPCMetrics, config.GRPCClientConfig)
		if err != nil {
			return errors.Wrap(err, "building gRPC options")
		}

		var g *query.EndpointGroup

		var spec []query.EndpointSpec
		if config.Mode == query.StrictEndpointMode {
			// Add strict & static nodes.
			for _, addr := range config.EndpointsConfig.Addresses {
				if dns.IsDynamicNode(addr) {
					return errors.Errorf("%s is a dynamically specified store i.e. it uses SD and that is not permitted under strict mode. Use --store for this", addr)
				}
				spec = append(spec, query.NewGRPCEndpointSpec(addr, true))
			}

			// No dynamic resources when endpoint is strict.
			g = query.NewEndpointGroup(nil, dialOpts)
		} else {
			// TODO(bwplotka): Consider adding provider per config name, for instrumentation purposes, but only if strongly requested.
			d, err := extgrpc.NewDiscoverer(logger, httpconfig.EndpointsConfig, dnsProvider.Clone())
			if err != nil {
				return errors.Wrap(err, "building discoverer")
			}
			addDiscoveryGroups(g, d, dnsSDInterval)
			g = query.NewEndpointGroup(d, dialOpts)

		}
	}

	endpointSet := query.NewEndpointSet(
		logger,
		reg,
		groups,
		unhealthyStoreTimeout,
	)

	var (
		proxy            = store.NewProxyStore(logger, reg, endpointSet.GetStoreClients, component.Query, selectorLset, storeResponseTimeout)
		rulesProxy       = rules.NewProxy(logger, endpointSet.GetRulesClients)
		targetsProxy     = targets.NewProxy(logger, endpointSet.GetTargetsClients)
		metadataProxy    = metadata.NewProxy(logger, endpointSet.GetMetricMetadataClients)
		exemplarsProxy   = exemplars.NewProxy(logger, endpointSet.GetExemplarsStores, selectorLset)
		queryableCreator = query.NewQueryableCreator(
			logger,
			extprom.WrapRegistererWithPrefix("thanos_query_", reg),
			proxy,
			maxConcurrentSelects,
			queryTimeout,
		)
		grpcProbe    = prober.NewGRPC()
		httpProbe    = prober.NewHTTP()
		statusProber = prober.Combine(
			httpProbe,
			grpcProbe,
			prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
		)
	)

	// Start query API + UI HTTP server.
	{
		router := route.New()

		// RoutePrefix must always start with '/'.
		webRoutePrefix = "/" + strings.Trim(webRoutePrefix, "/")

		// Redirect from / to /webRoutePrefix.
		if webRoutePrefix != "/" {
			router.Get("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, webRoutePrefix+"/graph", http.StatusFound)
			})
			router.Get(webRoutePrefix, func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, webRoutePrefix+"/graph", http.StatusFound)
			})
			router = router.WithPrefix(webRoutePrefix)
		}

		// Configure Request Logging for HTTP calls.
		logMiddleware := logging.NewHTTPServerMiddleware(logger, httpLogOpts...)

		ins := extpromhttp.NewInstrumentationMiddleware(reg, nil)
		// TODO(bplotka in PR #513 review): pass all flags, not only the flags needed by prefix rewriting.
		ui.NewQueryUI(logger, endpointSet.GetEndpointStatus, webExternalPrefix, webPrefixHeaderName).Register(router, ins)

		api := v1.NewQueryAPI(
			logger,
			endpointSet.GetEndpointStatus,
			engineFactory(promql.NewEngine, engineOpts, dynamicLookbackDelta),
			queryableCreator,
			// NOTE: Will share the same replica label as the query for now.
			rules.NewGRPCClientWithDedup(rulesProxy, queryReplicaLabels),
			targets.NewGRPCClientWithDedup(targetsProxy, queryReplicaLabels),
			metadata.NewGRPCClient(metadataProxy),
			exemplars.NewGRPCClientWithDedup(exemplarsProxy, queryReplicaLabels),
			enableAutodownsampling,
			enableQueryPartialResponse,
			enableRulePartialResponse,
			enableTargetPartialResponse,
			enableMetricMetadataPartialResponse,
			enableExemplarPartialResponse,
			queryReplicaLabels,
			flagsMap,
			defaultRangeQueryStep,
			instantDefaultMaxSourceResolution,
			defaultMetadataTimeRange,
			disableCORS,
			gate.New(
				extprom.WrapRegistererWithPrefix("thanos_query_concurrent_", reg),
				maxConcurrentQueries,
			),
			reg,
		)

		api.Register(router.WithPrefix("/api/v1"), tracer, logger, ins, logMiddleware)

		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(httpBindAddr),
			httpserver.WithGracePeriod(httpGracePeriod),
			httpserver.WithTLSConfig(httpTLSConfig),
		)
		srv.Handle("/", router)

		g.Add(func() error {
			statusProber.Healthy()

			return srv.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			defer statusProber.NotHealthy(err)

			srv.Shutdown(err)
		})
	}
	// Start query (proxy) gRPC StoreAPI.
	{
		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), grpcCert, grpcKey, grpcClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		s := grpcserver.New(logger, reg, tracer, grpcLogOpts, tagOpts, comp, grpcProbe,
			grpcserver.WithServer(store.RegisterStoreServer(proxy)),
			grpcserver.WithServer(rules.RegisterRulesServer(rulesProxy)),
			grpcserver.WithServer(targets.RegisterTargetsServer(targetsProxy)),
			grpcserver.WithServer(metadata.RegisterMetadataServer(metadataProxy)),
			grpcserver.WithServer(exemplars.RegisterExemplarsServer(exemplarsProxy)),
			grpcserver.WithListen(grpcBindAddr),
			grpcserver.WithGracePeriod(grpcGracePeriod),
			grpcserver.WithTLSConfig(tlsCfg),
			grpcserver.WithMaxConnAge(grpcMaxConnAge),
		)

		g.Add(func() error {
			statusProber.Ready()
			return s.ListenAndServe()
		}, func(error) {
			statusProber.NotReady(err)
			s.Shutdown(err)
		})
	}

	level.Info(logger).Log("msg", "starting query node")
	return nil
}

// firstDuplicate returns the first duplicate string in the given string slice
// or empty string if none was found.
func firstDuplicate(ss []string) string {
	set := map[string]struct{}{}

	for _, s := range ss {
		if _, ok := set[s]; ok {
			return s
		}

		set[s] = struct{}{}
	}

	return ""
}

// engineFactory creates from 1 to 3 promql.Engines depending on
// dynamicLookbackDelta and eo.LookbackDelta and returns a function
// that returns appropriate engine for given maxSourceResolutionMillis.
//
// TODO: it seems like a good idea to tweak Prometheus itself
// instead of creating several Engines here.
func engineFactory(
	newEngine func(promql.EngineOpts) *promql.Engine,
	eo promql.EngineOpts,
	dynamicLookbackDelta bool,
) func(int64) *promql.Engine {
	resolutions := []int64{downsample.ResLevel0}
	if dynamicLookbackDelta {
		resolutions = []int64{downsample.ResLevel0, downsample.ResLevel1, downsample.ResLevel2}
	}
	var (
		engines = make([]*promql.Engine, len(resolutions))
		ld      = eo.LookbackDelta.Milliseconds()
	)
	wrapReg := func(engineNum int) prometheus.Registerer {
		return extprom.WrapRegistererWith(map[string]string{"engine": strconv.Itoa(engineNum)}, eo.Reg)
	}

	lookbackDelta := eo.LookbackDelta
	for i, r := range resolutions {
		if ld < r {
			lookbackDelta = time.Duration(r) * time.Millisecond
		}
		engines[i] = newEngine(promql.EngineOpts{
			Logger:                   eo.Logger,
			Reg:                      wrapReg(i),
			MaxSamples:               eo.MaxSamples,
			Timeout:                  eo.Timeout,
			ActiveQueryTracker:       eo.ActiveQueryTracker,
			LookbackDelta:            lookbackDelta,
			NoStepSubqueryIntervalFn: eo.NoStepSubqueryIntervalFn,
		})
	}
	return func(maxSourceResolutionMillis int64) *promql.Engine {
		for i := len(resolutions) - 1; i >= 1; i-- {
			left := resolutions[i-1]
			if resolutions[i-1] < ld {
				left = ld
			}
			if left < maxSourceResolutionMillis {
				return engines[i]
			}
		}
		return engines[0]
	}
}
