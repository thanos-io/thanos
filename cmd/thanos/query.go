// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	apiv1 "github.com/thanos-io/thanos/pkg/api/query"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/exemplars"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/info"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/metadata"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/rules"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/status"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/strutil"
	"github.com/thanos-io/thanos/pkg/targets"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/ui"
)

const (
	promqlNegativeOffset        = "promql-negative-offset"
	promqlAtModifier            = "promql-at-modifier"
	queryPushdown               = "query-pushdown"
	promqlExperimentalFunctions = "promql-experimental-functions"
)

// registerQuery registers a query command.
func registerQuery(app *extkingpin.App) {
	comp := component.Query
	cmd := app.Command(comp.String(), "Query node exposing PromQL enabled Query API with data retrieved from multiple store nodes.")

	httpBindAddr, httpGracePeriod, httpTLSConfig := extkingpin.RegisterHTTPFlags(cmd)

	var grpcServerConfig grpcConfig
	grpcServerConfig.registerFlag(cmd)

	var grpcClientConfig grpcClientConfig
	grpcClientConfig.registerFlag(cmd)

	webRoutePrefix := cmd.Flag("web.route-prefix", "Prefix for API and UI endpoints. This allows thanos UI to be served on a sub-path. Defaults to the value of --web.external-prefix. This option is analogous to --web.route-prefix of Prometheus.").Default("").String()
	webExternalPrefix := cmd.Flag("web.external-prefix", "Static prefix for all HTML links and redirect URLs in the UI query web interface. Actual endpoints are still served on / or the web.route-prefix. This allows thanos UI to be served behind a reverse proxy that strips a URL sub-path.").Default("").String()
	webPrefixHeaderName := cmd.Flag("web.prefix-header", "Name of HTTP request header used for dynamic prefixing of UI links and redirects. This option is ignored if web.external-prefix argument is set. Security risk: enable this option only if a reverse proxy in front of thanos is resetting the header. The --web.prefix-header=X-Forwarded-Prefix option can be useful, for example, if Thanos UI is served via Traefik reverse proxy with PathPrefixStrip option enabled, which sends the stripped prefix value in X-Forwarded-Prefix header. This allows thanos UI to be served on a sub-path.").Default("").String()
	webDisableCORS := cmd.Flag("web.disable-cors", "Whether to disable CORS headers to be set by Thanos. By default Thanos sets CORS headers to be allowed by all.").Default("false").Bool()

	queryTimeout := extkingpin.ModelDuration(cmd.Flag("query.timeout", "Maximum time to process query by query node.").
		Default("2m"))

	defaultEngine := cmd.Flag("query.promql-engine", "Default PromQL engine to use.").Default(string(apiv1.PromqlEnginePrometheus)).
		Enum(string(apiv1.PromqlEnginePrometheus), string(apiv1.PromqlEngineThanos))
	disableQueryFallback := cmd.Flag("query.disable-fallback", "If set then thanos engine will throw an error if query falls back to prometheus engine").Hidden().Default("false").Bool()

	extendedFunctionsEnabled := cmd.Flag("query.enable-x-functions", "Whether to enable extended rate functions (xrate, xincrease and xdelta). Only has effect when used with Thanos engine.").Default("false").Bool()
	promqlQueryMode := cmd.Flag("query.mode", "PromQL query mode. One of: local, distributed.").
		Default(string(apiv1.PromqlQueryModeLocal)).
		Enum(string(apiv1.PromqlQueryModeLocal), string(apiv1.PromqlQueryModeDistributed))

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "Maximum number of queries processed concurrently by query node.").
		Default("20").Int()

	lookbackDelta := cmd.Flag("query.lookback-delta", "The maximum lookback duration for retrieving metrics during expression evaluations. PromQL always evaluates the query for the certain timestamp (query range timestamps are deduced by step). Since scrape intervals might be different, PromQL looks back for given amount of time to get latest sample. If it exceeds the maximum lookback delta it assumes series is stale and returns none (a gap). This is why lookback delta should be set to at least 2 times of the slowest scrape interval. If unset it will use the promql default of 5m.").Duration()
	dynamicLookbackDelta := cmd.Flag("query.dynamic-lookback-delta", "Allow for larger lookback duration for queries based on resolution.").Hidden().Default("true").Bool()

	maxConcurrentSelects := cmd.Flag("query.max-concurrent-select", "Maximum number of select requests made concurrently per a query.").
		Default("4").Int()

	queryConnMetricLabels := cmd.Flag("query.conn-metric.label", "Optional selection of query connection metric labels to be collected from endpoint set").
		Default(string(query.ExternalLabels), string(query.StoreType)).
		Enums(string(query.ExternalLabels), string(query.StoreType), string(query.IPPort))

	deduplicationFunc := cmd.Flag("deduplication.func", "Experimental. Deduplication algorithm for merging overlapping series. "+
		"Possible values are: \"penalty\", \"chain\". If no value is specified, penalty based deduplication algorithm will be used. "+
		"When set to chain, the default compact deduplication merger is used, which performs 1:1 deduplication for samples. At least one replica label has to be set via --query.replica-label flag.").
		Default(dedup.AlgorithmPenalty).Enum(dedup.AlgorithmPenalty, dedup.AlgorithmChain)

	queryReplicaLabels := cmd.Flag("query.replica-label", "Labels to treat as a replica indicator along which data is deduplicated. Still you will be able to query without deduplication using 'dedup=false' parameter. Data includes time series, recording rules, and alerting rules. Flag may be specified multiple times as well as a comma separated list of labels.").
		Strings()
	queryPartitionLabels := cmd.Flag("query.partition-label", "Labels that partition the leaf queriers. This is used to scope down the labelsets of leaf queriers when using the distributed query mode. If set, these labels must form a partition of the leaf queriers. Partition labels must not intersect with replica labels. Every TSDB of a leaf querier must have these labels. This is useful when there are multiple external labels that are irrelevant for the partition as it allows the distributed engine to ignore them for some optimizations. If this is empty then all labels are used as partition labels.").Strings()

	// currently, we choose the highest MinT of an engine when querying multiple engines. This flag allows to change this behavior to choose the lowest MinT.
	queryDistributedWithOverlappingInterval := cmd.Flag("query.distributed-with-overlapping-interval", "Allow for distributed queries using an engines lowest MinT.").Hidden().Default("false").Bool()

	instantDefaultMaxSourceResolution := extkingpin.ModelDuration(cmd.Flag("query.instant.default.max_source_resolution", "default value for max_source_resolution for instant queries. If not set, defaults to 0s only taking raw resolution into account. 1h can be a good value if you use instant queries over time ranges that incorporate times outside of your raw-retention.").Default("0s").Hidden())

	defaultMetadataTimeRange := cmd.Flag("query.metadata.default-time-range", "The default metadata time range duration for retrieving labels through Labels and Series API when the range parameters are not specified. The zero value means range covers the time since the beginning.").Default("0s").Duration()

	selectorLabels := cmd.Flag("selector-label", "Query selector labels that will be exposed in info endpoint (repeated).").
		PlaceHolder("<name>=\"<value>\"").Strings()

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

	activeQueryDir := cmd.Flag("query.active-query-path", "Directory to log currently active queries in the queries.active file.").Default("").String()

	featureList := cmd.Flag("enable-feature", "Comma separated feature names to enable. Valid options for now: promql-experimental-functions (enables promql experimental functions in query)").Default("").Strings()

	enableExemplarPartialResponse := cmd.Flag("exemplar.partial-response", "Enable partial response for exemplar endpoint. --no-exemplar.partial-response for disabling.").
		Hidden().Default("true").Bool()

	enableStatusPartialResponse := cmd.Flag("status.partial-response", "Enable partial response for status endpoint. --no-status.partial-response for disabling.").
		Hidden().Default("true").Bool()

	defaultEvaluationInterval := extkingpin.ModelDuration(cmd.Flag("query.default-evaluation-interval", "Set default evaluation interval for sub queries.").Default("1m"))

	defaultRangeQueryStep := extkingpin.ModelDuration(cmd.Flag("query.default-step", "Set default step for range queries. Default step is only used when step is not set in UI. In such cases, Thanos UI will use default step to calculate resolution (resolution = max(rangeSeconds / 250, defaultStep)). This will not work from Grafana, but Grafana has __step variable which can be used.").
		Default("1s"))

	storeResponseTimeout := extkingpin.ModelDuration(cmd.Flag("store.response-timeout", "If a Store doesn't send any data in this specified duration then a Store will be ignored and partial data will be returned if it's enabled. 0 disables timeout.").Default("0ms"))

	storeSelectorRelabelConf := *extflag.RegisterPathOrContent(
		cmd,
		"selector.relabel-config",
		"YAML file with relabeling configuration that allows selecting blocks to query based on their external labels. It follows the Thanos sharding relabel-config syntax. For format details see: https://thanos.io/tip/thanos/sharding.md/#relabelling ",
		extflag.WithEnvSubstitution(),
	)

	reqLogConfig := extkingpin.RegisterRequestLoggingFlags(cmd)

	alertQueryURL := cmd.Flag("alert.query-url", "The external Thanos Query URL that would be set in all alerts 'Source' field.").String()
	grpcProxyStrategy := cmd.Flag("grpc.proxy-strategy", "Strategy to use when proxying Series requests to leaf nodes. Hidden and only used for testing, will be removed after lazy becomes the default.").Default(string(store.EagerRetrieval)).Hidden().Enum(string(store.EagerRetrieval), string(store.LazyRetrieval))

	queryTelemetryDurationQuantiles := cmd.Flag("query.telemetry.request-duration-seconds-quantiles", "The quantiles for exporting metrics about the request duration quantiles.").Default("0.1", "0.25", "0.75", "1.25", "1.75", "2.5", "3", "5", "10").Float64List()
	queryTelemetrySamplesQuantiles := cmd.Flag("query.telemetry.request-samples-quantiles", "The quantiles for exporting metrics about the samples count quantiles.").Default("100", "1000", "10000", "100000", "1000000").Float64List()
	queryTelemetrySeriesQuantiles := cmd.Flag("query.telemetry.request-series-seconds-quantiles", "The quantiles for exporting metrics about the series count quantiles.").Default("10", "100", "1000", "10000", "100000").Float64List()

	tenantHeader := cmd.Flag("query.tenant-header", "HTTP header to determine tenant.").Default(tenancy.DefaultTenantHeader).String()
	defaultTenant := cmd.Flag("query.default-tenant-id", "Default tenant ID to use if tenant header is not present").Default(tenancy.DefaultTenant).String()
	tenantCertField := cmd.Flag("query.tenant-certificate-field", "Use TLS client's certificate field to determine tenant for write requests. Must be one of "+tenancy.CertificateFieldOrganization+", "+tenancy.CertificateFieldOrganizationalUnit+" or "+tenancy.CertificateFieldCommonName+". This setting will cause the query.tenant-header flag value to be ignored.").Default("").Enum("", tenancy.CertificateFieldOrganization, tenancy.CertificateFieldOrganizationalUnit, tenancy.CertificateFieldCommonName)
	enforceTenancy := cmd.Flag("query.enforce-tenancy", "Enforce tenancy on Query APIs. Responses are returned only if the label value of the configured tenant-label-name and the value of the tenant header matches.").Default("false").Bool()
	tenantLabel := cmd.Flag("query.tenant-label-name", "Label name to use when enforcing tenancy (if --query.enforce-tenancy is enabled).").Default(tenancy.DefaultTenantLabel).String()
	// TODO(bwplotka): Grab this from TTL at some point.
	dnsSDInterval := extkingpin.ModelDuration(cmd.Flag("store.sd-dns-interval", "Interval between DNS resolutions.").
		Default("30s"))

	dnsSDResolver := cmd.Flag("store.sd-dns-resolver", fmt.Sprintf("Resolver to use. Possible options: [%s, %s]", dns.GolangResolverType, dns.MiekgdnsResolverType)).
		Default(string(dns.MiekgdnsResolverType)).Hidden().String()

	unhealthyStoreTimeout := extkingpin.ModelDuration(cmd.Flag("store.unhealthy-timeout", "Timeout before an unhealthy store is cleaned from the store UI page.").Default("5m"))

	endpointInfoTimeout := extkingpin.ModelDuration(cmd.Flag("endpoint.info-timeout", "Timeout of gRPC Info requests.").Default("5s").Hidden())

	endpointSetConfig := extflag.RegisterPathOrContent(cmd, "endpoint.sd-config", "Config File with endpoint definitions")

	endpointSetConfigReloadInterval := extkingpin.ModelDuration(cmd.Flag("endpoint.sd-config-reload-interval", "Interval between endpoint config refreshes").Default("5m"))

	legacyFileSDFiles := cmd.Flag("store.sd-files", "(Deprecated) Path to files that contain addresses of store API servers. The path can be a glob pattern (repeatable).").
		PlaceHolder("<path>").Strings()

	legacyFileSDInterval := extkingpin.ModelDuration(cmd.Flag("store.sd-interval", "(Deprecated) Refresh interval to re-read file SD files. It is used as a resync fallback.").
		Default("5m"))

	endpoints := extkingpin.Addrs(cmd.Flag("endpoint", "(Deprecated): Addresses of statically configured Thanos API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect Thanos API servers through respective DNS lookups.").PlaceHolder("<endpoint>"))

	endpointGroups := extkingpin.Addrs(cmd.Flag("endpoint-group", "(Deprecated, Experimental): DNS name of statically configured Thanos API server groups (repeatable). Targets resolved from the DNS name will be queried in a round-robin, instead of a fanout manner. This flag should be used when connecting a Thanos Query to HA groups of Thanos components.").PlaceHolder("<endpoint-group>"))

	strictEndpoints := extkingpin.Addrs(cmd.Flag("endpoint-strict", "(Deprecated): Addresses of only statically configured Thanos API servers that are always used, even if the health check fails. Useful if you have a caching layer on top.").
		PlaceHolder("<endpoint-strict>"))

	strictEndpointGroups := extkingpin.Addrs(cmd.Flag("endpoint-group-strict", "(Deprecated, Experimental): DNS name of statically configured Thanos API server groups (repeatable) that are always used, even if the health check fails.").PlaceHolder("<endpoint-group-strict>"))

	injectTestAddresses := extkingpin.Addrs(cmd.Flag("inject-test-addresses", "Inject test addresses for DNS resolver (repeatable).").PlaceHolder("<inject-test-address>").Hidden())

	lazyRetrievalMaxBufferedResponses := cmd.Flag("query.lazy-retrieval-max-buffered-responses", "The lazy retrieval strategy can buffer up to this number of responses. This is to limit the memory usage. This flag takes effect only when the lazy retrieval strategy is enabled.").
		Default("20").Hidden().Int()

	var storeRateLimits store.SeriesSelectLimits
	storeRateLimits.RegisterFlags(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, debugLogging bool) error {
		selectorLset, err := parseFlagLabels(*selectorLabels)
		if err != nil {
			return errors.Wrap(err, "parse federation labels")
		}

		for _, feature := range *featureList {
			if feature == promqlExperimentalFunctions {
				parser.EnableExperimentalFunctions = true
				level.Info(logger).Log("msg", "Experimental PromQL functions enabled.", "option", promqlExperimentalFunctions)
			}
			if feature == promqlAtModifier {
				level.Warn(logger).Log("msg", "This option for --enable-feature is now permanently enabled and therefore a no-op.", "option", promqlAtModifier)
			}
			if feature == promqlNegativeOffset {
				level.Warn(logger).Log("msg", "This option for --enable-feature is now permanently enabled and therefore a no-op.", "option", promqlNegativeOffset)
			}
			if feature == queryPushdown {
				level.Warn(logger).Log("msg", "This option for --enable-feature is now permanently deprecated and therefore ignored.", "option", queryPushdown)
			}
		}

		httpLogOpts, err := logging.ParseHTTPOptions(reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		grpcLogOpts, logFilterMethods, err := logging.ParsegRPCOptions(reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		if *webRoutePrefix == "" {
			*webRoutePrefix = *webExternalPrefix
		}

		if *webRoutePrefix != *webExternalPrefix {
			level.Warn(logger).Log("msg", "different values for --web.route-prefix and --web.external-prefix detected, web UI may not work without a reverse-proxy.")
		}

		tsdbRelabelConfig, err := storeSelectorRelabelConf.Content()
		if err != nil {
			return errors.Wrap(err, "error while parsing tsdb selector configuration")
		}
		tsdbSelector, err := block.ParseRelabelConfig(tsdbRelabelConfig, block.SelectorSupportedRelabelActions)
		if err != nil {
			return err
		}

		dialOpts, err := grpcClientConfig.dialOptions(logger, reg, tracer)
		if err != nil {
			return err
		}

		globalTLSOpt, err := extgrpc.StoreClientTLSCredentials(logger, grpcClientConfig.secure, grpcClientConfig.skipVerify, grpcClientConfig.cert, grpcClientConfig.key, grpcClientConfig.caCert, grpcClientConfig.serverName, grpcClientConfig.minTLSVersion)
		if err != nil {
			return err
		}

		globalTLSConfig := &tlsConfig{
			Enabled:                  &grpcClientConfig.secure,
			InsecureSkipVerification: &grpcClientConfig.skipVerify,
			CertFile:                 &grpcClientConfig.cert,
			KeyFile:                  &grpcClientConfig.key,
			CAFile:                   &grpcClientConfig.caCert,
			MinVersion:               &grpcClientConfig.minTLSVersion,
		}

		if *promqlQueryMode != string(apiv1.PromqlQueryModeLocal) {
			level.Info(logger).Log("msg", "Distributed query mode enabled, using Thanos as the default query engine.")
			*defaultEngine = string(apiv1.PromqlEngineThanos)
		}

		endpointSet, err := setupEndpointSet(
			g,
			comp,
			reg,
			logger,
			endpointSetConfig,
			time.Duration(*endpointSetConfigReloadInterval),
			*legacyFileSDFiles,
			time.Duration(*legacyFileSDInterval),
			*endpoints,
			*endpointGroups,
			*strictEndpoints,
			*strictEndpointGroups,
			*dnsSDResolver,
			time.Duration(*dnsSDInterval),
			time.Duration(*unhealthyStoreTimeout),
			time.Duration(*endpointInfoTimeout),
			time.Duration(*queryTimeout),
			dialOpts,
			globalTLSConfig,
			globalTLSOpt,
			grpcClientConfig.compression, // global grpc tls compression.
			*injectTestAddresses,
			*queryConnMetricLabels...,
		)
		if err != nil {
			return err
		}

		return runQuery(
			g,
			logger,
			debugLogging,
			endpointSet,
			reg,
			tracer,
			httpLogOpts,
			grpcLogOpts,
			logFilterMethods,
			grpcServerConfig,
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
			*deduplicationFunc,
			*queryReplicaLabels,
			*queryPartitionLabels,
			selectorLset,
			getFlagsMap(cmd.Flags()),
			*enableAutodownsampling,
			*enableQueryPartialResponse,
			*enableRulePartialResponse,
			*enableTargetPartialResponse,
			*enableMetricMetadataPartialResponse,
			*enableExemplarPartialResponse,
			*enableStatusPartialResponse,
			*activeQueryDir,
			time.Duration(*instantDefaultMaxSourceResolution),
			*defaultMetadataTimeRange,
			*webDisableCORS,
			*alertQueryURL,
			*grpcProxyStrategy,
			*queryTelemetryDurationQuantiles,
			*queryTelemetrySamplesQuantiles,
			*queryTelemetrySeriesQuantiles,
			storeRateLimits,
			*extendedFunctionsEnabled,
			store.NewTSDBSelector(tsdbSelector),
			apiv1.PromqlEngineType(*defaultEngine),
			apiv1.PromqlQueryMode(*promqlQueryMode),
			*disableQueryFallback,
			*tenantHeader,
			*defaultTenant,
			*tenantCertField,
			*enforceTenancy,
			*tenantLabel,
			*queryDistributedWithOverlappingInterval,
			*lazyRetrievalMaxBufferedResponses,
			store.DefaultResponseBatchSize,
		)
	})
}

// runQuery starts a server that exposes PromQL Query API. It is responsible for querying configured
// store nodes, merging and deduplicating the data to satisfy user query.
func runQuery(
	g *run.Group,
	logger log.Logger,
	debugLogging bool,
	endpointSet *query.EndpointSet,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	httpLogOpts []logging.Option,
	grpcLogOpts []grpc_logging.Option,
	logFilterMethods []string,
	grpcServerConfig grpcConfig,
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
	deduplicationFunc string,
	queryReplicaLabels []string,
	queryPartitionLabels []string,
	selectorLset labels.Labels,
	flagsMap map[string]string,
	enableAutodownsampling bool,
	enableQueryPartialResponse bool,
	enableRulePartialResponse bool,
	enableTargetPartialResponse bool,
	enableMetricMetadataPartialResponse bool,
	enableExemplarPartialResponse bool,
	enableStatusPartialResponse bool,
	activeQueryDir string,
	instantDefaultMaxSourceResolution time.Duration,
	defaultMetadataTimeRange time.Duration,
	disableCORS bool,
	alertQueryURL string,
	grpcProxyStrategy string,
	queryTelemetryDurationQuantiles []float64,
	queryTelemetrySamplesQuantiles []float64,
	queryTelemetrySeriesQuantiles []float64,
	storeRateLimits store.SeriesSelectLimits,
	extendedFunctionsEnabled bool,
	tsdbSelector *store.TSDBSelector,
	defaultEngine apiv1.PromqlEngineType,
	queryMode apiv1.PromqlQueryMode,
	disableQueryFallback bool,
	tenantHeader string,
	defaultTenant string,
	tenantCertField string,
	enforceTenancy bool,
	tenantLabel string,
	queryDistributedWithOverlappingInterval bool,
	lazyRetrievalMaxBufferedResponses int,
	seriesResponseBatchSize int,
) error {
	comp := component.Query
	if alertQueryURL == "" {
		lastColon := strings.LastIndex(httpBindAddr, ":")
		if lastColon != -1 {
			alertQueryURL = fmt.Sprintf("http://localhost:%s", httpBindAddr[lastColon+1:])
		}
		// NOTE(GiedriusS): default is set in config.ts.
	}

	options := []store.ProxyStoreOption{
		store.WithTSDBSelector(tsdbSelector),
		store.WithProxyStoreDebugLogging(debugLogging),
		store.WithLazyRetrievalMaxBufferedResponsesForProxy(lazyRetrievalMaxBufferedResponses),
	}

	// Parse and sanitize the provided replica labels flags.
	queryReplicaLabels = strutil.ParseFlagLabels(queryReplicaLabels)

	var (
		proxyStore       = store.NewProxyStore(logger, reg, endpointSet.GetStoreClients, component.Query, selectorLset, storeResponseTimeout, store.RetrievalStrategy(grpcProxyStrategy), options...)
		seriesProxy      = store.NewLimitedStoreServer(store.NewInstrumentedStoreServer(reg, proxyStore), reg, storeRateLimits)
		rulesProxy       = rules.NewProxy(logger, endpointSet.GetRulesClients)
		targetsProxy     = targets.NewProxy(logger, endpointSet.GetTargetsClients)
		metadataProxy    = metadata.NewProxy(logger, endpointSet.GetMetricMetadataClients)
		exemplarsProxy   = exemplars.NewProxy(logger, endpointSet.GetExemplarsStores, selectorLset)
		statusProxy      = status.NewProxy(logger, endpointSet.GetStatusClients)
		queryableCreator = query.NewQueryableCreator(
			logger,
			extprom.WrapRegistererWithPrefix("thanos_query_", reg),
			seriesProxy,
			maxConcurrentSelects,
			queryTimeout,
			deduplicationFunc,
			seriesResponseBatchSize,
		)
		remoteEndpointsCreator = query.NewRemoteEndpointsCreator(
			logger,
			endpointSet.GetQueryAPIClients,
			queryPartitionLabels,
			queryTimeout,
			queryDistributedWithOverlappingInterval,
			enableAutodownsampling,
		)
	)

	grpcProbe := prober.NewGRPC()
	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		grpcProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	// An active query tracker will be added only if the user specifies a non-default path.
	// Otherwise, the nil active query tracker from existing engine options will be used.
	var activeQueryTracker *promql.ActiveQueryTracker
	if activeQueryDir != "" {
		activeQueryTracker = promql.NewActiveQueryTracker(activeQueryDir, maxConcurrentQueries, logutil.GoKitLogToSlog(logger))
	}

	queryCreator := apiv1.NewQueryFactory(
		reg,
		logger,
		queryTimeout,
		lookbackDelta,
		defaultEvaluationInterval,
		extendedFunctionsEnabled,
		activeQueryTracker,
		queryMode,
		disableQueryFallback,
	)

	lookbackDeltaCreator := LookbackDeltaFactory(lookbackDelta, dynamicLookbackDelta)

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

		ins := extpromhttp.NewTenantInstrumentationMiddleware(tenantHeader, defaultTenant, reg, nil)
		// TODO(bplotka in PR #513 review): pass all flags, not only the flags needed by prefix rewriting.
		ui.NewQueryUI(logger, endpointSet, webExternalPrefix, webPrefixHeaderName, alertQueryURL, tenantHeader, defaultTenant, enforceTenancy).Register(router, ins)

		api := apiv1.NewQueryAPI(
			logger,
			endpointSet.GetEndpointStatus,
			queryCreator,
			apiv1.PromqlEngineType(defaultEngine),
			lookbackDeltaCreator,
			queryableCreator,
			remoteEndpointsCreator,
			// NOTE: Will share the same replica label as the query for now.
			rules.NewGRPCClientWithDedup(rulesProxy, queryReplicaLabels),
			targets.NewGRPCClientWithDedup(targetsProxy, queryReplicaLabels),
			metadata.NewGRPCClient(metadataProxy),
			exemplars.NewGRPCClientWithDedup(exemplarsProxy, queryReplicaLabels),
			status.NewGRPCClient(statusProxy),
			enableAutodownsampling,
			enableQueryPartialResponse,
			enableRulePartialResponse,
			enableTargetPartialResponse,
			enableMetricMetadataPartialResponse,
			enableExemplarPartialResponse,
			enableStatusPartialResponse,
			queryReplicaLabels,
			flagsMap,
			defaultRangeQueryStep,
			instantDefaultMaxSourceResolution,
			defaultMetadataTimeRange,
			disableCORS,
			gate.New(
				extprom.WrapRegistererWithPrefix("thanos_query_concurrent_", reg),
				maxConcurrentQueries,
				gate.Queries,
			),
			store.NewSeriesStatsAggregatorFactory(
				reg,
				queryTelemetryDurationQuantiles,
				queryTelemetrySamplesQuantiles,
				queryTelemetrySeriesQuantiles,
			),
			reg,
			tenantHeader,
			defaultTenant,
			tenantCertField,
			enforceTenancy,
			tenantLabel,
			tsdbSelector,
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
		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), grpcServerConfig.tlsSrvCert, grpcServerConfig.tlsSrvKey, grpcServerConfig.tlsSrvClientCA, grpcServerConfig.tlsMinVersion)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		infoSrv := info.NewInfoServer(
			component.Query.String(),
			info.WithLabelSetFunc(func() []labelpb.ZLabelSet { return proxyStore.LabelSet() }),
			info.WithStoreInfoFunc(func() (*infopb.StoreInfo, error) {
				if httpProbe.IsReady() {
					mint, maxt := proxyStore.TimeRange()
					return &infopb.StoreInfo{
						MinTime:                      mint,
						MaxTime:                      maxt,
						SupportsSharding:             true,
						SupportsWithoutReplicaLabels: true,
						TsdbInfos:                    proxyStore.TSDBInfos(),
					}, nil
				}
				return nil, errors.New("Not ready")
			}),
			info.WithExemplarsInfoFunc(),
			info.WithRulesInfoFunc(),
			info.WithMetricMetadataInfoFunc(),
			info.WithTargetsInfoFunc(),
			info.WithQueryAPIInfoFunc(),
			info.WithStatusInfoFunc(),
		)

		defaultEngineType := querypb.EngineType(querypb.EngineType_value[string(defaultEngine)])
		grpcAPI := apiv1.NewGRPCAPI(time.Now, queryReplicaLabels, queryableCreator, remoteEndpointsCreator, queryCreator, defaultEngineType, lookbackDeltaCreator, instantDefaultMaxSourceResolution)
		s := grpcserver.New(logger, reg, tracer, grpcLogOpts, logFilterMethods, comp, grpcProbe,
			grpcserver.WithServer(apiv1.RegisterQueryServer(grpcAPI)),
			grpcserver.WithServer(store.RegisterStoreServer(seriesProxy, logger)),
			grpcserver.WithServer(rules.RegisterRulesServer(rulesProxy)),
			grpcserver.WithServer(targets.RegisterTargetsServer(targetsProxy)),
			grpcserver.WithServer(metadata.RegisterMetadataServer(metadataProxy)),
			grpcserver.WithServer(exemplars.RegisterExemplarsServer(exemplarsProxy)),
			grpcserver.WithServer(info.RegisterInfoServer(infoSrv)),
			grpcserver.WithServer(status.RegisterStatusServer(statusProxy)),
			grpcserver.WithListen(grpcServerConfig.bindAddress),
			grpcserver.WithGracePeriod(grpcServerConfig.gracePeriod),
			grpcserver.WithMaxConnAge(grpcServerConfig.maxConnectionAge),
			grpcserver.WithTLSConfig(tlsCfg),
		)

		g.Add(func() error {
			// Wait for initial endpoint update before marking as ready
			// Use store response timeout as timeout, if not set, use 30 seconds as default
			timeout := storeResponseTimeout
			if timeout == 0 {
				timeout = 30 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			level.Info(logger).Log("msg", "waiting for initial endpoint discovery before marking gRPC as ready", "timeout", timeout)
			if err := endpointSet.WaitForFirstUpdate(ctx); err != nil {
				level.Warn(logger).Log("msg", "timeout waiting for first endpoint update before marking gRPC as ready", "err", err, "timeout", timeout)
			} else {
				level.Info(logger).Log("msg", "initial endpoint discovery completed, marking gRPC as ready")
			}

			statusProber.Ready()
			return s.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			s.Shutdown(err)
			endpointSet.Close()
		})
	}

	level.Info(logger).Log("msg", "starting query node")
	return nil
}

// LookbackDeltaFactory creates from 1 to 3 lookback deltas depending on
// dynamicLookbackDelta and eo.LookbackDelta and returns a function
// that returns appropriate lookback delta for given maxSourceResolutionMillis.
func LookbackDeltaFactory(
	lookbackDelta time.Duration,
	dynamicLookbackDelta bool,
) func(int64) time.Duration {
	resolutions := []int64{downsample.ResLevel0}
	if dynamicLookbackDelta {
		resolutions = []int64{downsample.ResLevel0, downsample.ResLevel1, downsample.ResLevel2}
	}
	var (
		lds = make([]time.Duration, len(resolutions))
		ld  = lookbackDelta.Milliseconds()
	)

	for i, r := range resolutions {
		if ld < r {
			lookbackDelta = time.Duration(r) * time.Millisecond
		}

		lds[i] = lookbackDelta
	}
	return func(maxSourceResolutionMillis int64) time.Duration {
		for i := len(resolutions) - 1; i >= 1; i-- {
			left := max(resolutions[i-1], ld)
			if left < maxSourceResolutionMillis {
				return lds[i]
			}
		}
		return lds[0]
	}
}
