// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tags"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/discovery/file"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/thanos-io/promql-engine/api"

	apiv1 "github.com/thanos-io/thanos/pkg/api/query"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/discovery/cache"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/exemplars"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/extgrpc/snappy"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/info"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/metadata"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/rules"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/targets"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/ui"
)

const (
	promqlNegativeOffset = "promql-negative-offset"
	promqlAtModifier     = "promql-at-modifier"
	queryPushdown        = "query-pushdown"
)

type queryMode string

const (
	queryModeLocal       queryMode = "local"
	queryModeDistributed queryMode = "distributed"
)

// registerQuery registers a query command.
func registerQuery(app *extkingpin.App) {
	comp := component.Query
	cmd := app.Command(comp.String(), "Query node exposing PromQL enabled Query API with data retrieved from multiple store nodes.")

	httpBindAddr, httpGracePeriod, httpTLSConfig := extkingpin.RegisterHTTPFlags(cmd)

	var grpcServerConfig grpcConfig
	grpcServerConfig.registerFlag(cmd)

	secure := cmd.Flag("grpc-client-tls-secure", "Use TLS when talking to the gRPC server").Default("false").Bool()
	skipVerify := cmd.Flag("grpc-client-tls-skip-verify", "Disable TLS certificate verification i.e self signed, signed by fake CA").Default("false").Bool()
	cert := cmd.Flag("grpc-client-tls-cert", "TLS Certificates to use to identify this client to the server").Default("").String()
	key := cmd.Flag("grpc-client-tls-key", "TLS Key for the client's certificate").Default("").String()
	caCert := cmd.Flag("grpc-client-tls-ca", "TLS CA Certificates to use to verify gRPC servers").Default("").String()
	serverName := cmd.Flag("grpc-client-server-name", "Server name to verify the hostname on the returned gRPC certificates. See https://tools.ietf.org/html/rfc4366#section-3.1").Default("").String()
	compressionOptions := strings.Join([]string{snappy.Name, compressionNone}, ", ")
	grpcCompression := cmd.Flag("grpc-compression", "Compression algorithm to use for gRPC requests to other clients. Must be one of: "+compressionOptions).Default(compressionNone).Enum(snappy.Name, compressionNone)

	webRoutePrefix := cmd.Flag("web.route-prefix", "Prefix for API and UI endpoints. This allows thanos UI to be served on a sub-path. Defaults to the value of --web.external-prefix. This option is analogous to --web.route-prefix of Prometheus.").Default("").String()
	webExternalPrefix := cmd.Flag("web.external-prefix", "Static prefix for all HTML links and redirect URLs in the UI query web interface. Actual endpoints are still served on / or the web.route-prefix. This allows thanos UI to be served behind a reverse proxy that strips a URL sub-path.").Default("").String()
	webPrefixHeaderName := cmd.Flag("web.prefix-header", "Name of HTTP request header used for dynamic prefixing of UI links and redirects. This option is ignored if web.external-prefix argument is set. Security risk: enable this option only if a reverse proxy in front of thanos is resetting the header. The --web.prefix-header=X-Forwarded-Prefix option can be useful, for example, if Thanos UI is served via Traefik reverse proxy with PathPrefixStrip option enabled, which sends the stripped prefix value in X-Forwarded-Prefix header. This allows thanos UI to be served on a sub-path.").Default("").String()
	webDisableCORS := cmd.Flag("web.disable-cors", "Whether to disable CORS headers to be set by Thanos. By default Thanos sets CORS headers to be allowed by all.").Default("false").Bool()

	queryTimeout := extkingpin.ModelDuration(cmd.Flag("query.timeout", "Maximum time to process query by query node.").
		Default("2m"))

	defaultEngine := cmd.Flag("query.promql-engine", "Default PromQL engine to use.").Default(string(apiv1.PromqlEnginePrometheus)).
		Enum(string(apiv1.PromqlEnginePrometheus), string(apiv1.PromqlEngineThanos))

	promqlQueryMode := cmd.Flag("query.mode", "PromQL query mode. One of: local, distributed.").
		Hidden().
		Default(string(queryModeLocal)).
		Enum(string(queryModeLocal), string(queryModeDistributed))

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "Maximum number of queries processed concurrently by query node.").
		Default("20").Int()

	lookbackDelta := cmd.Flag("query.lookback-delta", "The maximum lookback duration for retrieving metrics during expression evaluations. PromQL always evaluates the query for the certain timestamp (query range timestamps are deduced by step). Since scrape intervals might be different, PromQL looks back for given amount of time to get latest sample. If it exceeds the maximum lookback delta it assumes series is stale and returns none (a gap). This is why lookback delta should be set to at least 2 times of the slowest scrape interval. If unset it will use the promql default of 5m.").Duration()
	dynamicLookbackDelta := cmd.Flag("query.dynamic-lookback-delta", "Allow for larger lookback duration for queries based on resolution.").Hidden().Default("true").Bool()

	maxConcurrentSelects := cmd.Flag("query.max-concurrent-select", "Maximum number of select requests made concurrently per a query.").
		Default("4").Int()

	queryConnMetricLabels := cmd.Flag("query.conn-metric.label", "Optional selection of query connection metric labels to be collected from endpoint set").
		Default(string(query.ExternalLabels), string(query.StoreType)).
		Enums(string(query.ExternalLabels), string(query.StoreType))

	queryReplicaLabels := cmd.Flag("query.replica-label", "Labels to treat as a replica indicator along which data is deduplicated. Still you will be able to query without deduplication using 'dedup=false' parameter. Data includes time series, recording rules, and alerting rules.").
		Strings()

	instantDefaultMaxSourceResolution := extkingpin.ModelDuration(cmd.Flag("query.instant.default.max_source_resolution", "default value for max_source_resolution for instant queries. If not set, defaults to 0s only taking raw resolution into account. 1h can be a good value if you use instant queries over time ranges that incorporate times outside of your raw-retention.").Default("0s").Hidden())

	defaultMetadataTimeRange := cmd.Flag("query.metadata.default-time-range", "The default metadata time range duration for retrieving labels through Labels and Series API when the range parameters are not specified. The zero value means range covers the time since the beginning.").Default("0s").Duration()

	selectorLabels := cmd.Flag("selector-label", "Query selector labels that will be exposed in info endpoint (repeated).").
		PlaceHolder("<name>=\"<value>\"").Strings()

	endpoints := extkingpin.Addrs(cmd.Flag("endpoint", "Addresses of statically configured Thanos API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect Thanos API servers through respective DNS lookups.").
		PlaceHolder("<endpoint>"))

	endpointGroups := extkingpin.Addrs(cmd.Flag("endpoint-group", "Experimental: DNS name of statically configured Thanos API server groups (repeatable). Targets resolved from the DNS name will be queried in a round-robin, instead of a fanout manner. This flag should be used when connecting a Thanos Query to HA groups of Thanos components.").
		PlaceHolder("<endpoint-group>"))

	stores := extkingpin.Addrs(cmd.Flag("store", "Deprecation Warning - This flag is deprecated and replaced with `endpoint`. Addresses of statically configured store API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect store API servers through respective DNS lookups.").
		PlaceHolder("<store>"))

	// TODO(bwplotka): Hidden because we plan to extract discovery to separate API: https://github.com/thanos-io/thanos/issues/2600.
	ruleEndpoints := extkingpin.Addrs(cmd.Flag("rule", "Deprecation Warning - This flag is deprecated and replaced with `endpoint`. Experimental: Addresses of statically configured rules API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect rule API servers through respective DNS lookups.").
		Hidden().PlaceHolder("<rule>"))

	metadataEndpoints := extkingpin.Addrs(cmd.Flag("metadata", "Deprecation Warning - This flag is deprecated and replaced with `endpoint`. Experimental: Addresses of statically configured metadata API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect metadata API servers through respective DNS lookups.").
		Hidden().PlaceHolder("<metadata>"))

	exemplarEndpoints := extkingpin.Addrs(cmd.Flag("exemplar", "Deprecation Warning - This flag is deprecated and replaced with `endpoint`. Experimental: Addresses of statically configured exemplars API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect exemplars API servers through respective DNS lookups.").
		Hidden().PlaceHolder("<exemplar>"))

	// TODO(atunik): Hidden because we plan to extract discovery to separate API: https://github.com/thanos-io/thanos/issues/2600.
	targetEndpoints := extkingpin.Addrs(cmd.Flag("target", "Deprecation Warning - This flag is deprecated and replaced with `endpoint`. Experimental: Addresses of statically configured target API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect target API servers through respective DNS lookups.").
		Hidden().PlaceHolder("<target>"))

	strictStores := cmd.Flag("store-strict", "Deprecation Warning - This flag is deprecated and replaced with `endpoint-strict`. Addresses of only statically configured store API servers that are always used, even if the health check fails. Useful if you have a caching layer on top.").
		PlaceHolder("<staticstore>").Strings()

	strictEndpoints := cmd.Flag("endpoint-strict", "Addresses of only statically configured Thanos API servers that are always used, even if the health check fails. Useful if you have a caching layer on top.").
		PlaceHolder("<staticendpoint>").Strings()

	strictEndpointGroups := extkingpin.Addrs(cmd.Flag("endpoint-group-strict", "Experimental: DNS name of statically configured Thanos API server groups (repeatable) that are always used, even if the health check fails.").
		PlaceHolder("<endpoint-group-strict>"))

	fileSDFiles := cmd.Flag("store.sd-files", "Path to files that contain addresses of store API servers. The path can be a glob pattern (repeatable).").
		PlaceHolder("<path>").Strings()

	fileSDInterval := extkingpin.ModelDuration(cmd.Flag("store.sd-interval", "Refresh interval to re-read file SD files. It is used as a resync fallback.").
		Default("5m"))

	// TODO(bwplotka): Grab this from TTL at some point.
	dnsSDInterval := extkingpin.ModelDuration(cmd.Flag("store.sd-dns-interval", "Interval between DNS resolutions.").
		Default("30s"))

	dnsSDResolver := cmd.Flag("store.sd-dns-resolver", fmt.Sprintf("Resolver to use. Possible options: [%s, %s]", dns.GolangResolverType, dns.MiekgdnsResolverType)).
		Default(string(dns.MiekgdnsResolverType)).Hidden().String()

	unhealthyStoreTimeout := extkingpin.ModelDuration(cmd.Flag("store.unhealthy-timeout", "Timeout before an unhealthy store is cleaned from the store UI page.").Default("5m"))

	endpointInfoTimeout := extkingpin.ModelDuration(cmd.Flag("endpoint.info-timeout", "Timeout of gRPC Info requests.").Default("5s").Hidden())

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

	featureList := cmd.Flag("enable-feature", "Comma separated experimental feature names to enable.The current list of features is "+queryPushdown+".").Default("").Strings()

	enableExemplarPartialResponse := cmd.Flag("exemplar.partial-response", "Enable partial response for exemplar endpoint. --no-exemplar.partial-response for disabling.").
		Hidden().Default("true").Bool()

	defaultEvaluationInterval := extkingpin.ModelDuration(cmd.Flag("query.default-evaluation-interval", "Set default evaluation interval for sub queries.").Default("1m"))

	defaultRangeQueryStep := extkingpin.ModelDuration(cmd.Flag("query.default-step", "Set default step for range queries. Default step is only used when step is not set in UI. In such cases, Thanos UI will use default step to calculate resolution (resolution = max(rangeSeconds / 250, defaultStep)). This will not work from Grafana, but Grafana has __step variable which can be used.").
		Default("1s"))

	storeResponseTimeout := extkingpin.ModelDuration(cmd.Flag("store.response-timeout", "If a Store doesn't send any data in this specified duration then a Store will be ignored and partial data will be returned if it's enabled. 0 disables timeout.").Default("0ms"))
	reqLogConfig := extkingpin.RegisterRequestLoggingFlags(cmd)

	alertQueryURL := cmd.Flag("alert.query-url", "The external Thanos Query URL that would be set in all alerts 'Source' field.").String()
	grpcProxyStrategy := cmd.Flag("grpc.proxy-strategy", "Strategy to use when proxying Series requests to leaf nodes. Hidden and only used for testing, will be removed after lazy becomes the default.").Default(string(store.EagerRetrieval)).Hidden().Enum(string(store.EagerRetrieval), string(store.LazyRetrieval))

	queryTelemetryDurationQuantiles := cmd.Flag("query.telemetry.request-duration-seconds-quantiles", "The quantiles for exporting metrics about the request duration quantiles.").Default("0.1", "0.25", "0.75", "1.25", "1.75", "2.5", "3", "5", "10").Float64List()
	queryTelemetrySamplesQuantiles := cmd.Flag("query.telemetry.request-samples-quantiles", "The quantiles for exporting metrics about the samples count quantiles.").Default("100", "1000", "10000", "100000", "1000000").Float64List()
	queryTelemetrySeriesQuantiles := cmd.Flag("query.telemetry.request-series-seconds-quantiles", "The quantiles for exporting metrics about the series count quantiles.").Default("10", "100", "1000", "10000", "100000").Float64List()

	tenantHeader := cmd.Flag("query.tenant-header", "HTTP header to determine tenant.").Default(tenancy.DefaultTenantHeader).Hidden().String()
	defaultTenant := cmd.Flag("query.default-tenant", "Name of the default tenant.").Default(tenancy.DefaultTenant).Hidden().String()
	tenantCertField := cmd.Flag("query.tenant-certificate-field", "Use TLS client's certificate field to determine tenant for write requests. Must be one of "+tenancy.CertificateFieldOrganization+", "+tenancy.CertificateFieldOrganizationalUnit+" or "+tenancy.CertificateFieldCommonName+". This setting will cause the query.tenant-header flag value to be ignored.").Default("").Hidden().Enum("", tenancy.CertificateFieldOrganization, tenancy.CertificateFieldOrganizationalUnit, tenancy.CertificateFieldCommonName)

	var storeRateLimits store.SeriesSelectLimits
	storeRateLimits.RegisterFlags(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, debugLogging bool) error {
		selectorLset, err := parseFlagLabels(*selectorLabels)
		if err != nil {
			return errors.Wrap(err, "parse federation labels")
		}

		var enableQueryPushdown bool
		for _, feature := range *featureList {
			if feature == queryPushdown {
				enableQueryPushdown = true
			}
			if feature == promqlAtModifier {
				level.Warn(logger).Log("msg", "This option for --enable-feature is now permanently enabled and therefore a no-op.", "option", promqlAtModifier)
			}
			if feature == promqlNegativeOffset {
				level.Warn(logger).Log("msg", "This option for --enable-feature is now permanently enabled and therefore a no-op.", "option", promqlNegativeOffset)
			}
		}

		httpLogOpts, err := logging.ParseHTTPOptions(reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		tagOpts, grpcLogOpts, err := logging.ParsegRPCOptions(reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		var fileSD *file.Discovery
		if len(*fileSDFiles) > 0 {
			conf := &file.SDConfig{
				Files:           *fileSDFiles,
				RefreshInterval: *fileSDInterval,
			}
			fileSD = file.NewDiscovery(conf, logger)
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
			debugLogging,
			reg,
			tracer,
			httpLogOpts,
			grpcLogOpts,
			tagOpts,
			grpcServerConfig,
			*grpcCompression,
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
			*queryConnMetricLabels,
			*queryReplicaLabels,
			selectorLset,
			getFlagsMap(cmd.Flags()),
			*endpoints,
			*endpointGroups,
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
			*activeQueryDir,
			fileSD,
			time.Duration(*dnsSDInterval),
			*dnsSDResolver,
			time.Duration(*unhealthyStoreTimeout),
			time.Duration(*endpointInfoTimeout),
			time.Duration(*instantDefaultMaxSourceResolution),
			*defaultMetadataTimeRange,
			*strictStores,
			*strictEndpoints,
			*strictEndpointGroups,
			*webDisableCORS,
			enableQueryPushdown,
			*alertQueryURL,
			*grpcProxyStrategy,
			component.Query,
			*queryTelemetryDurationQuantiles,
			*queryTelemetrySamplesQuantiles,
			*queryTelemetrySeriesQuantiles,
			*defaultEngine,
			storeRateLimits,
			queryMode(*promqlQueryMode),
			*tenantHeader,
			*defaultTenant,
			*tenantCertField,
		)
	})
}

// runQuery starts a server that exposes PromQL Query API. It is responsible for querying configured
// store nodes, merging and duplicating the data to satisfy user query.
func runQuery(
	g *run.Group,
	logger log.Logger,
	debugLogging bool,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	httpLogOpts []logging.Option,
	grpcLogOpts []grpc_logging.Option,
	tagOpts []tags.Option,
	grpcServerConfig grpcConfig,
	grpcCompression string,
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
	queryConnMetricLabels []string,
	queryReplicaLabels []string,
	selectorLset labels.Labels,
	flagsMap map[string]string,
	endpointAddrs []string,
	endpointGroupAddrs []string,
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
	activeQueryDir string,
	fileSD *file.Discovery,
	dnsSDInterval time.Duration,
	dnsSDResolver string,
	unhealthyStoreTimeout time.Duration,
	endpointInfoTimeout time.Duration,
	instantDefaultMaxSourceResolution time.Duration,
	defaultMetadataTimeRange time.Duration,
	strictStores []string,
	strictEndpoints []string,
	strictEndpointGroups []string,
	disableCORS bool,
	enableQueryPushdown bool,
	alertQueryURL string,
	grpcProxyStrategy string,
	comp component.Component,
	queryTelemetryDurationQuantiles []float64,
	queryTelemetrySamplesQuantiles []float64,
	queryTelemetrySeriesQuantiles []float64,
	defaultEngine string,
	storeRateLimits store.SeriesSelectLimits,
	queryMode queryMode,
	tenantHeader string,
	defaultTenant string,
	tenantCertField string,
) error {
	if alertQueryURL == "" {
		lastColon := strings.LastIndex(httpBindAddr, ":")
		if lastColon != -1 {
			alertQueryURL = fmt.Sprintf("http://localhost:%s", httpBindAddr[lastColon+1:])
		}
		// NOTE(GiedriusS): default is set in config.ts.
	}
	// TODO(bplotka in PR #513 review): Move arguments into struct.
	duplicatedStores := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_query_duplicated_store_addresses_total",
		Help: "The number of times a duplicated store addresses is detected from the different configs in query",
	})

	dialOpts, err := extgrpc.StoreClientGRPCOpts(logger, reg, tracer, secure, skipVerify, cert, key, caCert, serverName)
	if err != nil {
		return errors.Wrap(err, "building gRPC client")
	}
	if grpcCompression != compressionNone {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(grpcCompression)))
	}

	fileSDCache := cache.New()
	dnsStoreProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_query_store_apis_", reg),
		dns.ResolverType(dnsSDResolver),
	)

	for _, store := range strictStores {
		if dns.IsDynamicNode(store) {
			return errors.Errorf("%s is a dynamically specified store i.e. it uses SD and that is not permitted under strict mode. Use --store for this", store)
		}
	}

	for _, endpoint := range strictEndpoints {
		if dns.IsDynamicNode(endpoint) {
			return errors.Errorf("%s is a dynamically specified endpoint i.e. it uses SD and that is not permitted under strict mode. Use --endpoint for this", endpoint)
		}
	}

	dnsEndpointProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_query_endpoints_", reg),
		dns.ResolverType(dnsSDResolver),
	)

	dnsRuleProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_query_rule_apis_", reg),
		dns.ResolverType(dnsSDResolver),
	)

	dnsTargetProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_query_target_apis_", reg),
		dns.ResolverType(dnsSDResolver),
	)

	dnsMetadataProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_query_metadata_apis_", reg),
		dns.ResolverType(dnsSDResolver),
	)

	dnsExemplarProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_query_exemplar_apis_", reg),
		dns.ResolverType(dnsSDResolver),
	)

	options := []store.ProxyStoreOption{}
	if debugLogging {
		options = append(options, store.WithProxyStoreDebugLogging())
	}

	var (
		endpoints = query.NewEndpointSet(
			time.Now,
			logger,
			reg,
			func() (specs []*query.GRPCEndpointSpec) {
				// Add strict & static nodes.
				for _, addr := range strictStores {
					specs = append(specs, query.NewGRPCEndpointSpec(addr, true))
				}

				for _, addr := range strictEndpoints {
					specs = append(specs, query.NewGRPCEndpointSpec(addr, true))
				}

				for _, dnsProvider := range []*dns.Provider{
					dnsStoreProvider,
					dnsRuleProvider,
					dnsExemplarProvider,
					dnsMetadataProvider,
					dnsTargetProvider,
					dnsEndpointProvider,
				} {
					var tmpSpecs []*query.GRPCEndpointSpec

					for _, addr := range dnsProvider.Addresses() {
						tmpSpecs = append(tmpSpecs, query.NewGRPCEndpointSpec(addr, false))
					}
					tmpSpecs = removeDuplicateEndpointSpecs(logger, duplicatedStores, tmpSpecs)
					specs = append(specs, tmpSpecs...)
				}

				for _, eg := range endpointGroupAddrs {
					addr := fmt.Sprintf("dns:///%s", eg)
					spec := query.NewGRPCEndpointSpec(addr, false, extgrpc.EndpointGroupGRPCOpts()...)
					specs = append(specs, spec)
				}

				for _, eg := range strictEndpointGroups {
					addr := fmt.Sprintf("dns:///%s", eg)
					spec := query.NewGRPCEndpointSpec(addr, true, extgrpc.EndpointGroupGRPCOpts()...)
					specs = append(specs, spec)
				}

				return specs
			},
			dialOpts,
			unhealthyStoreTimeout,
			endpointInfoTimeout,
			queryConnMetricLabels...,
		)
		proxy            = store.NewProxyStore(logger, reg, endpoints.GetStoreClients, component.Query, selectorLset, storeResponseTimeout, store.RetrievalStrategy(grpcProxyStrategy), options...)
		rulesProxy       = rules.NewProxy(logger, endpoints.GetRulesClients)
		targetsProxy     = targets.NewProxy(logger, endpoints.GetTargetsClients)
		metadataProxy    = metadata.NewProxy(logger, endpoints.GetMetricMetadataClients)
		exemplarsProxy   = exemplars.NewProxy(logger, endpoints.GetExemplarsStores, selectorLset)
		queryableCreator = query.NewQueryableCreator(
			logger,
			extprom.WrapRegistererWithPrefix("thanos_query_", reg),
			proxy,
			maxConcurrentSelects,
			queryTimeout,
		)
	)

	// Periodically update the store set with the addresses we see in our cluster.
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return runutil.Repeat(5*time.Second, ctx.Done(), func() error {
				endpoints.Update(ctx)
				return nil
			})
		}, func(error) {
			cancel()
			endpoints.Close()
		})
	}

	// Run File Service Discovery and update the store set when the files are modified.
	if fileSD != nil {
		var fileSDUpdates chan []*targetgroup.Group
		ctxRun, cancelRun := context.WithCancel(context.Background())

		fileSDUpdates = make(chan []*targetgroup.Group)

		g.Add(func() error {
			fileSD.Run(ctxRun, fileSDUpdates)
			return nil
		}, func(error) {
			cancelRun()
		})

		ctxUpdate, cancelUpdate := context.WithCancel(context.Background())
		g.Add(func() error {
			for {
				select {
				case update := <-fileSDUpdates:
					// Discoverers sometimes send nil updates so need to check for it to avoid panics.
					if update == nil {
						continue
					}
					fileSDCache.Update(update)
					endpoints.Update(ctxUpdate)

					if err := dnsStoreProvider.Resolve(ctxUpdate, append(fileSDCache.Addresses(), storeAddrs...)); err != nil {
						level.Error(logger).Log("msg", "failed to resolve addresses for storeAPIs", "err", err)
					}

					// Rules apis do not support file service discovery as of now.
				case <-ctxUpdate.Done():
					return nil
				}
			}
		}, func(error) {
			cancelUpdate()
		})
	}
	// Periodically update the addresses from static flags and file SD by resolving them using DNS SD if necessary.
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return runutil.Repeat(dnsSDInterval, ctx.Done(), func() error {
				resolveCtx, resolveCancel := context.WithTimeout(ctx, dnsSDInterval)
				defer resolveCancel()
				if err := dnsStoreProvider.Resolve(resolveCtx, append(fileSDCache.Addresses(), storeAddrs...)); err != nil {
					level.Error(logger).Log("msg", "failed to resolve addresses for storeAPIs", "err", err)
				}
				if err := dnsRuleProvider.Resolve(resolveCtx, ruleAddrs); err != nil {
					level.Error(logger).Log("msg", "failed to resolve addresses for rulesAPIs", "err", err)
				}
				if err := dnsTargetProvider.Resolve(ctx, targetAddrs); err != nil {
					level.Error(logger).Log("msg", "failed to resolve addresses for targetsAPIs", "err", err)
				}
				if err := dnsMetadataProvider.Resolve(resolveCtx, metadataAddrs); err != nil {
					level.Error(logger).Log("msg", "failed to resolve addresses for metadataAPIs", "err", err)
				}
				if err := dnsExemplarProvider.Resolve(resolveCtx, exemplarAddrs); err != nil {
					level.Error(logger).Log("msg", "failed to resolve addresses for exemplarsAPI", "err", err)
				}
				if err := dnsEndpointProvider.Resolve(resolveCtx, endpointAddrs); err != nil {
					level.Error(logger).Log("msg", "failed to resolve addresses passed using endpoint flag", "err", err)

				}
				return nil
			})
		}, func(error) {
			cancel()
		})
	}

	grpcProbe := prober.NewGRPC()
	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		grpcProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
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
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	}

	// An active query tracker will be added only if the user specifies a non-default path.
	// Otherwise, the nil active query tracker from existing engine options will be used.
	if activeQueryDir != "" {
		engineOpts.ActiveQueryTracker = promql.NewActiveQueryTracker(activeQueryDir, maxConcurrentQueries, logger)
	}

	var remoteEngineEndpoints api.RemoteEndpoints
	if queryMode != queryModeLocal {
		remoteEngineEndpoints = query.NewRemoteEndpoints(logger, endpoints.GetQueryAPIClients, query.Opts{
			AutoDownsample:        enableAutodownsampling,
			ReplicaLabels:         queryReplicaLabels,
			Timeout:               queryTimeout,
			EnablePartialResponse: enableQueryPartialResponse,
		})
	}

	engineFactory := apiv1.NewQueryEngineFactory(
		engineOpts,
		remoteEngineEndpoints,
	)

	lookbackDeltaCreator := LookbackDeltaFactory(engineOpts, dynamicLookbackDelta)

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
		ui.NewQueryUI(logger, endpoints, webExternalPrefix, webPrefixHeaderName, alertQueryURL).Register(router, ins)

		api := apiv1.NewQueryAPI(
			logger,
			endpoints.GetEndpointStatus,
			engineFactory,
			apiv1.PromqlEngineType(defaultEngine),
			lookbackDeltaCreator,
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
			enableQueryPushdown,
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
		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), grpcServerConfig.tlsSrvCert, grpcServerConfig.tlsSrvKey, grpcServerConfig.tlsSrvClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		infoSrv := info.NewInfoServer(
			component.Query.String(),
			info.WithLabelSetFunc(func() []labelpb.ZLabelSet { return proxy.LabelSet() }),
			info.WithStoreInfoFunc(func() *infopb.StoreInfo {
				if httpProbe.IsReady() {
					mint, maxt := proxy.TimeRange()
					return &infopb.StoreInfo{
						MinTime:                      mint,
						MaxTime:                      maxt,
						SupportsSharding:             true,
						SupportsWithoutReplicaLabels: true,
						TsdbInfos:                    proxy.TSDBInfos(),
					}
				}
				return nil
			}),
			info.WithExemplarsInfoFunc(),
			info.WithRulesInfoFunc(),
			info.WithMetricMetadataInfoFunc(),
			info.WithTargetsInfoFunc(),
			info.WithQueryAPIInfoFunc(),
		)

		defaultEngineType := querypb.EngineType(querypb.EngineType_value[defaultEngine])
		grpcAPI := apiv1.NewGRPCAPI(time.Now, queryReplicaLabels, queryableCreator, engineFactory, defaultEngineType, lookbackDeltaCreator, instantDefaultMaxSourceResolution)
		storeServer := store.NewLimitedStoreServer(store.NewInstrumentedStoreServer(reg, proxy), reg, storeRateLimits)
		s := grpcserver.New(logger, reg, tracer, grpcLogOpts, tagOpts, comp, grpcProbe,
			grpcserver.WithServer(apiv1.RegisterQueryServer(grpcAPI)),
			grpcserver.WithServer(store.RegisterStoreServer(storeServer, logger)),
			grpcserver.WithServer(rules.RegisterRulesServer(rulesProxy)),
			grpcserver.WithServer(targets.RegisterTargetsServer(targetsProxy)),
			grpcserver.WithServer(metadata.RegisterMetadataServer(metadataProxy)),
			grpcserver.WithServer(exemplars.RegisterExemplarsServer(exemplarsProxy)),
			grpcserver.WithServer(info.RegisterInfoServer(infoSrv)),
			grpcserver.WithListen(grpcServerConfig.bindAddress),
			grpcserver.WithGracePeriod(grpcServerConfig.gracePeriod),
			grpcserver.WithMaxConnAge(grpcServerConfig.maxConnectionAge),
			grpcserver.WithTLSConfig(tlsCfg),
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

func removeDuplicateEndpointSpecs(logger log.Logger, duplicatedStores prometheus.Counter, specs []*query.GRPCEndpointSpec) []*query.GRPCEndpointSpec {
	set := make(map[string]*query.GRPCEndpointSpec)
	for _, spec := range specs {
		addr := spec.Addr()
		if _, ok := set[addr]; ok {
			level.Warn(logger).Log("msg", "Duplicate store address is provided", "addr", addr)
			duplicatedStores.Inc()
		}
		set[addr] = spec
	}
	deduplicated := make([]*query.GRPCEndpointSpec, 0, len(set))
	for _, value := range set {
		deduplicated = append(deduplicated, value)
	}
	return deduplicated
}

// LookbackDeltaFactory creates from 1 to 3 lookback deltas depending on
// dynamicLookbackDelta and eo.LookbackDelta and returns a function
// that returns appropriate lookback delta for given maxSourceResolutionMillis.
func LookbackDeltaFactory(
	eo promql.EngineOpts,
	dynamicLookbackDelta bool,
) func(int64) time.Duration {
	resolutions := []int64{downsample.ResLevel0}
	if dynamicLookbackDelta {
		resolutions = []int64{downsample.ResLevel0, downsample.ResLevel1, downsample.ResLevel2}
	}
	var (
		lds = make([]time.Duration, len(resolutions))
		ld  = eo.LookbackDelta.Milliseconds()
	)

	lookbackDelta := eo.LookbackDelta
	for i, r := range resolutions {
		if ld < r {
			lookbackDelta = time.Duration(r) * time.Millisecond
		}

		lds[i] = lookbackDelta
	}
	return func(maxSourceResolutionMillis int64) time.Duration {
		for i := len(resolutions) - 1; i >= 1; i-- {
			left := resolutions[i-1]
			if resolutions[i-1] < ld {
				left = ld
			}
			if left < maxSourceResolutionMillis {
				return lds[i]
			}
		}
		return lds[0]
	}
}
