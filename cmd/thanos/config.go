// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// TODO(kakkoyun): Fix linter issues - The pattern we use makes linter unhappy (returning unused config pointers).
//
//nolint:unparam
package main

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/extgrpc/snappy"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/shipper"
)

type grpcConfig struct {
	bindAddress      string
	tlsSrvCert       string
	tlsSrvKey        string
	tlsSrvClientCA   string
	tlsMinVersion    string
	gracePeriod      time.Duration
	maxConnectionAge time.Duration
}

func (gc *grpcConfig) registerFlag(cmd extkingpin.FlagClause) *grpcConfig {
	cmd.Flag("grpc-address",
		"Listen ip:port address for gRPC endpoints (StoreAPI). Make sure this address is routable from other components.").
		Default("0.0.0.0:10901").StringVar(&gc.bindAddress)
	cmd.Flag("grpc-server-tls-cert",
		"TLS Certificate for gRPC server, leave blank to disable TLS").
		Default("").StringVar(&gc.tlsSrvCert)
	cmd.Flag("grpc-server-tls-key",
		"TLS Key for the gRPC server, leave blank to disable TLS").
		Default("").StringVar(&gc.tlsSrvKey)
	cmd.Flag("grpc-server-tls-client-ca",
		"TLS CA to verify clients against. If no client CA is specified, there is no client verification on server side. (tls.NoClientCert)").
		Default("").StringVar(&gc.tlsSrvClientCA)
	cmd.Flag("grpc-server-tls-min-version",
		"TLS supported minimum version for gRPC server. If no version is specified, it'll default to 1.3. Allowed values: [\"1.0\", \"1.1\", \"1.2\", \"1.3\"]").
		Default("1.3").StringVar(&gc.tlsMinVersion)
	cmd.Flag("grpc-server-max-connection-age", "The grpc server max connection age. This controls how often to re-establish connections and redo TLS handshakes.").
		Default("60m").DurationVar(&gc.maxConnectionAge)
	cmd.Flag("grpc-grace-period",
		"Time to wait after an interrupt received for GRPC Server.").
		Default("2m").DurationVar(&gc.gracePeriod)

	return gc
}

type grpcClientConfig struct {
	secure            bool
	skipVerify        bool
	cert, key, caCert string
	serverName        string
	compression       string
	minTLSVersion     string
}

func (gc *grpcClientConfig) registerFlag(cmd extkingpin.FlagClause) *grpcClientConfig {
	cmd.Flag("grpc-client-tls-secure", "Use TLS when talking to the gRPC server").Default("false").BoolVar(&gc.secure)
	cmd.Flag("grpc-client-tls-skip-verify", "Disable TLS certificate verification i.e self signed, signed by fake CA").Default("false").BoolVar(&gc.skipVerify)
	cmd.Flag("grpc-client-tls-cert", "TLS Certificates to use to identify this client to the server").Default("").StringVar(&gc.cert)
	cmd.Flag("grpc-client-tls-key", "TLS Key for the client's certificate").Default("").StringVar(&gc.key)
	cmd.Flag("grpc-client-tls-ca", "TLS CA Certificates to use to verify gRPC servers").Default("").StringVar(&gc.caCert)
	cmd.Flag("grpc-client-server-name", "Server name to verify the hostname on the returned gRPC certificates. See https://tools.ietf.org/html/rfc4366#section-3.1").Default("").StringVar(&gc.serverName)
	compressionOptions := strings.Join([]string{snappy.Name, compressionNone}, ", ")
	cmd.Flag("grpc-compression", "Compression algorithm to use for gRPC requests to other clients. Must be one of: "+compressionOptions).Default(compressionNone).EnumVar(&gc.compression, snappy.Name, compressionNone)
	cmd.Flag("grpc-client-tls-min-version",
		"TLS supported minimum version for gRPC client. If no version is specified, it'll default to 1.3. Allowed values: [\"1.0\", \"1.1\", \"1.2\", \"1.3\"]").
		Default("1.3").StringVar(&gc.minTLSVersion)
	return gc
}

func (gc *grpcClientConfig) dialOptions(logger log.Logger, reg prometheus.Registerer, tracer opentracing.Tracer) ([]grpc.DialOption, error) {
	dialOpts, err := extgrpc.StoreClientGRPCOpts(logger, reg, tracer)
	if err != nil {
		return nil, errors.Wrapf(err, "building gRPC client")
	}
	return dialOpts, nil
}

type httpConfig struct {
	bindAddress string
	tlsConfig   string
	gracePeriod model.Duration
}

func (hc *httpConfig) registerFlag(cmd extkingpin.FlagClause) *httpConfig {
	cmd.Flag("http-address",
		"Listen host:port for HTTP endpoints.").
		Default("0.0.0.0:10902").StringVar(&hc.bindAddress)
	cmd.Flag("http-grace-period",
		"Time to wait after an interrupt received for HTTP Server.").
		Default("2m").SetValue(&hc.gracePeriod)
	cmd.Flag(
		"http.config",
		"[EXPERIMENTAL] Path to the configuration file that can enable TLS or authentication for all HTTP endpoints.",
	).Default("").StringVar(&hc.tlsConfig)
	return hc
}

type prometheusConfig struct {
	url               *url.URL
	readyTimeout      time.Duration
	getConfigInterval time.Duration
	getConfigTimeout  time.Duration
	httpClient        *extflag.PathOrContent
}

func (pc *prometheusConfig) registerFlag(cmd extkingpin.FlagClause) *prometheusConfig {
	cmd.Flag("prometheus.url",
		"URL at which to reach Prometheus's API. For better performance use local network.").
		Default("http://localhost:9090").URLVar(&pc.url)
	cmd.Flag("prometheus.ready_timeout",
		"Maximum time to wait for the Prometheus instance to start up").
		Default("10m").DurationVar(&pc.readyTimeout)
	cmd.Flag("prometheus.get_config_interval",
		"How often to get Prometheus config").
		Default("30s").DurationVar(&pc.getConfigInterval)
	cmd.Flag("prometheus.get_config_timeout",
		"Timeout for getting Prometheus config").
		Default("30s").DurationVar(&pc.getConfigTimeout)
	pc.httpClient = extflag.RegisterPathOrContent(
		cmd,
		"prometheus.http-client",
		"YAML file or string with http client configs. See Format details: https://thanos.io/tip/components/sidecar.md/#configuration.",
	)

	return pc
}

type tsdbConfig struct {
	path string
}

func (tc *tsdbConfig) registerFlag(cmd extkingpin.FlagClause) *tsdbConfig {
	cmd.Flag("tsdb.path", "Data directory of TSDB.").Default("./data").StringVar(&tc.path)
	return tc
}

type reloaderConfig struct {
	confFile        string
	envVarConfFile  string
	ruleDirectories []string
	watchInterval   time.Duration
	retryInterval   time.Duration
	method          string
	processName     string
}

const (
	// HTTPReloadMethod reloads the configuration using the HTTP reload endpoint.
	HTTPReloadMethod = "http"

	// SignalReloadMethod reloads the configuration sending a SIGHUP signal to the process.
	SignalReloadMethod = "signal"
)

func (rc *reloaderConfig) registerFlag(cmd extkingpin.FlagClause) *reloaderConfig {
	cmd.Flag("reloader.config-file",
		"Config file watched by the reloader.").
		Default("").StringVar(&rc.confFile)
	cmd.Flag("reloader.config-envsubst-file",
		"Output file for environment variable substituted config file.").
		Default("").StringVar(&rc.envVarConfFile)
	cmd.Flag("reloader.rule-dir",
		"Rule directories for the reloader to refresh (repeated field).").
		StringsVar(&rc.ruleDirectories)
	cmd.Flag("reloader.watch-interval",
		"Controls how often reloader re-reads config and rules.").
		Default("3m").DurationVar(&rc.watchInterval)
	cmd.Flag("reloader.retry-interval",
		"Controls how often reloader retries config reload in case of error.").
		Default("5s").DurationVar(&rc.retryInterval)
	cmd.Flag("reloader.method",
		"Method used to reload the configuration.").
		Default(HTTPReloadMethod).EnumVar(&rc.method, HTTPReloadMethod, SignalReloadMethod)
	cmd.Flag("reloader.process-name",
		"Executable name used to match the process being reloaded when using the signal method.").
		Default("prometheus").StringVar(&rc.processName)

	return rc
}

type shipperConfig struct {
	uploadCompacted       bool
	ignoreBlockSize       bool
	allowOutOfOrderUpload bool
	skipCorruptedBlocks   bool
	hashFunc              string
	metaFileName          string
	uploadConcurrency     int
}

func (sc *shipperConfig) registerFlag(cmd extkingpin.FlagClause) *shipperConfig {
	cmd.Flag("shipper.upload-compacted",
		"If true shipper will try to upload compacted blocks as well. Useful for migration purposes. Works only if compaction is disabled on Prometheus. Do it once and then disable the flag when done.").
		Default("false").BoolVar(&sc.uploadCompacted)
	cmd.Flag("shipper.ignore-unequal-block-size",
		"If true shipper will not require prometheus min and max block size flags to be set to the same value. Only use this if you want to keep long retention and compaction enabled on your Prometheus instance, as in the worst case it can result in ~2h data loss for your Thanos bucket storage.").
		Default("false").Hidden().BoolVar(&sc.ignoreBlockSize)
	cmd.Flag("shipper.allow-out-of-order-uploads",
		"If true, shipper will skip failed block uploads in the given iteration and retry later. This means that some newer blocks might be uploaded sooner than older blocks."+
			"This can trigger compaction without those blocks and as a result will create an overlap situation. Set it to true if you have vertical compaction enabled and wish to upload blocks as soon as possible without caring"+
			"about order.").
		Default("false").Hidden().BoolVar(&sc.allowOutOfOrderUpload)
	cmd.Flag("shipper.skip-corrupted-blocks",
		"If true, shipper will skip corrupted blocks in the given iteration and retry later. This means that some newer blocks might be uploaded sooner than older blocks."+
			"This can trigger compaction without those blocks and as a result will create an overlap situation. Set it to true if you have vertical compaction enabled and wish to upload blocks as soon as possible without caring"+
			"about order.").
		Default("false").Hidden().BoolVar(&sc.skipCorruptedBlocks)
	cmd.Flag("hash-func", "Specify which hash function to use when calculating the hashes of produced files. If no function has been specified, it does not happen. This permits avoiding downloading some files twice albeit at some performance cost. Possible values are: \"\", \"SHA256\".").
		Default("").EnumVar(&sc.hashFunc, "SHA256", "")
	cmd.Flag("shipper.meta-file-name", "the file to store shipper metadata in").Default(shipper.DefaultMetaFilename).StringVar(&sc.metaFileName)
	cmd.Flag("shipper.upload-concurrency", "Number of goroutines to use when uploading block files to object storage.").Default("0").IntVar(&sc.uploadConcurrency)
	return sc
}

type webConfig struct {
	routePrefix      string
	externalPrefix   string
	prefixHeaderName string
	disableCORS      bool
}

func (wc *webConfig) registerFlag(cmd extkingpin.FlagClause) *webConfig {
	cmd.Flag("web.route-prefix",
		"Prefix for API and UI endpoints. This allows thanos UI to be served on a sub-path. This option is analogous to --web.route-prefix of Prometheus.").
		Default("").StringVar(&wc.routePrefix)
	cmd.Flag("web.external-prefix",
		"Static prefix for all HTML links and redirect URLs in the bucket web UI interface. Actual endpoints are still served on / or the web.route-prefix. This allows thanos bucket web UI to be served behind a reverse proxy that strips a URL sub-path.").
		Default("").StringVar(&wc.externalPrefix)
	cmd.Flag("web.prefix-header", "Name of HTTP request header used for dynamic prefixing of UI links and redirects. This option is ignored if web.external-prefix argument is set. Security risk: enable this option only if a reverse proxy in front of thanos is resetting the header. The --web.prefix-header=X-Forwarded-Prefix option can be useful, for example, if Thanos UI is served via Traefik reverse proxy with PathPrefixStrip option enabled, which sends the stripped prefix value in X-Forwarded-Prefix header. This allows thanos UI to be served on a sub-path.").
		Default("").StringVar(&wc.prefixHeaderName)
	cmd.Flag("web.disable-cors", "Whether to disable CORS headers to be set by Thanos. By default Thanos sets CORS headers to be allowed by all.").Default("false").BoolVar(&wc.disableCORS)
	return wc
}

type queryConfig struct {
	addrs                []string
	sdFiles              []string
	sdInterval           time.Duration
	configPath           *extflag.PathOrContent
	dnsSDInterval        time.Duration
	httpMethod           string
	dnsSDResolver        string
	step                 time.Duration
	doNotAddThanosParams bool
}

func (qc *queryConfig) registerFlag(cmd extkingpin.FlagClause) *queryConfig {
	cmd.Flag("query", "Addresses of statically configured query API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect query API servers through respective DNS lookups.").
		PlaceHolder("<query>").StringsVar(&qc.addrs)
	qc.configPath = extflag.RegisterPathOrContent(cmd, "query.config", "YAML file that contains query API servers configuration. See format details: https://thanos.io/tip/components/rule.md/#configuration. If defined, it takes precedence over the '--query' and '--query.sd-files' flags.", extflag.WithEnvSubstitution())
	cmd.Flag("query.sd-files", "Path to file that contains addresses of query API servers. The path can be a glob pattern (repeatable).").
		PlaceHolder("<path>").StringsVar(&qc.sdFiles)
	cmd.Flag("query.sd-interval", "Refresh interval to re-read file SD files. (used as a fallback)").
		Default("5m").DurationVar(&qc.sdInterval)
	cmd.Flag("query.sd-dns-interval", "Interval between DNS resolutions.").
		Default("30s").DurationVar(&qc.dnsSDInterval)
	cmd.Flag("query.http-method", "HTTP method to use when sending queries. Possible options: [GET, POST]").
		Default("POST").EnumVar(&qc.httpMethod, "GET", "POST")
	cmd.Flag("query.sd-dns-resolver", "Resolver to use. Possible options: [golang, miekgdns]").
		Default("miekgdns").Hidden().StringVar(&qc.dnsSDResolver)
	cmd.Flag("query.default-step", "Default range query step to use. This is only used in stateless Ruler and alert state restoration.").
		Default("1s").DurationVar(&qc.step)
	cmd.Flag("query.only-prometheus-params", "Disable adding Thanos parameters (e.g dedup, partial_response) when querying metrics. Some non-Thanos systems have strict API validation.").Hidden().
		Default("false").BoolVar(&qc.doNotAddThanosParams)
	return qc
}

type alertMgrConfig struct {
	configPath             *extflag.PathOrContent
	alertmgrURLs           []string
	alertmgrsTimeout       time.Duration
	alertmgrsDNSSDInterval time.Duration
	alertExcludeLabels     []string
	alertQueryURL          *string
	alertRelabelConfigPath *extflag.PathOrContent
	alertSourceTemplate    *string
}

func (ac *alertMgrConfig) registerFlag(cmd extflag.FlagClause) *alertMgrConfig {
	ac.configPath = extflag.RegisterPathOrContent(cmd, "alertmanagers.config", "YAML file that contains alerting configuration. See format details: https://thanos.io/tip/components/rule.md/#configuration. If defined, it takes precedence over the '--alertmanagers.url' and '--alertmanagers.send-timeout' flags.", extflag.WithEnvSubstitution())
	cmd.Flag("alertmanagers.url", "Alertmanager replica URLs to push firing alerts. Ruler claims success if push to at least one alertmanager from discovered succeeds. The scheme should not be empty e.g `http` might be used. The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect Alertmanager IPs through respective DNS lookups. The port defaults to 9093 or the SRV record's value. The URL path is used as a prefix for the regular Alertmanager API path.").
		StringsVar(&ac.alertmgrURLs)
	cmd.Flag("alertmanagers.send-timeout", "Timeout for sending alerts to Alertmanager").Default("10s").
		DurationVar(&ac.alertmgrsTimeout)
	cmd.Flag("alertmanagers.sd-dns-interval", "Interval between DNS resolutions of Alertmanager hosts.").
		Default("30s").DurationVar(&ac.alertmgrsDNSSDInterval)
	ac.alertQueryURL = cmd.Flag("alert.query-url", "The external Thanos Query URL that would be set in all alerts 'Source' field").String()
	cmd.Flag("alert.label-drop", "Labels by name to drop before sending to alertmanager. This allows alert to be deduplicated on replica label (repeated). Similar Prometheus alert relabelling").
		StringsVar(&ac.alertExcludeLabels)
	ac.alertRelabelConfigPath = extflag.RegisterPathOrContent(cmd, "alert.relabel-config", "YAML file that contains alert relabelling configuration.", extflag.WithEnvSubstitution())
	ac.alertSourceTemplate = cmd.Flag("alert.query-template", "Template to use in alerts source field. Need only include {{.Expr}} parameter").Default("/graph?g0.expr={{.Expr}}&g0.tab=1").String()

	return ac
}

func parseFlagLabels(s []string) (labels.Labels, error) {
	var lset labels.ScratchBuilder
	for _, l := range s {
		parts := strings.SplitN(l, "=", 2)
		if len(parts) != 2 {
			return labels.EmptyLabels(), errors.Errorf("unrecognized label %q", l)
		}
		if !model.UTF8Validation.IsValidLabelName(parts[0]) {
			return labels.EmptyLabels(), errors.Errorf("unsupported format for label %s", l)
		}
		val, err := strconv.Unquote(parts[1])
		if err != nil {
			return labels.EmptyLabels(), errors.Wrap(err, "unquote label value")
		}
		lset.Add(parts[0], val)
	}
	lset.Sort()
	return lset.Labels(), nil
}

type goMemLimitConfig struct {
	enableAutoGoMemlimit bool
	memlimitRatio        float64
}

func (gml *goMemLimitConfig) registerFlag(cmd extkingpin.FlagClause) *goMemLimitConfig {
	cmd.Flag("enable-auto-gomemlimit",
		"Enable go runtime to automatically limit memory consumption.").
		Default("false").BoolVar(&gml.enableAutoGoMemlimit)

	cmd.Flag("auto-gomemlimit.ratio",
		"The ratio of reserved GOMEMLIMIT memory to the detected maximum container or system memory.").
		Default("0.9").FloatVar(&gml.memlimitRatio)

	return gml
}

func configureGoAutoMemLimit(common goMemLimitConfig) (int64, error) {
	var (
		err    error
		limits int64 = -1
	)

	if common.memlimitRatio <= 0.0 || common.memlimitRatio > 1.0 {
		return limits, errors.New("--auto-gomemlimit.ratio must be greater than 0 and less than or equal to 1.")
	}

	if common.enableAutoGoMemlimit {
		limits, err = memlimit.SetGoMemLimitWithOpts(
			memlimit.WithRatio(common.memlimitRatio),
			memlimit.WithProvider(
				memlimit.ApplyFallback(
					memlimit.FromCgroup,
					memlimit.FromSystem,
				),
			),
		)
		if err != nil {
			return -1, errors.Wrap(err, "Failed to set GOMEMLIMIT automatically")
		}
	}

	return limits, nil
}
