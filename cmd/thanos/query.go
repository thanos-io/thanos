// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/discovery/file"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/discovery/cache"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/query"
	v1 "github.com/thanos-io/thanos/pkg/query/api"
	"github.com/thanos-io/thanos/pkg/rules"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/ui"
)

// registerQuery registers a query command.
func registerQuery(m map[string]setupFunc, app *kingpin.Application) {
	comp := component.Query
	cmd := app.Command(comp.String(), "query node exposing PromQL enabled Query API with data retrieved from multiple store nodes")
	conf := &queryConfig{}
	conf.registerFlag(cmd)

	m[comp.String()] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		selectorLset, err := parseFlagLabels(conf.selectorLabels)
		if err != nil {
			return errors.Wrap(err, "parse federation labels")
		}

		if dup := firstDuplicate(conf.stores); dup != "" {
			return errors.Errorf("Address %s is duplicated for --store flag.", dup)
		}

		if dup := firstDuplicate(conf.ruleEndpoints); dup != "" {
			return errors.Errorf("Address %s is duplicated for --rule flag.", dup)
		}

		var fileSD *file.Discovery
		if len(conf.fileSDFiles) > 0 {
			sdConf := &file.SDConfig{
				Files:           conf.fileSDFiles,
				RefreshInterval: conf.fileSDInterval,
			}
			fileSD = file.NewDiscovery(sdConf, logger)
		}

		promql.SetDefaultEvaluationInterval(time.Duration(conf.defaultEvaluationInterval))

		flagsMap := map[string]string{}

		// Exclude kingpin default flags to expose only Thanos ones.
		boilerplateFlags := kingpin.New("", "").Version("")

		for _, f := range cmd.Model().Flags {
			if boilerplateFlags.GetFlag(f.Name) != nil {
				continue
			}

			flagsMap[f.Name] = f.Value.String()
		}

		return runQuery(
			g,
			logger,
			reg,
			tracer,
			component.Query,
			fileSD,
			selectorLset,
			flagsMap,
			*conf,
		)
	}
}

// runQuery starts a server that exposes PromQL Query API. It is responsible for querying configured
// store nodes, merging and duplicating the data to satisfy user query.
func runQuery(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	comp component.Component,
	fileSD *file.Discovery,
	selectorLset labels.Labels,
	flagsMap map[string]string,
	conf queryConfig,
) error {
	// TODO(bplotka in PR #513 review): Move arguments into struct.
	duplicatedStores := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_query_duplicated_store_addresses_total",
		Help: "The number of times a duplicated store addresses is detected from the different configs in query",
	})

	dialOpts, err := extgrpc.StoreClientGRPCOpts(logger, reg, tracer, conf.secure, conf.clientTlsCert,
		conf.clientTlsKey, conf.clientTlsCA, conf.serverName,
	)
	if err != nil {
		return errors.Wrap(err, "building gRPC client")
	}

	fileSDCache := cache.New()
	dnsStoreProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_querier_store_apis_", reg),
		dns.ResolverType(conf.dnsSDResolver),
	)

	for _, store := range conf.strictStores {
		if dns.IsDynamicNode(store) {
			return errors.Errorf("%s is a dynamically specified store i.e. it uses SD and that is not permitted under strict mode. Use --store for this", store)
		}
	}

	dnsRuleProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_querier_rule_apis_", reg),
		dns.ResolverType(conf.dnsSDResolver),
	)

	var (
		stores = query.NewStoreSet(
			logger,
			reg,
			func() (specs []query.StoreSpec) {

				// Add strict & static nodes.
				for _, addr := range conf.strictStores {
					specs = append(specs, query.NewGRPCStoreSpec(addr, true))
				}
				// Add DNS resolved addresses from static flags and file SD.
				for _, addr := range dnsStoreProvider.Addresses() {
					specs = append(specs, query.NewGRPCStoreSpec(addr, false))
				}
				return removeDuplicateStoreSpecs(logger, duplicatedStores, specs)
			},
			func() (specs []query.RuleSpec) {
				for _, addr := range dnsRuleProvider.Addresses() {
					specs = append(specs, query.NewGRPCStoreSpec(addr, false))
				}

				// NOTE(s-urbaniak): No need to remove duplicates, as rule apis are a subset of store apis.
				// hence, any duplicates will be tracked in the store api set.

				return specs
			},
			dialOpts,
			time.Duration(conf.unhealthyStoreTimeout),
		)
		proxy            = store.NewProxyStore(logger, reg, stores.Get, component.Query, selectorLset, time.Duration(conf.storeResponseTimeout))
		rulesProxy       = rules.NewProxy(logger, stores.GetRulesClients)
		queryableCreator = query.NewQueryableCreator(logger, reg, proxy, conf.maxConcurrentSelect, time.Duration(conf.timeout))
		engine           = promql.NewEngine(
			promql.EngineOpts{
				Logger: logger,
				Reg:    reg,
				// TODO(bwplotka): Expose this as a flag: https://github.com/thanos-io/thanos/issues/703.
				MaxSamples: math.MaxInt32,
				Timeout:    time.Duration(conf.timeout),
			},
		)
	)
	// Periodically update the store set with the addresses we see in our cluster.
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return runutil.Repeat(5*time.Second, ctx.Done(), func() error {
				stores.Update(ctx)
				return nil
			})
		}, func(error) {
			cancel()
			stores.Close()
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
					stores.Update(ctxUpdate)

					if err := dnsStoreProvider.Resolve(ctxUpdate, append(fileSDCache.Addresses(), conf.stores...)); err != nil {
						level.Error(logger).Log("msg", "failed to resolve addresses for storeAPIs", "err", err)
					}
					// Rules apis do not support file service discovery as of now.
				case <-ctxUpdate.Done():
					return nil
				}
			}
		}, func(error) {
			cancelUpdate()
			close(fileSDUpdates)
		})
	}
	// Periodically update the addresses from static flags and file SD by resolving them using DNS SD if necessary.
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return runutil.Repeat(time.Duration(conf.dnsSDInterval), ctx.Done(), func() error {
				if err := dnsStoreProvider.Resolve(ctx, append(fileSDCache.Addresses(), conf.stores...)); err != nil {
					level.Error(logger).Log("msg", "failed to resolve addresses for storeAPIs", "err", err)
				}
				if err := dnsRuleProvider.Resolve(ctx, conf.ruleEndpoints); err != nil {
					level.Error(logger).Log("msg", "failed to resolve addresses for rulesAPIs", "err", err)
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

	// Start query API + UI HTTP server.
	{
		router := route.New()

		// RoutePrefix must always start with '/'.
		webRoutePrefix := "/" + strings.Trim(conf.webConf.routePrefix.prefix, "/")

		// Redirect from / to /webRoutePrefix.
		if webRoutePrefix != "/" {
			router.Get("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, webRoutePrefix, http.StatusFound)
			})
			router = router.WithPrefix(webRoutePrefix)
		}

		buildInfo := &v1.ThanosVersion{
			Version:   version.Version,
			Revision:  version.Revision,
			Branch:    version.Branch,
			BuildUser: version.BuildUser,
			BuildDate: version.BuildDate,
			GoVersion: version.GoVersion,
		}

		CWD, err := os.Getwd()
		if err != nil {
			CWD = "<error retrieving current working directory>"
			level.Warn(logger).Log("msg", "failed to retrieve current working directory", "err", err)
		}

		birth := time.Now()

		var runtimeInfo v1.RuntimeInfoFn = func() v1.RuntimeInfo {
			return v1.RuntimeInfo{
				StartTime:      birth,
				CWD:            CWD,
				GoroutineCount: runtime.NumGoroutine(),
				GOMAXPROCS:     runtime.GOMAXPROCS(0),
				GOGC:           os.Getenv("GOGC"),
				GODEBUG:        os.Getenv("GODEBUG"),
			}
		}

		ins := extpromhttp.NewInstrumentationMiddleware(reg)
		// TODO(bplotka in PR #513 review): pass all flags, not only the flags needed by prefix rewriting.
		ui.NewQueryUI(logger, reg, stores, conf.webConf.externalPrefix, conf.webConf.prefixHeaderName, runtimeInfo, *buildInfo).
			Register(router, ins)

		api := v1.NewAPI(
			logger,
			reg,
			stores,
			engine,
			queryableCreator,
			// NOTE: Will share the same replica label as the query for now.
			rules.NewGRPCClientWithDedup(rulesProxy, conf.replicaLabels),
			conf.enableAutoDownsampling,
			conf.enablePartialResponse,
			conf.enableRulePartialResponse,
			conf.replicaLabels,
			flagsMap,
			time.Duration(conf.instantDefaultMaxSourceResolution),
			conf.maxConcurrent,
			runtimeInfo,
			buildInfo,
		)

		api.Register(router.WithPrefix("/api/v1"), tracer, logger, ins)

		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(conf.http.bindAddress),
			httpserver.WithGracePeriod(time.Duration(conf.http.gracePeriod)),
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
		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"),
			conf.clientTlsCert, conf.clientTlsKey, conf.clientTlsCA,
		)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		s := grpcserver.New(logger, reg, tracer, comp, grpcProbe, proxy, rulesProxy,
			grpcserver.WithListen(conf.grpc.bindAddress),
			grpcserver.WithGracePeriod(time.Duration(conf.grpc.gracePeriod)),
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

func removeDuplicateStoreSpecs(logger log.Logger, duplicatedStores prometheus.Counter, specs []query.StoreSpec) []query.StoreSpec {
	set := make(map[string]query.StoreSpec)
	for _, spec := range specs {
		addr := spec.Addr()
		if _, ok := set[addr]; ok {
			level.Warn(logger).Log("msg", "Duplicate store address is provided - %v", addr)
			duplicatedStores.Inc()
		}
		set[addr] = spec
	}
	deduplicated := make([]query.StoreSpec, 0, len(set))
	for _, value := range set {
		deduplicated = append(deduplicated, value)
	}
	return deduplicated
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

type queryConfig struct {
	http httpConfig

	grpc          grpcConfig
	secure        bool
	clientTlsCert string
	clientTlsKey  string
	clientTlsCA   string
	serverName    string

	webConf webConfig

	timeout                           model.Duration
	maxConcurrent                     int
	maxConcurrentSelect               int
	replicaLabels                     []string
	instantDefaultMaxSourceResolution model.Duration
	enableAutoDownsampling            bool
	enablePartialResponse             bool
	defaultEvaluationInterval         model.Duration

	enableRulePartialResponse bool

	selectorLabels []string
	stores         []string
	ruleEndpoints  []string
	strictStores   []string

	fileSDFiles           []string
	fileSDInterval        model.Duration
	dnsSDInterval         model.Duration
	dnsSDResolver         string
	unhealthyStoreTimeout model.Duration
	storeResponseTimeout  model.Duration
}

func (qc *queryConfig) registerFlag(cmd *kingpin.CmdClause) *queryConfig {
	qc.http.registerFlag(cmd)

	qc.grpc.registerFlag(cmd)
	cmd.Flag("grpc-client-tls-secure", "Use TLS when talking to the gRPC server").
		Default("false").BoolVar(&qc.secure)
	cmd.Flag("grpc-client-tls-cert", "TLS Certificates to use to identify this client to the server").
		Default("").StringVar(&qc.clientTlsCert)
	cmd.Flag("grpc-client-tls-key", "TLS Key for the client's certificate").
		Default("").StringVar(&qc.clientTlsKey)
	cmd.Flag("grpc-client-tls-ca", "TLS CA Certificates to use to verify gRPC servers").
		Default("").StringVar(&qc.clientTlsCA)
	cmd.Flag("grpc-client-server-name", "Server name to verify the hostname on the returned gRPC certificates. See https://tools.ietf.org/html/rfc4366#section-3.1").
		Default("").StringVar(&qc.serverName)

	qc.webConf.registerFlag(cmd)
	qc.webConf.routePrefix.registerFlag(cmd)

	cmd.Flag("query.timeout", "Maximum time to process query by query node.").
		Default("2m").SetValue(&qc.timeout)
	cmd.Flag("query.max-concurrent", "Maximum number of queries processed concurrently by query node.").
		Default("20").IntVar(&qc.maxConcurrent)
	cmd.Flag("query.max-concurrent-select", "Maximum number of select requests made concurrently per a query.").
		Default("4").IntVar(&qc.maxConcurrentSelect)
	cmd.Flag("query.replica-label", "Labels to treat as a replica indicator along which data is deduplicated. Still you will be able to query without deduplication using 'dedup=false' parameter. Data includes time series, recording rules, and alerting rules.").
		StringsVar(&qc.replicaLabels)
	cmd.Flag("query.instant.default.max_source_resolution", "default value for max_source_resolution for instant queries. If not set, defaults to 0s only taking raw resolution into account. 1h can be a good value if you use instant queries over time ranges that incorporate times outside of your raw-retention.").
		Default("0s").Hidden().SetValue(&qc.instantDefaultMaxSourceResolution)
	cmd.Flag("query.auto-downsampling", "Enable automatic adjustment (step / 5) to what source of data should be used in store gateways if no max_source_resolution param is specified.").
		Default("false").BoolVar(&qc.enableAutoDownsampling)
	cmd.Flag("query.partial-response", "Enable partial response for queries if no partial_response param is specified. --no-query.partial-response for disabling.").
		Default("true").BoolVar(&qc.enablePartialResponse)
	cmd.Flag("query.default-evaluation-interval", "Set default evaluation interval for sub queries.").
		Default("1m").SetValue(&qc.defaultEvaluationInterval)

	cmd.Flag("rule.partial-response", "Enable partial response for rules endpoint. --no-rule.partial-response for disabling.").
		Hidden().Default("true").BoolVar(&qc.enableRulePartialResponse)

	cmd.Flag("selector-label", "Query selector labels that will be exposed in info endpoint (repeated).").
		PlaceHolder("<name>=\"<value>\"").StringsVar(&qc.selectorLabels)
	cmd.Flag("store", "Addresses of statically configured store API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect store API servers through respective DNS lookups.").
		PlaceHolder("<store>").StringsVar(&qc.stores)
	cmd.Flag("rule", "Experimental: Addresses of statically configured rules API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect rule API servers through respective DNS lookups.").
		Hidden().PlaceHolder("<rule>").StringsVar(&qc.ruleEndpoints)
	cmd.Flag("store-strict", "Addresses of only statically configured store API servers that are always used, even if the health check fails. Useful if you have a caching layer on top.").
		PlaceHolder("<staticstore>").StringsVar(&qc.strictStores)

	cmd.Flag("store.sd-files", "Path to files that contain addresses of store API servers. The path can be a glob pattern (repeatable).").
		PlaceHolder("<path>").StringsVar(&qc.fileSDFiles)
	cmd.Flag("store.sd-interval", "Refresh interval to re-read file SD files. It is used as a resync fallback.").
		Default("5m").SetValue(&qc.fileSDInterval)
	// TODO(bwplotka): Grab this from TTL at some point.
	cmd.Flag("store.sd-dns-interval", "Interval between DNS resolutions.").
		Default("30s").SetValue(&qc.dnsSDInterval)
	cmd.Flag("store.sd-dns-resolver", fmt.Sprintf("Resolver to use. Possible options: [%s, %s]", dns.GolangResolverType, dns.MiekgdnsResolverType)).
		Default(string(dns.GolangResolverType)).Hidden().StringVar(&qc.dnsSDResolver)
	cmd.Flag("store.unhealthy-timeout", "Timeout before an unhealthy store is cleaned from the store UI page.").
		Default("5m").SetValue(&qc.unhealthyStoreTimeout)
	cmd.Flag("store.response-timeout", "If a Store doesn't send any data in this specified duration then a Store will be ignored and partial data will be returned if it's enabled. 0 disables timeout.").
		Default("0ms").SetValue(&qc.storeResponseTimeout)

	return qc
}
