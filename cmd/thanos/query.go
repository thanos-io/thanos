package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"path"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/component"
	"github.com/improbable-eng/thanos/pkg/discovery/cache"
	"github.com/improbable-eng/thanos/pkg/discovery/dns"
	"github.com/improbable-eng/thanos/pkg/extprom"
	"github.com/improbable-eng/thanos/pkg/query"
	v1 "github.com/improbable-eng/thanos/pkg/query/api"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/improbable-eng/thanos/pkg/ui"
	"github.com/oklog/run"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/discovery/file"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

// registerQuery registers a query command.
func registerQuery(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "query node exposing PromQL enabled Query API with data retrieved from multiple store nodes")

	grpcBindAddr, httpBindAddr, srvCert, srvKey, srvClientCA, newPeerFn := regCommonServerFlags(cmd)

	httpAdvertiseAddr := cmd.Flag("http-advertise-address", "Explicit (external) host:port address to advertise for HTTP QueryAPI in gossip cluster. If empty, 'http-address' will be used.").
		String()

	secure := cmd.Flag("grpc-client-tls-secure", "Use TLS when talking to the gRPC server").Default("false").Bool()
	cert := cmd.Flag("grpc-client-tls-cert", "TLS Certificates to use to identify this client to the server").Default("").String()
	key := cmd.Flag("grpc-client-tls-key", "TLS Key for the client's certificate").Default("").String()
	caCert := cmd.Flag("grpc-client-tls-ca", "TLS CA Certificates to use to verify gRPC servers").Default("").String()
	serverName := cmd.Flag("grpc-client-server-name", "Server name to verify the hostname on the returned gRPC certificates. See https://tools.ietf.org/html/rfc4366#section-3.1").Default("").String()

	webRoutePrefix := cmd.Flag("web.route-prefix", "Prefix for API and UI endpoints. This allows thanos UI to be served on a sub-path. This option is analogous to --web.route-prefix of Promethus.").Default("").String()
	webExternalPrefix := cmd.Flag("web.external-prefix", "Static prefix for all HTML links and redirect URLs in the UI query web interface. Actual endpoints are still served on / or the web.route-prefix. This allows thanos UI to be served behind a reverse proxy that strips a URL sub-path.").Default("").String()
	webPrefixHeaderName := cmd.Flag("web.prefix-header", "Name of HTTP request header used for dynamic prefixing of UI links and redirects. This option is ignored if web.external-prefix argument is set. Security risk: enable this option only if a reverse proxy in front of thanos is resetting the header. The --web.prefix-header=X-Forwarded-Prefix option can be useful, for example, if Thanos UI is served via Traefik reverse proxy with PathPrefixStrip option enabled, which sends the stripped prefix value in X-Forwarded-Prefix header. This allows thanos UI to be served on a sub-path.").Default("").String()

	queryTimeout := modelDuration(cmd.Flag("query.timeout", "Maximum time to process query by query node.").
		Default("2m"))

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "Maximum number of queries processed concurrently by query node.").
		Default("20").Int()

	replicaLabel := cmd.Flag("query.replica-label", "Label to treat as a replica indicator along which data is deduplicated. Still you will be able to query without deduplication using 'dedup=false' parameter.").
		String()

	selectorLabels := cmd.Flag("selector-label", "Query selector labels that will be exposed in info endpoint (repeated).").
		PlaceHolder("<name>=\"<value>\"").Strings()

	stores := cmd.Flag("store", "Addresses of statically configured store API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect store API servers through respective DNS lookups.").
		PlaceHolder("<store>").Strings()

	fileSDFiles := cmd.Flag("store.sd-files", "Path to files that contain addresses of store API servers. The path can be a glob pattern (repeatable).").
		PlaceHolder("<path>").Strings()

	fileSDInterval := modelDuration(cmd.Flag("store.sd-interval", "Refresh interval to re-read file SD files. It is used as a resync fallback.").
		Default("5m"))

	dnsSDInterval := modelDuration(cmd.Flag("store.sd-dns-interval", "Interval between DNS resolutions.").
		Default("30s"))

	enableAutodownsampling := cmd.Flag("query.auto-downsampling", "Enable automatic adjustment (step / 5) to what source of data should be used in store gateways if no max_source_resolution param is specified. ").
		Default("false").Bool()

	enablePartialResponse := cmd.Flag("query.partial-response", "Enable partial response for queries if no partial_response param is specified.").
		Default("true").Bool()

	storeResponseTimeout := modelDuration(cmd.Flag("store.response-timeout", "If a Store doesn't send any data in this specified duration then a Store will be ignored and partial data will be returned if it's enabled. 0 disables timeout.").Default("0ms"))

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		peer, err := newPeerFn(logger, reg, true, *httpAdvertiseAddr, true)
		if err != nil {
			return errors.Wrap(err, "new cluster peer")
		}
		selectorLset, err := parseFlagLabels(*selectorLabels)
		if err != nil {
			return errors.Wrap(err, "parse federation labels")
		}

		lookupStores := map[string]struct{}{}
		for _, s := range *stores {
			if _, ok := lookupStores[s]; ok {
				return errors.Errorf("Address %s is duplicated for --store flag.", s)
			}

			lookupStores[s] = struct{}{}
		}

		var fileSD *file.Discovery
		if len(*fileSDFiles) > 0 {
			conf := &file.SDConfig{
				Files:           *fileSDFiles,
				RefreshInterval: *fileSDInterval,
			}
			fileSD = file.NewDiscovery(conf, logger)
		}

		return runQuery(
			g,
			logger,
			reg,
			tracer,
			*grpcBindAddr,
			*srvCert,
			*srvKey,
			*srvClientCA,
			*secure,
			*cert,
			*key,
			*caCert,
			*serverName,
			*httpBindAddr,
			*webRoutePrefix,
			*webExternalPrefix,
			*webPrefixHeaderName,
			*maxConcurrentQueries,
			time.Duration(*queryTimeout),
			time.Duration(*storeResponseTimeout),
			*replicaLabel,
			peer,
			selectorLset,
			*stores,
			*enableAutodownsampling,
			*enablePartialResponse,
			fileSD,
			time.Duration(*dnsSDInterval),
		)
	}
}

func storeClientGRPCOpts(logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, secure bool, cert, key, caCert string, serverName string) ([]grpc.DialOption, error) {
	grpcMets := grpc_prometheus.NewClientMetrics()
	grpcMets.EnableClientHandlingTimeHistogram(
		grpc_prometheus.WithHistogramBuckets([]float64{
			0.001, 0.01, 0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4,
		}),
	)
	dialOpts := []grpc.DialOption{
		// We want to make sure that we can receive huge gRPC messages from storeAPI.
		// On TCP level we can be fine, but the gRPC overhead for huge messages could be significant.
		// Current limit is ~2GB.
		// TODO(bplotka): Split sent chunks on store node per max 4MB chunks if needed.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
		grpc.WithUnaryInterceptor(
			grpc_middleware.ChainUnaryClient(
				grpcMets.UnaryClientInterceptor(),
				tracing.UnaryClientInterceptor(tracer),
			),
		),
		grpc.WithStreamInterceptor(
			grpc_middleware.ChainStreamClient(
				grpcMets.StreamClientInterceptor(),
				tracing.StreamClientInterceptor(tracer),
			),
		),
	}

	if reg != nil {
		reg.MustRegister(grpcMets)
	}

	if !secure {
		return append(dialOpts, grpc.WithInsecure()), nil
	}

	level.Info(logger).Log("msg", "Enabling client to server TLS")

	var certPool *x509.CertPool

	if caCert != "" {
		caPEM, err := ioutil.ReadFile(caCert)
		if err != nil {
			return nil, errors.Wrap(err, "reading client CA")
		}

		certPool = x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caPEM) {
			return nil, errors.Wrap(err, "building client CA")
		}
		level.Info(logger).Log("msg", "TLS Client using provided certificate pool")
	} else {
		var err error
		certPool, err = x509.SystemCertPool()
		if err != nil {
			return nil, errors.Wrap(err, "reading system certificate pool")
		}
		level.Info(logger).Log("msg", "TLS Client using system certificate pool")
	}

	tlsCfg := &tls.Config{
		RootCAs: certPool,
	}

	if serverName != "" {
		tlsCfg.ServerName = serverName
	}

	if cert != "" {
		cert, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, errors.Wrap(err, "client credentials")
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
		level.Info(logger).Log("msg", "TLS Client authentication enabled")
	}

	creds := credentials.NewTLS(tlsCfg)

	return append(dialOpts, grpc.WithTransportCredentials(creds)), nil
}

// runQuery starts a server that exposes PromQL Query API. It is responsible for querying configured
// store nodes, merging and duplicating the data to satisfy user query.
func runQuery(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	grpcBindAddr string,
	srvCert string,
	srvKey string,
	srvClientCA string,
	secure bool,
	cert string,
	key string,
	caCert string,
	serverName string,
	httpBindAddr string,
	webRoutePrefix string,
	webExternalPrefix string,
	webPrefixHeaderName string,
	maxConcurrentQueries int,
	queryTimeout time.Duration,
	storeResponseTimeout time.Duration,
	replicaLabel string,
	peer cluster.Peer,
	selectorLset labels.Labels,
	storeAddrs []string,
	enableAutodownsampling bool,
	enablePartialResponse bool,
	fileSD *file.Discovery,
	dnsSDInterval time.Duration,
) error {
	// TODO(bplotka in PR #513 review): Move arguments into struct.
	duplicatedStores := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_query_duplicated_store_address",
		Help: "The number of times a duplicated store addresses is detected from the different configs in query",
	})
	reg.MustRegister(duplicatedStores)

	dialOpts, err := storeClientGRPCOpts(logger, reg, tracer, secure, cert, key, caCert, serverName)
	if err != nil {
		return errors.Wrap(err, "building gRPC client")
	}

	fileSDCache := cache.New()
	dnsProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_querier_store_apis", reg),
	)

	var (
		stores = query.NewStoreSet(
			logger,
			reg,
			func() (specs []query.StoreSpec) {
				// Add store specs from gossip.
				for id, ps := range peer.PeerStates(cluster.PeerTypesStoreAPIs()...) {
					if ps.StoreAPIAddr == "" {
						level.Error(logger).Log("msg", "Gossip found peer that propagates empty address, ignoring.", "lset", fmt.Sprintf("%v", ps.Metadata.Labels))
						continue
					}

					specs = append(specs, &gossipSpec{id: id, addr: ps.StoreAPIAddr, stateFetcher: peer})
				}

				// Add DNS resolved addresses from static flags and file SD.
				for _, addr := range dnsProvider.Addresses() {
					specs = append(specs, query.NewGRPCStoreSpec(addr))
				}

				specs = removeDuplicateStoreSpecs(logger, duplicatedStores, specs)

				return specs
			},
			dialOpts,
		)
		proxy            = store.NewProxyStore(logger, stores.Get, component.Query, selectorLset, storeResponseTimeout)
		queryableCreator = query.NewQueryableCreator(logger, proxy, replicaLabel)
		engine           = promql.NewEngine(
			promql.EngineOpts{
				Logger:        logger,
				Reg:           reg,
				MaxConcurrent: maxConcurrentQueries,
				// TODO(bwplotka): Expose this as a flag: https://github.com/improbable-eng/thanos/issues/703
				MaxSamples: math.MaxInt32,
				Timeout:    queryTimeout,
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
					dnsProvider.Resolve(ctxUpdate, append(fileSDCache.Addresses(), storeAddrs...))
				case <-ctxUpdate.Done():
					return nil
				}
			}
		}, func(error) {
			cancelUpdate()
			close(fileSDUpdates)
		})
	}
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			// New gossip cluster.
			if err := peer.Join(cluster.PeerTypeQuery, cluster.PeerMetadata{}); err != nil {
				return errors.Wrap(err, "join cluster")
			}

			<-ctx.Done()
			return nil
		}, func(error) {
			cancel()
			peer.Close(5 * time.Second)
		})
	}
	// Periodically update the addresses from static flags and file SD by resolving them using DNS SD if necessary.
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return runutil.Repeat(dnsSDInterval, ctx.Done(), func() error {
				dnsProvider.Resolve(ctx, append(fileSDCache.Addresses(), storeAddrs...))
				return nil
			})
		}, func(error) {
			cancel()
		})
	}
	// Start query API + UI HTTP server.
	{
		router := route.New()

		// redirect from / to /webRoutePrefix
		if webRoutePrefix != "" {
			router.Get("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, webRoutePrefix, http.StatusFound)
			})
		}

		flagsMap := map[string]string{
			// TODO(bplotka in PR #513 review): pass all flags, not only the flags needed by prefix rewriting.
			"web.external-prefix": webExternalPrefix,
			"web.prefix-header":   webPrefixHeaderName,
		}

		ui.NewQueryUI(logger, stores, flagsMap).Register(router.WithPrefix(webRoutePrefix))

		api := v1.NewAPI(logger, reg, engine, queryableCreator, enableAutodownsampling, enablePartialResponse)

		api.Register(router.WithPrefix(path.Join(webRoutePrefix, "/api/v1")), tracer, logger)

		router.Get("/-/healthy", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			if _, err := fmt.Fprintf(w, "Thanos Querier is Healthy.\n"); err != nil {
				level.Error(logger).Log("msg", "Could not write health check response.")
			}
		})

		mux := http.NewServeMux()
		registerMetrics(mux, reg)
		registerProfile(mux)
		mux.Handle("/", router)

		l, err := net.Listen("tcp", httpBindAddr)
		if err != nil {
			return errors.Wrapf(err, "listen HTTP on address %s", httpBindAddr)
		}

		g.Add(func() error {
			level.Info(logger).Log("msg", "Listening for query and metrics", "address", httpBindAddr)
			return errors.Wrap(http.Serve(l, mux), "serve query")
		}, func(error) {
			runutil.CloseWithLogOnErr(logger, l, "query and metric listener")
		})
	}
	// Start query (proxy) gRPC StoreAPI.
	{
		l, err := net.Listen("tcp", grpcBindAddr)
		if err != nil {
			return errors.Wrapf(err, "listen gRPC on address")
		}
		logger := log.With(logger, "component", component.Query.String())

		opts, err := defaultGRPCServerOpts(logger, reg, tracer, srvCert, srvKey, srvClientCA)
		if err != nil {
			return errors.Wrapf(err, "build gRPC server")
		}

		s := grpc.NewServer(opts...)
		storepb.RegisterStoreServer(s, proxy)

		g.Add(func() error {
			level.Info(logger).Log("msg", "Listening for StoreAPI gRPC", "address", grpcBindAddr)
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			s.Stop()
			runutil.CloseWithLogOnErr(logger, l, "store gRPC listener")
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

type gossipSpec struct {
	id   string
	addr string

	stateFetcher cluster.PeerStateFetcher
}

func (s *gossipSpec) Addr() string {
	return s.addr
}

// Metadata method for gossip store tries get current peer state.
func (s *gossipSpec) Metadata(_ context.Context, _ storepb.StoreClient) (labels []storepb.Label, mint int64, maxt int64, err error) {
	state, ok := s.stateFetcher.PeerState(s.id)
	if !ok {
		return nil, 0, 0, errors.Errorf("peer %s is no longer in gossip cluster", s.id)
	}
	return state.Metadata.Labels, state.Metadata.MinTime, state.Metadata.MaxTime, nil
}
