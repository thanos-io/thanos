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
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/improbable-eng/thanos/pkg/query/api"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/improbable-eng/thanos/pkg/ui"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gopkg.in/alecthomas/kingpin.v2"
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

	queryTimeout := modelDuration(cmd.Flag("query.timeout", "Maximum time to process query by query node.").
		Default("2m"))

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "Maximum number of queries processed concurrently by query node.").
		Default("20").Int()

	replicaLabel := cmd.Flag("query.replica-label", "Label to treat as a replica indicator along which data is deduplicated. Still you will be able to query without deduplication using 'dedup=false' parameter.").
		String()

	selectorLabels := cmd.Flag("selector-label", "Query selector labels that will be exposed in info endpoint (repeated).").
		PlaceHolder("<name>=\"<value>\"").Strings()

	stores := cmd.Flag("store", "Addresses of statically configured store API servers (repeatable).").
		PlaceHolder("<store>").Strings()

	enableAutodownsampling := cmd.Flag("query.auto-downsampling", "Enable automatic adjustment (step / 5) to what source of data should be used in store gateways if no max_source_resolution param is specified. ").
		Default("false").Bool()

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
			*maxConcurrentQueries,
			time.Duration(*queryTimeout),
			*replicaLabel,
			peer,
			selectorLset,
			*stores,
			*enableAutodownsampling,
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
	maxConcurrentQueries int,
	queryTimeout time.Duration,
	replicaLabel string,
	peer *cluster.Peer,
	selectorLset labels.Labels,
	storeAddrs []string,
	enableAutodownsampling bool,
) error {
	var staticSpecs []query.StoreSpec
	for _, addr := range storeAddrs {
		if addr == "" {
			return errors.New("static store address cannot be empty")
		}

		staticSpecs = append(staticSpecs, query.NewGRPCStoreSpec(addr))
	}

	dialOpts, err := storeClientGRPCOpts(logger, reg, tracer, secure, cert, key, caCert, serverName)
	if err != nil {
		return errors.Wrap(err, "building gRPC client")
	}

	var (
		stores = query.NewStoreSet(
			logger,
			reg,
			func() (specs []query.StoreSpec) {
				specs = append(staticSpecs)

				for id, ps := range peer.PeerStates(cluster.PeerTypesStoreAPIs()...) {
					if ps.StoreAPIAddr == "" {
						level.Error(logger).Log("msg", "Gossip found peer that propagates empty address, ignoring.", "lset", fmt.Sprintf("%v", ps.Metadata.Labels))
						continue
					}

					specs = append(specs, &gossipSpec{id: id, addr: ps.StoreAPIAddr, peer: peer})
				}
				return specs
			},
			dialOpts,
		)
		proxy = store.NewProxyStore(logger, func(context.Context) ([]store.Client, error) {
			return stores.Get(), nil
		}, selectorLset)
		queryableCreator = query.NewQueryableCreator(logger, proxy, replicaLabel)
		engine           = promql.NewEngine(logger, reg, maxConcurrentQueries, queryTimeout)
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
	// Start query API + UI HTTP server.
	{
		router := route.New()
		ui.NewQueryUI(logger, nil).Register(router)

		api := v1.NewAPI(logger, reg, engine, queryableCreator, enableAutodownsampling)
		api.Register(router.WithPrefix("/api/v1"), tracer, logger)

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
		logger := log.With(logger, "component", "query")

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

type gossipSpec struct {
	id   string
	addr string

	peer *cluster.Peer
}

func (s *gossipSpec) Addr() string {
	return s.addr
}

// Metadata method for gossip store tries get current peer state.
func (s *gossipSpec) Metadata(_ context.Context, _ storepb.StoreClient) (labels []storepb.Label, mint int64, maxt int64, err error) {
	state, ok := s.peer.PeerState(s.id)
	if !ok {
		return nil, 0, 0, errors.Errorf("peer %s is no longer in gossip cluster", s.id)
	}
	return state.Metadata.Labels, state.Metadata.MinTime, state.Metadata.MaxTime, nil
}
