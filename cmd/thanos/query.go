package main

import (
	"context"
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
	"github.com/improbable-eng/thanos/pkg/query/ui"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

// registerQuery registers a query command.
func registerQuery(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "query node exposing PromQL enabled Query API with data retrieved from multiple store nodes")

	httpAddr := cmd.Flag("http-address", "listen host:port for HTTP endpoints").
		Default(defaultHTTPAddr).String()

	grpcAddr := cmd.Flag("grpc-address", "listen host:port for gRPC endpoints").
		Default(defaultGRPCAddr).String()

	queryTimeout := cmd.Flag("query.timeout", "maximum time to process query by query node").
		Default("2m").Duration()

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "maximum number of queries processed concurrently by query node").
		Default("20").Int()

	replicaLabel := cmd.Flag("query.replica-label", "label to treat as a replica indicator along which data is deduplicated. Still you will be able to query without deduplication using 'dedup=false' parameter").
		String()

	peers := cmd.Flag("cluster.peers", "initial peers to join the cluster. It can be either <ip:port>, or <domain:port>").Strings()

	clusterBindAddr := cmd.Flag("cluster.address", "listen address for cluster").
		Default(defaultClusterAddr).String()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "explicit address to advertise in cluster").
		String()

	gossipInterval := cmd.Flag("cluster.gossip-interval", "interval between sending gossip messages. By lowering this value (more frequent) gossip messages are propagated across the cluster more quickly at the expense of increased bandwidth.").
		Default(cluster.DefaultGossipInterval.String()).Duration()

	pushPullInterval := cmd.Flag("cluster.pushpull-interval", "interval for gossip state syncs . Setting this interval lower (more frequent) will increase convergence speeds across larger clusters at the expense of increased bandwidth usage.").
		Default(cluster.DefaultPushPullInterval.String()).Duration()

	selectorLabels := cmd.Flag("selector-label", "query selector labels that will be exposed in info endpoint (repeated)").
		PlaceHolder("<name>=\"<value>\"").Strings()

	stores := cmd.Flag("store", "addresses of statically configured store API servers (repeatable)").
		PlaceHolder("<store>").Strings()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer) error {
		peer, err := cluster.New(logger, reg, *clusterBindAddr, *clusterAdvertiseAddr, *peers, true, *gossipInterval, *pushPullInterval)
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

		return runQuery(g, logger, reg, tracer,
			*httpAddr,
			*grpcAddr,
			*maxConcurrentQueries,
			*queryTimeout,
			*replicaLabel,
			peer,
			selectorLset,
			*stores,
		)
	}
}

func storeClientGRPCOpts(reg *prometheus.Registry, tracer opentracing.Tracer) []grpc.DialOption {
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
		grpc.WithInsecure(),
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

	return dialOpts
}

// runQuery starts a server that exposes PromQL Query API. It is responsible for querying configured
// store nodes, merging and duplicating the data to satisfy user query.
func runQuery(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	httpAddr string,
	grpcAddr string,
	maxConcurrentQueries int,
	queryTimeout time.Duration,
	replicaLabel string,
	peer *cluster.Peer,
	selectorLset labels.Labels,
	storeAddrs []string,
) error {
	var staticSpecs []query.StoreSpec
	for _, addr := range storeAddrs {
		staticSpecs = append(staticSpecs, query.NewStaticStoreSpec(addr))
	}
	var (
		stores = query.NewStoreSet(
			logger,
			reg,
			func() (specs []query.StoreSpec) {
				specs = append(staticSpecs)

				for id, ps := range peer.PeerStates(cluster.PeerTypesStoreAPIs()...) {
					specs = append(specs, &gossipSpec{id: id, addr: ps.APIAddr, peer: peer})
				}
				return specs
			},
			storeClientGRPCOpts(reg, tracer),
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
			err := peer.Join(cluster.PeerState{
				Type:    cluster.PeerTypeQuery,
				APIAddr: httpAddr,
			})
			if err != nil {
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
		ui.New(logger, nil).Register(router)

		api := v1.NewAPI(reg, engine, queryableCreator)
		api.Register(router.WithPrefix("/api/v1"), tracer, logger)

		mux := http.NewServeMux()
		registerMetrics(mux, reg)
		registerProfile(mux)
		mux.Handle("/", router)

		l, err := net.Listen("tcp", httpAddr)
		if err != nil {
			return errors.Wrapf(err, "listen HTTP on address %s", httpAddr)
		}

		g.Add(func() error {
			return errors.Wrap(http.Serve(l, mux), "serve query")
		}, func(error) {
			l.Close()
		})
	}
	// Start query (proxy) gRPC StoreAPI.
	{
		l, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			return errors.Wrapf(err, "listen gRPC on address")
		}
		logger := log.With(logger, "component", "query")

		s := grpc.NewServer(defaultGRPCServerOpts(logger, reg, tracer)...)
		storepb.RegisterStoreServer(s, proxy)

		g.Add(func() error {
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			s.Stop()
			l.Close()
		})
	}
	level.Info(logger).Log("msg", "starting query node", "peer", peer.Name())
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

// Metadata method for gossip store tries get current peer state. If nothing is found, it means that gossip assumed
// this host is unhealthy in the meantime.
func (s *gossipSpec) Metadata(_ context.Context, _ storepb.StoreClient) (labels []storepb.Label, mint int64, maxt int64, err error) {
	state, ok := s.peer.PeerState(s.id)
	if !ok {
		return nil, 0, 0, errors.Errorf("peer %s is no longer in gossip cluster", s.id)
	}
	return state.Metadata.Labels, state.Metadata.MinTime, state.Metadata.MaxTime, nil
}
