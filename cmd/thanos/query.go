package main

import (
	"context"
	"math"
	"net"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
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
		pstate := cluster.PeerState{
			Type:    cluster.PeerTypeQuery,
			APIAddr: *httpAddr,
		}
		peer, err := cluster.Join(logger, reg,
			*clusterBindAddr,
			*clusterAdvertiseAddr,
			*peers,
			pstate,
			true,
			*gossipInterval,
			*pushPullInterval,
		)
		if err != nil {
			return errors.Wrap(err, "join cluster")
		}

		selectorLset, err := parseFlagLabels(*selectorLabels)
		if err != nil {
			return errors.Wrap(err, "parse federation labels")
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
	var (
		stores = newStoreSet(logger, reg, tracer, peer, storeAddrs)
		proxy  = store.NewProxyStore(logger, func(context.Context) ([]*store.Info, error) {
			return stores.Get(), nil
		}, selectorLset)
		queryableCreator = query.NewQueryableCreator(logger, proxy, replicaLabel)
		engine           = promql.NewEngine(logger, reg, maxConcurrentQueries, queryTimeout)
	)
	// Periodically update the store set with the addresses we see in our cluster.
	{
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			return runutil.Repeat(3*time.Minute, ctx.Done(), func() error {
				stores.UpdateStatic(ctx)
				return nil
			})
		}, func(error) {
			cancel()
		})
	}
	{
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			return runutil.Repeat(5*time.Second, ctx.Done(), func() error {
				stores.UpdatePeers(ctx)
				return nil
			})
		}, func(error) {
			cancel()
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

// storeSet maintains a set of active stores. It is backed by a peer's view of the cluster
// and a list of static store addresses.
type storeSet struct {
	logger      log.Logger
	peer        *cluster.Peer
	staticAddrs []string
	dialOpts    []grpc.DialOption

	mtx          sync.RWMutex
	staticStores map[string]*store.Info
	peerStores   map[string]*store.Info

	storeNodeConnections prometheus.Gauge
}

// newStoreSet returns a new set of stores from cluster peers and statically configured ones.
func newStoreSet(
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	peer *cluster.Peer,
	static []string,
) *storeSet {
	storeNodeConnections := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_store_nodes_grpc_connections",
		Help: "Number indicating current number of gRPC connection to store nodes. This indicates also to how many stores query node have access to.",
	})
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

	if logger == nil {
		logger = log.NewNopLogger()
	}
	if reg != nil {
		reg.MustRegister(grpcMets, storeNodeConnections)
	}
	return &storeSet{
		logger:               logger,
		peer:                 peer,
		staticAddrs:          static,
		dialOpts:             dialOpts,
		storeNodeConnections: storeNodeConnections,
	}
}

func (s *storeSet) dialConn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, addr, s.dialOpts...)
	if err != nil {
		return nil, err
	}
	runtime.SetFinalizer(conn, func(cc *grpc.ClientConn) { cc.Close() })

	return conn, nil
}

func (s *storeSet) UpdateStatic(ctx context.Context) {
	stores := make(map[string]*store.Info, len(s.staticStores))

	for _, addr := range s.staticAddrs {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		var client storepb.StoreClient

		if store, ok := s.staticStores[addr]; ok {
			client = store.Client
		} else {
			conn, err := s.dialConn(ctx, addr)
			if err != nil {
				level.Warn(s.logger).Log("msg", "dialing connection failed", "addr", addr, "err", err)
				continue
			}
			client = storepb.NewStoreClient(conn)
		}

		resp, err := client.Info(ctx, &storepb.InfoRequest{})
		if err != nil {
			level.Warn(s.logger).Log("msg", "fetching store info failed", "addr", addr, "err", err)
			continue
		}
		stores[addr] = &store.Info{
			Addr:    addr,
			Client:  client,
			Labels:  resp.Labels,
			MinTime: resp.MinTime,
			MaxTime: resp.MaxTime,
		}
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.staticStores = stores
	s.storeNodeConnections.Set(float64(len(s.peerStores) + len(s.staticStores)))
}

// Update the store set to the new set of addresses and labels for that addresses.
// New background processes initiated respect the lifecycle of the given context.
func (s *storeSet) UpdatePeers(ctx context.Context) {
	stores := make(map[string]*store.Info, len(s.peerStores))

	for _, ps := range s.peer.PeerStates(cluster.PeerTypesStoreAPIs()...) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		var client storepb.StoreClient

		if store, ok := s.peerStores[ps.APIAddr]; ok {
			client = store.Client
		} else {
			conn, err := s.dialConn(ctx, ps.APIAddr)
			if err != nil {
				level.Warn(s.logger).Log("msg", "dialing connection failed", "addr", ps.APIAddr, "err", err)
				continue
			}
			client = storepb.NewStoreClient(conn)
		}
		stores[ps.APIAddr] = &store.Info{
			Addr:    ps.APIAddr,
			Client:  client,
			Labels:  ps.Metadata.Labels,
			MinTime: ps.Metadata.MinTime,
			MaxTime: ps.Metadata.MaxTime,
		}
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.peerStores = stores
	s.storeNodeConnections.Set(float64(len(s.peerStores) + len(s.staticStores)))
}

// Get returns a list of all active stores.
func (s *storeSet) Get() []*store.Info {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	stores := make([]*store.Info, 0, len(s.peerStores)+len(s.staticStores))

	for _, store := range s.staticStores {
		stores = append(stores, store)
	}
	for _, store := range s.peerStores {
		stores = append(stores, store)
	}
	return stores
}
