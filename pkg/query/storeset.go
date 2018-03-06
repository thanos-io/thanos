package query

import (
	"context"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

// StoreSet maintains a set of active stores. It is backed by a peer's view of the cluster
// and a list of static store addresses.
type StoreSet struct {
	logger      log.Logger
	peer        *cluster.Peer
	staticAddrs []string
	dialOpts    []grpc.DialOption

	mtx          sync.RWMutex
	staticStores map[string]*store.Info
	peerStores   map[string]*store.Info

	storeNodeConnections prometheus.Gauge
}

// NewStoreSet returns a new set of stores from cluster peers and statically configured ones.
func NewStoreSet(
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	peer *cluster.Peer,
	static []string,
) *StoreSet {
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
	return &StoreSet{
		logger:               logger,
		peer:                 peer,
		staticAddrs:          static,
		dialOpts:             dialOpts,
		storeNodeConnections: storeNodeConnections,
	}
}

func (s *StoreSet) createClientConn(ctx context.Context, addr string, stores map[string]*store.Info) (storepb.StoreClient, error) {
	if st, ok := s.staticStores[addr]; ok {
		return st.Client, nil
	}

	// Consider blocking to catch errors sooner.
	conn, err := grpc.DialContext(ctx, addr, s.dialOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "dialing connection")
	}
	runtime.SetFinalizer(conn, func(cc *grpc.ClientConn) { cc.Close() })

	return storepb.NewStoreClient(conn), nil
}

func (s *StoreSet) UpdateStatic(ctx context.Context) {
	stores := make(map[string]*store.Info, len(s.staticStores))

	// TODO(bplotka): Pack it in errgroup.
	for _, addr := range s.staticAddrs {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		client, err := s.createClientConn(ctx, addr, s.staticStores)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to create client connection for static address", "addr", addr, "err", err)
			continue
		}

		// If this fails, close connection explicitly.
		resp, err := client.Info(ctx, &storepb.InfoRequest{})
		if err != nil {
			level.Warn(s.logger).Log("msg", "fetching store info failed", "addr", addr, "err", err)
			continue
		}

		// We need to handle explicitly unavailable static stores. They are not part of gossip so won't automatically
		// removed from `stores` if they are unavailable.
		// TODO(bplotka): Remove if not responding.
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

// UpdatePeers updates the store set to the new set of addresses and labels for that not seen peer.
func (s *StoreSet) UpdatePeers(ctx context.Context) {
	stores := make(map[string]*store.Info, len(s.peerStores))

	for _, ps := range s.peer.PeerStates(cluster.PeerTypesStoreAPIs()...) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		client, err := s.createClientConn(ctx, ps.APIAddr, s.peerStores)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to create client connection for peer address", "addr", ps.APIAddr, "err", err)
			continue
		}

		// We always assume, that healthy store node from gossip will be ready to serve gRPC traffic, so
		// we immediately put it as ready store (dial is non-blocking).
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
func (s *StoreSet) Get() []*store.Info {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	stores := make([]*store.Info, 0, len(s.peerStores)+len(s.staticStores))

	for _, st := range s.staticStores {
		stores = append(stores, st)
	}
	for _, st := range s.peerStores {
		stores = append(stores, st)
	}
	return stores
}
