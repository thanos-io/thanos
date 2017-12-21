package cluster

import (
	"context"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-prometheus"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

// StoreSet maintains a set of active stores. It is backed by a peer's view of the cluster.
type StoreSet struct {
	logger   log.Logger
	peer     *Peer
	dialOpts []grpc.DialOption
	mtx      sync.RWMutex
	conns    map[string]*grpc.ClientConn
	stores   []*query.StoreInfo

	storeNodeConnections prometheus.Gauge
}

// NewStoreSet returns a new store backed by the peers view of the cluster.
func NewStoreSet(logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, peer *Peer) *StoreSet {
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
		dialOpts:             dialOpts,
		conns:                map[string]*grpc.ClientConn{},
		storeNodeConnections: storeNodeConnections,
	}
}

// Update the store set to the new set of addresses and labels for that addresses.
// New background processes initiated respect the lifecycle of the given context.
func (s *StoreSet) Update(ctx context.Context) {
	var err error

	stores := make([]*query.StoreInfo, 0, len(s.conns))
	conns := make(map[string]*grpc.ClientConn, len(s.conns))

	for _, ps := range s.peer.PeerStates(PeerTypesStoreAPIs()...) {
		conn, ok := s.conns[ps.APIAddr]
		if !ok {
			conn, err = grpc.DialContext(ctx, ps.APIAddr, s.dialOpts...)
			if err != nil {
				level.Warn(s.logger).Log("msg", "dialing connection failed; skipping",
					"store", ps.APIAddr, "err", err)
				continue
			}
		}
		stores = append(stores, &query.StoreInfo{
			Addr:    ps.APIAddr,
			Client:  storepb.NewStoreClient(conn),
			Labels:  ps.Metadata.Labels,
			MinTime: ps.Metadata.MinTime,
			MaxTime: ps.Metadata.MaxTime,
		})
		conns[ps.APIAddr] = conn
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	for k, conn := range s.conns {
		if _, ok := conns[k]; !ok {
			conn.Close()
		}
	}

	s.stores = stores
	s.conns = conns

	s.storeNodeConnections.Set(float64(len(s.stores)))
}

// Get returns a list of all active stores.
func (s *StoreSet) Get() []*query.StoreInfo {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.stores
}
