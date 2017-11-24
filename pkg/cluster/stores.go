package cluster

import (
	"context"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-prometheus"

	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

// StoreSet maintains a set of active stores. It is backed by a peer's view of the cluster.
type StoreSet struct {
	logger      log.Logger
	peer        *Peer
	mtx         sync.RWMutex
	stores      map[string]*storeInfo
	grpcMetrics *grpc_prometheus.ClientMetrics

	storeNodeConnections  prometheus.Gauge
	storeNodeDialDuration prometheus.Histogram
	storeNodeFailedDials  prometheus.Counter
}

// NewStoreSet returns a new store backed by the peers view of the cluster.
func NewStoreSet(logger log.Logger, reg *prometheus.Registry, peer *Peer) *StoreSet {
	met := grpc_prometheus.NewClientMetrics()

	met.EnableClientHandlingTimeHistogram(
		grpc_prometheus.WithHistogramBuckets([]float64{
			0.001, 0.01, 0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4,
		}),
	)
	reg.MustRegister(met)

	storeNodeConnections := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_store_nodes_grpc_connections",
		Help: "Number indicating current number of gRPC connection to store nodes. This indicates also to how many stores query node have access to.",
	})
	storeNodeDialDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanost_store_node_grpc_dial_seconds",
		Help: "Histogram of block gRPC dialing latency (seconds) until connection is maintained.",
	})
	storeNodeFailedDials := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanost_store_node_grpc_dial_failures_total",
		Help: "Number of failed gRPC dials targeting store node.",
	})
	reg.MustRegister(storeNodeConnections)
	reg.MustRegister(storeNodeDialDuration)
	reg.MustRegister(storeNodeFailedDials)

	return &StoreSet{
		logger:                logger,
		peer:                  peer,
		stores:                map[string]*storeInfo{},
		grpcMetrics:           met,
		storeNodeConnections:  storeNodeConnections,
		storeNodeDialDuration: storeNodeDialDuration,
		storeNodeFailedDials:  storeNodeFailedDials,
	}
}

// Update the store set to the new set of addresses and labels for that addresses.
// New background processes initiated respect the lifecycle of the given context.
func (s *StoreSet) Update(ctx context.Context) {
	// XXX(fabxc): The store as is barely ties into the cluster. This is the only place where
	// we depend on it. However, in the future this may change when we fetch additional information
	// about storePeers or make the set self-updating through events rather than explicit calls to Update.
	storePeers := map[string]PeerState{}
	for _, ps := range s.peer.PeerStates(PeerTypesStoreAPIs()...) {
		storePeers[ps.APIAddr] = ps
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	// For each new store peer we create a new gRPC connection.
	// In every case we also updates the labels from peer state.
	for addr, state := range storePeers {
		if _, ok := s.stores[addr]; !ok {
			level.Debug(s.logger).Log("msg", "grpc dialing", "store", addr)

			startTime := time.Now()
			conn, err := grpc.DialContext(ctx, addr,
				grpc.WithInsecure(),
				grpc.WithUnaryInterceptor(s.grpcMetrics.UnaryClientInterceptor()),
				grpc.WithStreamInterceptor(s.grpcMetrics.StreamClientInterceptor()),
			)
			if err != nil {
				s.storeNodeFailedDials.Inc()
				level.Warn(s.logger).Log("msg", "dialing connection failed; skipping", "store", addr, "err", err)
				continue
			}

			s.storeNodeDialDuration.Observe(time.Since(startTime).Seconds())
			level.Debug(s.logger).Log("msg", "successfully made grpc connection", "store", addr)

			store := &storeInfo{conn: conn}
			s.stores[addr] = store
		}

		// Always fetch up-to-date labels propagated in peer state.
		s.stores[addr].setLabels(state.Metadata.Labels)
	}

	// Delete stores that no longer exist.
	for addr, store := range s.stores {
		if _, ok := storePeers[addr]; !ok {
			store.conn.Close()
			level.Info(s.logger).Log("msg", "closing connection for store")
			delete(s.stores, addr)
		}
	}

	s.storeNodeConnections.Set(float64(len(s.stores)))
}

// Get returns a list of all active stores.
func (s *StoreSet) Get() []query.StoreInfo {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var res []query.StoreInfo
	for _, store := range s.stores {
		res = append(res, store)
	}
	return res
}

type storeInfo struct {
	conn   *grpc.ClientConn
	mtx    sync.RWMutex
	labels []storepb.Label
}

var _ query.StoreInfo = (*storeInfo)(nil)

func (s *storeInfo) Client() storepb.StoreClient {
	return storepb.NewStoreClient(s.conn)
}

func (s *storeInfo) Labels() []storepb.Label {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.labels
}

func (s *storeInfo) setLabels(lset []storepb.Label) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.labels = lset
}
