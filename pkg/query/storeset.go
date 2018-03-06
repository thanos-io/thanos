package query

import (
	"context"
	"math"
	"sync"
	"time"

	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// StoreSet maintains a set of active stores. It is backed by a peer's view of the cluster
// and a list of static store addresses.
type StoreSet struct {
	logger           log.Logger
	peerAddrs        func() []string
	staticAddrs      []string
	dialOpts         []grpc.DialOption
	gRPCRetryTimeout time.Duration

	mtx                  sync.RWMutex
	stores               map[string]*storeRef
	storeNodeConnections prometheus.Gauge
}

// NewStoreSet returns a new set of stores from cluster peers and statically configured ones.
func NewStoreSet(
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	peerAddrs func() []string,
	staticAddrs []string,
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
	if peerAddrs == nil {
		peerAddrs = func() []string { return nil }
	}
	return &StoreSet{
		logger:               logger,
		peerAddrs:            peerAddrs,
		staticAddrs:          staticAddrs,
		dialOpts:             dialOpts,
		storeNodeConnections: storeNodeConnections,
		gRPCRetryTimeout:     3 * time.Second,
	}
}

type storeRef struct {
	storepb.StoreClient

	mtx    sync.RWMutex
	cc     *grpc.ClientConn
	addr   string
	static bool

	// Meta (can change during runtime).
	labels  []storepb.Label
	minTime int64
	maxTime int64
}

func (s *storeRef) Labels() []storepb.Label {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.labels
}

func (s *storeRef) RangeTime() (int64, int64) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.minTime, s.maxTime
}

func (s *storeRef) String() string {
	if s.static {
		return fmt.Sprintf("[static] %s", s.addr)
	}
	return fmt.Sprintf("[gossip] %s", s.addr)
}

func (s *storeRef) close() {
	s.cc.Close()
}

func (s *StoreSet) updateStore(ctx context.Context, addr string, static bool) (*storeRef, error) {
	ctx, cancel := context.WithTimeout(ctx, s.gRPCRetryTimeout)
	defer cancel()

	st, ok := s.stores[addr]
	if !ok {
		// New store or was unhealthy and was removed in the past - create new one.
		conn, err := grpc.DialContext(ctx, addr, s.dialOpts...)
		if err != nil {
			return nil, errors.Wrap(err, "dialing connection")
		}

		st = &storeRef{
			StoreClient: storepb.NewStoreClient(conn),
			cc:          conn,
			addr:        addr,
			static:      static,
		}
	}

	// Try to reach host until timeout. If we are unable -> host is unhealthy.
	resp, err := st.Info(ctx, &storepb.InfoRequest{}, grpc.FailFast(false))
	if err != nil {
		st.close()
		return nil, errors.Wrapf(err, "fetching store info from %s", addr)
	}

	st.labels = resp.Labels
	st.maxTime = resp.MaxTime
	st.minTime = resp.MinTime
	return st, nil
}

// Update updates the store set
func (s *StoreSet) Update(ctx context.Context) {
	var (
		stores   = make(map[string]*storeRef, len(s.stores))
		innerMtx sync.Mutex
		g        errgroup.Group
	)

	for _, storeAddr := range s.staticAddrs {
		addr := storeAddr
		g.Go(func() error {
			st, err := s.updateStore(ctx, addr, true)
			if err != nil {
				return err
			}
			innerMtx.Lock()
			if dupSt, ok := stores[addr]; ok {
				level.Error(s.logger).Log("msg", "duplicated address in static store nodes.", "addr", addr)
				dupSt.close()
			}
			stores[addr] = st
			innerMtx.Unlock()

			return nil
		})
	}

	for _, storeAddr := range s.peerAddrs() {
		addr := storeAddr
		g.Go(func() error {
			if _, ok := stores[addr]; ok {
				level.Warn(s.logger).Log("msg", "statically configured node is also available via gossip.", "addr", addr)
				return nil
			}

			st, err := s.updateStore(ctx, addr, false)
			if err != nil {
				return err
			}
			innerMtx.Lock()
			if dupSt, ok := stores[addr]; ok {
				level.Error(s.logger).Log("msg", "duplicated address in gossip store nodes.", "addr", addr)
				dupSt.close()
			}
			stores[addr] = st
			innerMtx.Unlock()

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		level.Warn(s.logger).Log("msg", "update of some store nodes failed", "err", err)
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	// Remove stores that where not updated in this update, because were not in s.peerAddr() this time.
	for addr, st := range s.stores {
		if _, ok := stores[addr]; ok {
			continue
		}

		// Peer does not exists anymore.
		st.close()
	}

	s.stores = stores
	s.storeNodeConnections.Set(float64(len(s.stores)))
}

// Get returns a list of all active stores.
func (s *StoreSet) Get() []store.Client {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	stores := make([]store.Client, 0, len(s.stores))

	for _, st := range s.stores {
		stores = append(stores, st)
	}
	return stores
}

func (s *StoreSet) Close() {
	for _, st := range s.stores {
		st.close()
	}
}
