package cluster

import (
	"context"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
)

// StoreSet maintains a set of active stores. It is backed by a peer's view of the cluster.
type StoreSet struct {
	logger log.Logger
	peer   *Peer
	mtx    sync.RWMutex
	stores map[string]*storeInfo
}

// NewStoreSet returns a new store backed by the peers view of the cluster.
func NewStoreSet(logger log.Logger, peer *Peer) *StoreSet {
	return &StoreSet{
		logger: logger,
		peer:   peer,
		stores: map[string]*storeInfo{},
	}
}

// Update the store set to the new set of addresses and labels for that addresses.
// New background processes initiated respect the lifecycle of the given context.
func (s *StoreSet) Update(ctx context.Context) {
	// XXX(fabxc): The store as is barely ties into the cluster. This is the only place where
	// we depend on it. However, in the future this may change when we fetch additional information
	// about storePeers or make the set self-updating through events rather than explicit calls to Update.
	storePeers := map[string]PeerState{}
	for _, ps := range s.peer.PeerStates(PeerTypeStore) {
		storePeers[ps.APIAddr] = ps
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	// For each new store peer we create a new gRPC connection.
	// In every case we also updates the labels from peer state.
	for addr, state := range storePeers {
		if _, ok := s.stores[addr]; !ok {
			conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				level.Warn(s.logger).Log("msg", "dialing connection failed; skipping", "store", addr, "err", err)
				continue
			}
			store := &storeInfo{conn: conn}
			s.stores[addr] = store
		}

		// Always fetch up-to-date labels propagated in peer state.
		s.stores[addr].setLabels(state.Labels)
	}

	// Delete stores that no longer exist.
	for addr, store := range s.stores {
		if _, ok := storePeers[addr]; !ok {
			store.conn.Close()
			level.Info(s.logger).Log("msg", "closing connection for store")
			delete(s.stores, addr)
		}
	}
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

func (s *storeInfo) Conn() *grpc.ClientConn {
	return s.conn
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
