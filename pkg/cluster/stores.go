package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/improbable-eng/thanos/pkg/runutil"
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

// Update the store set to the new set of addresses. New background processes intitiated respect
// the lifecycle of the given context.
func (s *StoreSet) Update(ctx context.Context) {
	// XXX(fabxc): The store as is barely ties into the cluster. This is the only place where
	// we depend on it. However, in the future this may change when we fetch additional information
	// about peers or make the set self-updating through events rather than explicit calls to Update.
	addresses := map[string]struct{}{}
	for _, ps := range s.peer.PeerStates(PeerTypeStore) {
		addresses[ps.APIAddr] = struct{}{}
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	// For each new address we create a new gRPC connection and start a background routine
	// which updates relevant metadata for the store (e.g. labels).
	for addr := range addresses {
		if _, ok := s.stores[addr]; ok {
			continue
		}
		conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			level.Warn(s.logger).Log("msg", "dialing connection failed; skipping", "store", addr, "err", err)
			continue
		}
		iterCtx, cancel := context.WithCancel(ctx)

		store := &storeInfo{conn: conn, cancel: cancel}
		s.stores[addr] = store

		go runutil.Repeat(60*time.Second, iterCtx.Done(), func() error {
			ctx, cancel := context.WithTimeout(iterCtx, 30*time.Second)
			defer cancel()

			resp, err := storepb.NewStoreClient(store.conn).Info(ctx, &storepb.InfoRequest{})
			if err != nil {
				level.Warn(s.logger).Log("msg", "failed fetching store info", "err", err)
			} else {
				store.setLabels(resp.Labels)
			}
			return nil
		})

	}
	// Delete stores that no longer exist.
	for addr, store := range s.stores {
		if _, ok := addresses[addr]; !ok {
			store.cancel()
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

	res := make([]query.StoreInfo, 0, len(s.stores))

	for _, store := range s.stores {
		res = append(res, store)
	}
	return res
}

type storeInfo struct {
	conn   *grpc.ClientConn
	cancel func()
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
