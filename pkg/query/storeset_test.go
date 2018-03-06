package query

import (
	"context"
	"net"
	"testing"

	"time"

	"github.com/fortytw2/leaktest"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testStore struct {
	info storepb.InfoResponse
}

func (s *testStore) Info(ctx context.Context, r *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	return &s.info, nil
}

func (s *testStore) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	return status.Error(codes.Unimplemented, "not implemented")
}

func (s *testStore) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *testStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

type testStores struct {
	srvs map[string]*grpc.Server
}

func newTestStores(numStores int) (*testStores, error) {
	st := &testStores{
		srvs: map[string]*grpc.Server{},
	}

	for i := 0; i < numStores; i++ {
		srv, addr, err := startStore()
		if err != nil {
			// Close so far started servers.
			st.Close()
			return nil, err
		}

		st.srvs[addr] = srv
	}

	return st, nil
}

func startStore() (*grpc.Server, string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", err
	}

	srv := grpc.NewServer()
	storepb.RegisterStoreServer(srv, &testStore{info: storepb.InfoResponse{
		Labels: []storepb.Label{
			{
				Name:  "addr",
				Value: listener.Addr().String(),
			},
		},
	}})
	go func() {
		srv.Serve(listener)
	}()

	return srv, listener.Addr().String(), nil
}

func (s *testStores) StoreAddresses() []string {
	var stores []string
	for addr := range s.srvs {
		stores = append(stores, addr)
	}
	return stores
}

func (s *testStores) Close() {
	for _, srv := range s.srvs {
		srv.Stop()
	}
	s.srvs = nil
}

func (s *testStores) CloseOne(addr string) {
	srv, ok := s.srvs[addr]
	if !ok {
		return
	}

	srv.Stop()
	delete(s.srvs, addr)
}

func TestStoreSet_StaticStores_AllAvailable(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	st, err := newTestStores(2)
	testutil.Ok(t, err)
	defer st.Close()

	initialStoreAddr := st.StoreAddresses()
	storeSet := NewStoreSet(nil, nil, nil, nil, initialStoreAddr)

	storeSet.UpdateStatic(context.Background())
	defer func() {
		// Make sure gRPC finalizers will clean up client connections.
		storeSet.staticStores = nil
		time.Sleep(9*time.Second)
	}()

	testutil.Assert(t, len(storeSet.staticStores) == 2, "all services should respond just fine, so we expect all clients to be ready.")

	for addr, store := range storeSet.staticStores {
		testutil.Equals(t, addr, store.Addr)
		testutil.Equals(t, 1, len(store.Labels))
		testutil.Equals(t, "addr", store.Labels[0].Name)
		testutil.Equals(t, addr, store.Labels[0].Value)
	}

	//unavailableAddress := initialStoreAddr[0]
	//st.CloseOne(unavailableAddress)
	//
	//// Expect UpdateStatic to tear down that node.
}

func TestStoreSet_StaticStores_OneAvailable(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	st, err := newTestStores(2)
	testutil.Ok(t, err)
	defer st.Close()

	initialStoreAddr := st.StoreAddresses()
	unavailableAddress := initialStoreAddr[0]
	st.CloseOne(unavailableAddress)

	storeSet := NewStoreSet(nil, nil, nil, nil, initialStoreAddr)

	storeSet.UpdateStatic(context.Background())
	testutil.Assert(t, len(storeSet.staticStores) == 1, "only one service should respond just fine, so we expect one client to be ready.")

	addr := initialStoreAddr[1]
	store, ok := storeSet.staticStores[addr]
	testutil.Assert(t, ok, "addr exist")
	testutil.Equals(t, addr, store.Addr)
	testutil.Equals(t, 1, len(store.Labels))
	testutil.Equals(t, "addr", store.Labels[0].Name)
	testutil.Equals(t, addr, store.Labels[0].Value)
}
