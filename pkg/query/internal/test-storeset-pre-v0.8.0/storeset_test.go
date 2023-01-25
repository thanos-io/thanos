// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package testoldstoreset

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
)

var testGRPCOpts = []grpc.DialOption{
	grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
	grpc.WithTransportCredentials(insecure.NewCredentials()),
}

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

type testStoreMeta struct {
	extlsetFn func(addr string) []labelpb.ZLabelSet
	storeType component.StoreAPI
}

type testStores struct {
	srvs map[string]*grpc.Server
}

func startTestStores(stores []testStoreMeta) (*testStores, error) {
	st := &testStores{
		srvs: map[string]*grpc.Server{},
	}

	for _, store := range stores {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			// Close so far started servers.
			st.Close()
			return nil, err
		}

		srv := grpc.NewServer()
		storepb.RegisterStoreServer(srv, &testStore{info: storepb.InfoResponse{LabelSets: store.extlsetFn(listener.Addr().String()), StoreType: store.storeType.ToProto()}})
		go func() {
			_ = srv.Serve(listener)
		}()

		st.srvs[listener.Addr().String()] = srv
	}

	return st, nil
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

func specsFromAddrFunc(addrs []string) func() []StoreSpec {
	return func() (specs []StoreSpec) {
		for _, addr := range addrs {
			specs = append(specs, NewGRPCStoreSpec(addr))
		}
		return specs
	}
}

func TestMain(m *testing.M) {
	custom.TolerantVerifyLeakMain(m)
}

func TestPre0_8_0_StoreSet_AgainstNewStoreGW(t *testing.T) {
	st, err := startTestStores([]testStoreMeta{
		{
			storeType: component.Sidecar,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
				}
			},
		},
		{
			storeType: component.Store,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							// This is the labelset exposed by store when having only one sidecar's data.
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
					{
						Labels: []labelpb.ZLabel{{Name: store.CompatibilityTypeLabelName, Value: "store"}},
					},
				}
			},
		},
		// We expect this to be duplicated.
		{
			storeType: component.Store,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
					{
						Labels: []labelpb.ZLabel{{Name: store.CompatibilityTypeLabelName, Value: "store"}},
					},
				}
			},
		},
	})
	testutil.Ok(t, err)
	defer st.Close()

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = level.NewFilter(logger, level.AllowDebug())
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	storeSet := NewStoreSet(logger, nil, specsFromAddrFunc(st.StoreAddresses()), testGRPCOpts, time.Minute)
	storeSet.gRPCInfoCallTimeout = 2 * time.Second
	defer storeSet.Close()

	// Should not matter how many of these we run.
	storeSet.Update(context.Background())
	storeSet.Update(context.Background())
	storeSet.Update(context.Background())
	storeSet.Update(context.Background())

	testutil.Assert(t, len(storeSet.stores) == 2, fmt.Sprintf("all services should respond just fine, but we expect duplicates being blocked. Expected %d stores, got %d", 5, len(storeSet.stores)))

	// Sort result to be able to compare.
	var existingStoreLabels [][]labels.Labels
	for _, store := range storeSet.stores {
		lset := append([]labels.Labels{}, store.LabelSets()...)
		existingStoreLabels = append(existingStoreLabels, lset)
	}
	sort.Slice(existingStoreLabels, func(i, j int) bool {
		return len(existingStoreLabels[i]) > len(existingStoreLabels[j])
	})

	testutil.Equals(t, [][]labels.Labels{
		{
			{
				{Name: "l1", Value: "v2"},
				{Name: "l2", Value: "v3"},
			},
			{
				{Name: store.CompatibilityTypeLabelName, Value: "store"},
			},
		},
		{
			{
				{Name: "l1", Value: "v2"},
				{Name: "l2", Value: "v3"},
			},
		},
	}, existingStoreLabels)
}
