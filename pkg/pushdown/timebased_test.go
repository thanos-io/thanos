// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pushdown

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/pushdown/querypb"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	grpc "google.golang.org/grpc"
)

type storeClientMock struct {
	mint, maxt  int64
	hasQueryAPI bool
}

func (sc *storeClientMock) Addr() string {
	return ""
}

func (sc *storeClientMock) String() string {
	return ""
}

func (sc *storeClientMock) TimeRange() (int64, int64) {
	return sc.mint, sc.maxt
}

func (sc *storeClientMock) LabelSets() []labels.Labels {
	return []labels.Labels{}
}

func (sc *storeClientMock) Info(ctx context.Context, in *storepb.InfoRequest, opts ...grpc.CallOption) (*storepb.InfoResponse, error) {
	return nil, nil
}

func (sc *storeClientMock) Series(ctx context.Context, in *storepb.SeriesRequest, opts ...grpc.CallOption) (storepb.Store_SeriesClient, error) {
	return nil, nil
}

func (sc *storeClientMock) LabelNames(ctx context.Context, in *storepb.LabelNamesRequest, opts ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	return nil, nil
}

func (sc *storeClientMock) LabelValues(ctx context.Context, in *storepb.LabelValuesRequest, opts ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	return nil, nil
}

type mockQueryClient struct {
}

func (m *mockQueryClient) Query(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (*querypb.QueryResponse, error) {
	return nil, nil
}

func (sc *storeClientMock) QueryAPI() querypb.QueryClient {
	if sc.hasQueryAPI {
		return &mockQueryClient{}
	}
	return nil
}

func TestOverlappingRanges(t *testing.T) {
	for _, tcase := range []struct {
		stores         []storeClientMock
		match          bool
		startNS, endNS int64
		name           string
	}{
		{
			name:   "Zero test",
			stores: []storeClientMock{},
			match:  false,
		},
		{
			name:  "One overlapping store but no queryapi",
			match: false,
			stores: []storeClientMock{
				{
					mint:        1000_000,
					maxt:        2000_000,
					hasQueryAPI: false,
				},
			},
			startNS: 1000 * 1e9,
			endNS:   2000 * 1e9,
		},
		{
			name:  "One overlapping store but with queryapi",
			match: true,
			stores: []storeClientMock{
				{
					mint:        1000_000,
					maxt:        2000_000,
					hasQueryAPI: true,
				},
			},
			startNS: 1000 * 1e9,
			endNS:   2000 * 1e9,
		},
		{
			name:  "Multiple overlapping stores with QueryAPI",
			match: false,
			stores: []storeClientMock{
				{
					mint:        1000_000,
					maxt:        2000_000,
					hasQueryAPI: true,
				},
				{
					mint:        500_000,
					maxt:        2500_000,
					hasQueryAPI: true,
				},
			},
			startNS: 1000 * 1e9,
			endNS:   2000 * 1e9,
		},
		{
			name:  "Multiple overlapping stores with no QueryAPI",
			match: false,
			stores: []storeClientMock{
				{
					mint:        1000_000,
					maxt:        2000_000,
					hasQueryAPI: false,
				},
				{
					mint:        500_000,
					maxt:        2500_000,
					hasQueryAPI: false,
				},
			},
			startNS: 1000 * 1e9,
			endNS:   2000 * 1e9,
		},
		{
			name:  "Partially overlapping store with QueryAPI",
			match: true,
			stores: []storeClientMock{
				{
					mint:        500_000,
					maxt:        1500_000,
					hasQueryAPI: true,
				},
			},
			startNS: 1000 * 1e9,
			endNS:   2000 * 1e9,
		},
	} {
		tcase := tcase

		t.Run(tcase.name, func(t *testing.T) {
			adapter := NewTimeBasedPushdown(
				func() []store.Client {
					clients := []store.Client{}
					for _, st := range tcase.stores {
						clients = append(clients, store.Client(&st))
					}
					return clients
				},
				prometheus.NewRegistry(),
			)

			qapi, m := adapter.Match(tcase.startNS, tcase.endNS)
			testutil.Assert(t, m == tcase.match)
			if m {
				testutil.Assert(t, qapi != nil)
			}
		})

	}

}
