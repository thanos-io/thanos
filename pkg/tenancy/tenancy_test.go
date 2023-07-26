// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tenancy_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tenancy"

	"github.com/pkg/errors"

	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
)

// mockedStoreAPI is test gRPC store API client.
type mockedStoreAPI struct {
	RespSeries      []*storepb.SeriesResponse
	RespLabelValues *storepb.LabelValuesResponse
	RespLabelNames  *storepb.LabelNamesResponse
	RespError       error
	RespDuration    time.Duration
	// Index of series in store to slow response.
	SlowSeriesIndex int

	LastSeriesReq      *storepb.SeriesRequest
	LastLabelValuesReq *storepb.LabelValuesRequest
	LastLabelNamesReq  *storepb.LabelNamesRequest

	t *testing.T
}

// storeSeriesServer is test gRPC storeAPI series server.
type storeSeriesServer struct {
	storepb.Store_SeriesServer

	ctx context.Context
}

func (s *storeSeriesServer) Context() context.Context {
	return s.ctx
}

const testTenant = "test-tenant"

func getAndAssertTenant(ctx context.Context, t *testing.T) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok || len(md.Get(tenancy.DefaultTenantHeader)) == 0 {
		testutil.Ok(t, errors.Errorf("could not get tenant from grpc metadata, using default: %s", tenancy.DefaultTenantHeader))
	}
	tenant := md.Get(tenancy.DefaultTenantHeader)[0]
	testutil.Assert(t, tenant == testTenant)
}

func (s *mockedStoreAPI) Info(context.Context, *storepb.InfoRequest, ...grpc.CallOption) (*storepb.InfoResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *mockedStoreAPI) Series(ctx context.Context, req *storepb.SeriesRequest, _ ...grpc.CallOption) (storepb.Store_SeriesClient, error) {
	getAndAssertTenant(ctx, s.t)

	return &storetestutil.StoreSeriesClient{Ctx: ctx, RespSet: s.RespSeries, RespDur: s.RespDuration, SlowSeriesIndex: s.SlowSeriesIndex}, s.RespError
}

func (s *mockedStoreAPI) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest, _ ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	getAndAssertTenant(ctx, s.t)

	return s.RespLabelNames, s.RespError
}

func (s *mockedStoreAPI) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest, _ ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	getAndAssertTenant(ctx, s.t)

	return s.RespLabelValues, s.RespError
}

func TestTenantFromGRPC(t *testing.T) {
	t.Run("tenant-present", func(t *testing.T) {

		ctx := context.Background()
		md := metadata.New(map[string]string{tenancy.DefaultTenantHeader: testTenant})
		ctx = metadata.NewIncomingContext(ctx, md)

		tenant, foundTenant := tenancy.GetTenantFromGRPCMetadata(ctx)
		testutil.Equals(t, true, foundTenant)
		testutil.Assert(t, tenant == testTenant)
	})
	t.Run("no-tenant", func(t *testing.T) {

		ctx := context.Background()

		tenant, foundTenant := tenancy.GetTenantFromGRPCMetadata(ctx)
		testutil.Equals(t, false, foundTenant)
		testutil.Assert(t, tenant == tenancy.DefaultTenant)
	})
}

func TestTenantProxyPassing(t *testing.T) {
	// When a querier requests info from a store
	// the tenant information is passed from the query api
	// to the proxy via context. This test ensures
	// proxy store correct places the tenant into the
	// outgoing grpc metadata
	t.Run("tenant-via-context", func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctx = context.WithValue(ctx, tenancy.TenantKey, testTenant)

		mockedStore := &mockedStoreAPI{
			RespLabelValues: &storepb.LabelValuesResponse{
				Values:   []string{"1", "2"},
				Warnings: []string{"warning"},
			},
			RespLabelNames: &storepb.LabelNamesResponse{
				Names: []string{"a", "b"},
			},
			t: t,
		}

		cls := []store.Client{
			&storetestutil.TestClient{StoreClient: mockedStore},
		}

		q := store.NewProxyStore(nil,
			nil,
			func() []store.Client { return cls },
			component.Query,
			nil, 0*time.Second, store.EagerRetrieval,
		)
		// We assert directly in the mocked store apis LabelValues/LabelNames/Series funcs
		_, _ = q.LabelValues(ctx, &storepb.LabelValuesRequest{})
		_, _ = q.LabelNames(ctx, &storepb.LabelNamesRequest{})

		seriesMatchers := []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
		}

		_ = q.Series(&storepb.SeriesRequest{Matchers: seriesMatchers}, &storeSeriesServer{ctx: ctx})
	})

	// In the case of nested queriers, the 2nd querier
	// will get the tenant via grpc metadata from the 1st querier.
	// This test ensures that the proxy store of the 2nd querier
	// correctly places the tenant information in the outgoing
	// grpc metadata to be sent to its stores.
	t.Run("tenant-via-grpc", func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		md := metadata.New(map[string]string{tenancy.DefaultTenantHeader: testTenant})
		ctx = metadata.NewIncomingContext(ctx, md)

		mockedStore := &mockedStoreAPI{
			RespLabelValues: &storepb.LabelValuesResponse{
				Values:   []string{"1", "2"},
				Warnings: []string{"warning"},
			},
			RespLabelNames: &storepb.LabelNamesResponse{
				Names: []string{"a", "b"},
			},
			t: t,
		}

		cls := []store.Client{
			&storetestutil.TestClient{StoreClient: mockedStore},
		}

		q := store.NewProxyStore(nil,
			nil,
			func() []store.Client { return cls },
			component.Query,
			nil, 0*time.Second, store.EagerRetrieval,
		)

		// We assert directly in the mocked store apis LabelValues/LabelNames/Series funcs
		_, _ = q.LabelValues(ctx, &storepb.LabelValuesRequest{})
		_, _ = q.LabelNames(ctx, &storepb.LabelNamesRequest{})

		seriesMatchers := []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
		}

		_ = q.Series(&storepb.SeriesRequest{Matchers: seriesMatchers}, &storeSeriesServer{ctx: ctx})
	})
}
