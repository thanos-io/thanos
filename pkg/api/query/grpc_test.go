package v1

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
)

func TestGRPCQueryAPIErrorHandling(t *testing.T) {
	proxy := store.NewProxyStore(log.NewNopLogger(), prometheus.NewRegistry(), func() []store.Client {
		client := storepb.ServerAsClient(&storepb.UnimplementedStoreServer{}, 1)
		return []store.Client{storetestutil.TestClient{
			StoreClient: client,
			MinTime:     math.MinInt64,
			MaxTime:     math.MaxInt64,
		}}
	}, component.Store, nil, 1*time.Minute, store.LazyRetrieval)

	queryableCreator := query.NewQueryableCreator(log.NewNopLogger(), prometheus.NewRegistry(), proxy, 1, 1*time.Minute)
	engine := promql.NewEngine(promql.EngineOpts{
		Reg:           prometheus.NewRegistry(),
		MaxSamples:    math.MaxInt64,
		Timeout:       1 * time.Minute,
		LookbackDelta: 5 * time.Minute,
	})
	lookbackDeltaFunc := func(i int64) time.Duration { return 5 * time.Minute }
	api := NewGRPCAPI(time.Now, nil, queryableCreator, engine, lookbackDeltaFunc, 0)

	t.Run("range_query", func(t *testing.T) {
		rangeRequest := &querypb.QueryRangeRequest{
			Query:            "metric",
			StartTimeSeconds: 0,
			IntervalSeconds:  10,
			EndTimeSeconds:   300,
		}
		srv := newQueryRangeServer(context.Background())
		testutil.NotOk(t, api.QueryRange(rangeRequest, srv))
	})

	t.Run("instant_query", func(t *testing.T) {
		instantRequest := &querypb.QueryRequest{
			Query:          "metric",
			TimeoutSeconds: 60,
		}
		srv := newQueryServer(context.Background())
		testutil.NotOk(t, api.Query(instantRequest, srv))
	})
}

type queryServer struct {
	querypb.Query_QueryServer

	ctx       context.Context
	responses []querypb.QueryResponse
}

func newQueryServer(ctx context.Context) *queryServer {
	return &queryServer{ctx: ctx}
}

func (q *queryServer) Send(r *querypb.QueryResponse) error {
	q.responses = append(q.responses, *r)
	return nil
}

func (q *queryServer) Context() context.Context {
	return q.ctx
}

type queryRangeServer struct {
	querypb.Query_QueryRangeServer

	ctx       context.Context
	responses []querypb.QueryRangeResponse
}

func newQueryRangeServer(ctx context.Context) *queryRangeServer {
	return &queryRangeServer{ctx: ctx}
}

func (q *queryRangeServer) Send(r *querypb.QueryRangeResponse) error {
	q.responses = append(q.responses, *r)
	return nil
}

func (q *queryRangeServer) Context() context.Context {
	return q.ctx
}
