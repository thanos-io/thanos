// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	promstats "github.com/prometheus/prometheus/util/stats"
	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/logicalplan"
	equery "github.com/thanos-io/promql-engine/query"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/extpromql"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
)

func TestGRPCQueryAPIWithQueryPlan(t *testing.T) {
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	proxy := store.NewProxyStore(logger, reg, func() []store.Client { return nil }, component.Store, labels.EmptyLabels(), 1*time.Minute, store.LazyRetrieval)
	queryableCreator := query.NewQueryableCreator(logger, reg, proxy, 1, 1*time.Minute, dedup.AlgorithmPenalty, 1)
	remoteEndpointsCreator := query.NewRemoteEndpointsCreator(logger, func() []query.Client { return nil }, nil, 1*time.Minute, true, true)
	lookbackDeltaFunc := func(i int64) time.Duration { return 5 * time.Minute }
	api := NewGRPCAPI(time.Now, nil, queryableCreator, remoteEndpointsCreator, queryFactory, querypb.EngineType_thanos, lookbackDeltaFunc, 0)

	expr, err := extpromql.ParseExpr("metric")
	testutil.Ok(t, err)
	lplan, err := logicalplan.NewFromAST(expr, &equery.Options{}, logicalplan.PlanOptions{})
	testutil.Ok(t, err)
	// Create a mock query plan.
	planBytes, err := logicalplan.Marshal(lplan.Root())
	testutil.Ok(t, err)

	rangeRequest := &querypb.QueryRangeRequest{
		Query:            "metric",
		StartTimeSeconds: 0,
		IntervalSeconds:  10,
		EndTimeSeconds:   300,
		QueryPlan:        &querypb.QueryPlan{Encoding: &querypb.QueryPlan_Json{Json: planBytes}},
	}

	srv := newQueryRangeServer(context.Background())
	err = api.QueryRange(rangeRequest, srv)
	testutil.Ok(t, err)

	// must also handle without query plan.
	rangeRequest.QueryPlan = nil
	err = api.QueryRange(rangeRequest, srv)
	testutil.Ok(t, err)

	instantRequest := &querypb.QueryRequest{
		Query:          "metric",
		TimeoutSeconds: 60,
		QueryPlan:      &querypb.QueryPlan{Encoding: &querypb.QueryPlan_Json{Json: planBytes}},
	}
	instSrv := newQueryServer(context.Background())
	err = api.Query(instantRequest, instSrv)
	testutil.Ok(t, err)
}

func TestGRPCQueryAPIErrorHandling(t *testing.T) {
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	proxy := store.NewProxyStore(logger, reg, func() []store.Client { return nil }, component.Store, labels.EmptyLabels(), 1*time.Minute, store.LazyRetrieval)
	queryableCreator := query.NewQueryableCreator(logger, reg, proxy, 1, 1*time.Minute, dedup.AlgorithmPenalty, 1)
	remoteEndpointsCreator := query.NewRemoteEndpointsCreator(logger, func() []query.Client { return nil }, nil, 1*time.Minute, true, true)
	lookbackDeltaFunc := func(i int64) time.Duration { return 5 * time.Minute }
	tests := []struct {
		name         string
		queryCreator queryCreatorStub
	}{
		{
			name: "error response",
			queryCreator: queryCreatorStub{
				err: errors.New("error stub"),
			},
		},
		{
			name: "error response",
			queryCreator: queryCreatorStub{
				warns: annotations.New().Add(errors.New("warn stub")),
			},
		},
	}

	for _, test := range tests {
		api := NewGRPCAPI(time.Now, nil, queryableCreator, remoteEndpointsCreator, test.queryCreator, querypb.EngineType_prometheus, lookbackDeltaFunc, 0)
		t.Run("range_query", func(t *testing.T) {
			rangeRequest := &querypb.QueryRangeRequest{
				Query:            "metric",
				StartTimeSeconds: 0,
				IntervalSeconds:  10,
				EndTimeSeconds:   300,
			}
			srv := newQueryRangeServer(context.Background())
			err := api.QueryRange(rangeRequest, srv)

			if test.queryCreator.err != nil {
				testutil.NotOk(t, err)
				return
			}
			if len(test.queryCreator.warns) > 0 {
				testutil.Ok(t, err)
				for i, resp := range srv.responses {
					if resp.GetWarnings() != "" {
						testutil.Equals(t, test.queryCreator.warns.AsErrors()[i].Error(), resp.GetWarnings())
					}
				}
			}
		})

		t.Run("instant_query", func(t *testing.T) {
			instantRequest := &querypb.QueryRequest{
				Query:          "metric",
				TimeoutSeconds: 60,
			}
			srv := newQueryServer(context.Background())
			err := api.Query(instantRequest, srv)
			if test.queryCreator.err != nil {
				testutil.NotOk(t, err)
				return
			}
			if len(test.queryCreator.warns) > 0 {
				testutil.Ok(t, err)
				for i, resp := range srv.responses {
					if resp.GetWarnings() != "" {
						testutil.Equals(t, test.queryCreator.warns.AsErrors()[i].Error(), resp.GetWarnings())
					}
				}
			}
		})
	}
}

type queryCreatorStub struct {
	err    error
	warns  annotations.Annotations
	result parser.Value // promql.Vector or promql.Matrix; if set, returned as result value
}

func (qs queryCreatorStub) makeInstantQuery(
	ctx context.Context,
	t PromqlEngineType,
	q storage.Queryable,
	e api.RemoteEndpoints,
	qry planOrQuery,
	opts *engine.QueryOpts,
	ts time.Time,
) (res promql.Query, err error) {
	return queryStub{err: qs.err, warns: qs.warns, result: qs.result}, nil
}
func (qs queryCreatorStub) makeRangeQuery(
	ctx context.Context,
	t PromqlEngineType,
	q storage.Queryable,
	e api.RemoteEndpoints,
	qry planOrQuery,
	opts *engine.QueryOpts,
	start time.Time,
	end time.Time,
	step time.Duration,
) (res promql.Query, err error) {
	return queryStub{err: qs.err, warns: qs.warns, result: qs.result}, nil
}

type queryStub struct {
	promql.Query
	err    error
	warns  annotations.Annotations
	result parser.Value // promql.Vector or promql.Matrix
}

func (q queryStub) Close() {}

func (q queryStub) Exec(context.Context) *promql.Result {
	return &promql.Result{Err: q.err, Warnings: q.warns, Value: q.result}
}

func (q queryStub) Stats() *promstats.Statistics { return nil }

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

// statsQueryStub is a promql.Query whose Stats() returns a pre-populated
// *stats.Statistics so extractQueryStats can be tested without running the engine.
type statsQueryStub struct {
	promql.Query
	stats *promstats.Statistics
}

func (q statsQueryStub) Stats() *promstats.Statistics { return q.stats }
func (q statsQueryStub) Close()                       {}

func TestExtractQueryStats_Timings(t *testing.T) {
	timers := promstats.NewQueryTimers()

	for _, qt := range []promstats.QueryTiming{
		promstats.EvalTotalTime,
		promstats.ResultSortTime,
		promstats.QueryPreparationTime,
		promstats.InnerEvalTime,
		promstats.ExecQueueTime,
		promstats.ExecTotalTime,
	} {
		tm := timers.GetTimer(qt)
		tm.Start()
		time.Sleep(2 * time.Millisecond)
		tm.Stop()
	}

	qry := statsQueryStub{stats: &promstats.Statistics{Timers: timers}}
	got := extractQueryStats(qry)

	testutil.Assert(t, got.EvalTotalTimeMs > 0, "EvalTotalTimeMs should be > 0, got %d", got.EvalTotalTimeMs)
	testutil.Assert(t, got.ResultSortTimeMs > 0, "ResultSortTimeMs should be > 0, got %d", got.ResultSortTimeMs)
	testutil.Assert(t, got.QueryPreparationTimeMs > 0, "QueryPreparationTimeMs should be > 0, got %d", got.QueryPreparationTimeMs)
	testutil.Assert(t, got.InnerEvalTimeMs > 0, "InnerEvalTimeMs should be > 0, got %d", got.InnerEvalTimeMs)
	testutil.Assert(t, got.ExecQueueTimeMs > 0, "ExecQueueTimeMs should be > 0, got %d", got.ExecQueueTimeMs)
	testutil.Assert(t, got.ExecTotalTimeMs > 0, "ExecTotalTimeMs should be > 0, got %d", got.ExecTotalTimeMs)
}

// TestGRPCQueryAPIStatsOptIn verifies that QueryStats responses are only sent
// when the request opts in via EnableStats.
func TestGRPCQueryAPIStatsOptIn(t *testing.T) {
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	proxy := store.NewProxyStore(logger, reg, func() []store.Client { return nil }, component.Store, labels.EmptyLabels(), 1*time.Minute, store.LazyRetrieval)
	queryableCreator := query.NewQueryableCreator(logger, reg, proxy, 1, 1*time.Minute, dedup.AlgorithmPenalty, 1)
	remoteEndpointsCreator := query.NewRemoteEndpointsCreator(logger, func() []query.Client { return nil }, nil, 1*time.Minute, true, true)
	lookbackDeltaFunc := func(i int64) time.Duration { return 5 * time.Minute }

	hasStats := func(in any) bool {
		switch r := in.(type) {
		case []querypb.QueryResponse:
			for _, resp := range r {
				if resp.GetStats() != nil {
					return true
				}
			}
		case []querypb.QueryRangeResponse:
			for _, resp := range r {
				if resp.GetStats() != nil {
					return true
				}
			}
		}
		return false
	}

	for _, enable := range []bool{false, true} {
		t.Run(fmt.Sprintf("Query enable_stats=%v", enable), func(t *testing.T) {
			api := NewGRPCAPI(time.Now, nil, queryableCreator, remoteEndpointsCreator, queryCreatorStub{result: makeVector(1)}, querypb.EngineType_thanos, lookbackDeltaFunc, 0)
			srv := newQueryServer(context.Background())
			err := api.Query(&querypb.QueryRequest{Query: "metric", EnableStats: enable}, srv)
			testutil.Ok(t, err)
			testutil.Equals(t, enable, hasStats(srv.responses))
		})

		t.Run(fmt.Sprintf("QueryRange enable_stats=%v", enable), func(t *testing.T) {
			api := NewGRPCAPI(time.Now, nil, queryableCreator, remoteEndpointsCreator, queryCreatorStub{result: makeMatrix(1, 1)}, querypb.EngineType_thanos, lookbackDeltaFunc, 0)
			srv := newQueryRangeServer(context.Background())
			err := api.QueryRange(&querypb.QueryRangeRequest{
				Query:            "metric",
				StartTimeSeconds: 0,
				EndTimeSeconds:   300,
				IntervalSeconds:  10,
				EnableStats:      enable,
			}, srv)
			testutil.Ok(t, err)
			testutil.Equals(t, enable, hasStats(srv.responses))
		})
	}
}

// makeVector creates a test promql.Vector with the given number of samples.
func makeVector(numSamples int) promql.Vector {
	vector := make(promql.Vector, numSamples)
	for i := 0; i < numSamples; i++ {
		vector[i] = promql.Sample{
			Metric: labels.FromStrings(
				"__name__", "test_metric",
				"instance", "localhost:9090",
				"job", "test",
				"series_id", fmt.Sprintf("series_%d", i),
			),
			T: 1000,
			F: float64(i),
		}
	}
	return vector
}

// makeMatrix creates a test promql.Matrix with the given dimensions.
func makeMatrix(numSeries, samplesPerSeries int) promql.Matrix {
	matrix := make(promql.Matrix, numSeries)
	for i := 0; i < numSeries; i++ {
		floats := make([]promql.FPoint, samplesPerSeries)
		for j := 0; j < samplesPerSeries; j++ {
			floats[j] = promql.FPoint{T: int64(j * 1000), F: float64(j)}
		}
		matrix[i] = promql.Series{
			Metric: labels.FromStrings(
				"__name__", "test_metric",
				"instance", "localhost:9090",
				"job", "test",
				"series_id", fmt.Sprintf("series_%d", i),
			),
			Floats: floats,
		}
	}
	return matrix
}

func BenchmarkQueryGRPCBatching(b *testing.B) {
	seriesCounts := []int{100, 1000, 10000}
	batchSizes := []int64{1, 10, 64, 100}

	for _, seriesCount := range seriesCounts {
		b.Run(fmt.Sprintf("series=%d", seriesCount), func(b *testing.B) {
			for _, batchSize := range batchSizes {
				b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
					benchmarkQueryGRPCBatching(b, seriesCount, batchSize)
				})
			}
		})
	}
}

func benchmarkQueryGRPCBatching(b *testing.B, seriesCount int, batchSize int64) {
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	proxy := store.NewProxyStore(logger, reg, func() []store.Client { return nil }, component.Store, labels.EmptyLabels(), 1*time.Minute, store.LazyRetrieval)
	queryableCreator := query.NewQueryableCreator(logger, reg, proxy, 1, 1*time.Minute, dedup.AlgorithmPenalty, 1)
	remoteEndpointsCreator := query.NewRemoteEndpointsCreator(logger, func() []query.Client { return nil }, nil, 1*time.Minute, true, true)
	lookbackDeltaFunc := func(i int64) time.Duration { return 5 * time.Minute }

	qc := queryCreatorStub{result: makeVector(seriesCount)}
	api := NewGRPCAPI(time.Now, nil, queryableCreator, remoteEndpointsCreator, qc, querypb.EngineType_thanos, lookbackDeltaFunc, 0)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		srv := newQueryServer(context.Background())
		request := &querypb.QueryRequest{
			Query:             "test_metric",
			TimeoutSeconds:    60,
			ResponseBatchSize: batchSize,
		}
		err := api.Query(request, srv)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryRangeGRPCBatching(b *testing.B) {
	seriesCounts := []int{100, 1000, 10000}
	samplesPerSeriesCounts := []int{100, 240}
	batchSizes := []int64{1, 10, 64, 100}

	for _, seriesCount := range seriesCounts {
		b.Run(fmt.Sprintf("series=%d", seriesCount), func(b *testing.B) {
			for _, samplesPerSeries := range samplesPerSeriesCounts {
				b.Run(fmt.Sprintf("samples=%d", samplesPerSeries), func(b *testing.B) {
					for _, batchSize := range batchSizes {
						b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
							benchmarkQueryRangeGRPCBatching(b, seriesCount, samplesPerSeries, batchSize)
						})
					}
				})
			}
		})
	}
}

func benchmarkQueryRangeGRPCBatching(b *testing.B, seriesCount, samplesPerSeries int, batchSize int64) {
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	proxy := store.NewProxyStore(logger, reg, func() []store.Client { return nil }, component.Store, labels.EmptyLabels(), 1*time.Minute, store.LazyRetrieval)
	queryableCreator := query.NewQueryableCreator(logger, reg, proxy, 1, 1*time.Minute, dedup.AlgorithmPenalty, 1)
	remoteEndpointsCreator := query.NewRemoteEndpointsCreator(logger, func() []query.Client { return nil }, nil, 1*time.Minute, true, true)
	lookbackDeltaFunc := func(i int64) time.Duration { return 5 * time.Minute }

	qc := queryCreatorStub{result: makeMatrix(seriesCount, samplesPerSeries)}
	api := NewGRPCAPI(time.Now, nil, queryableCreator, remoteEndpointsCreator, qc, querypb.EngineType_thanos, lookbackDeltaFunc, 0)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		srv := newQueryRangeServer(context.Background())
		request := &querypb.QueryRangeRequest{
			Query:             "test_metric",
			StartTimeSeconds:  0,
			EndTimeSeconds:    300,
			IntervalSeconds:   10,
			TimeoutSeconds:    60,
			ResponseBatchSize: batchSize,
		}
		err := api.QueryRange(request, srv)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestGRPCQueryAPIMaxResolutionConversion verifies that MaxResolutionSeconds (in seconds)
// from the gRPC request is correctly converted to milliseconds before being forwarded to
// the store as MaxResolutionWindow.
func TestGRPCQueryAPIMaxResolutionConversion(t *testing.T) {
	const resolutionSeconds = int64(300)
	const expectedMillis = resolutionSeconds * 1000

	captureStore := &resolutionCapturingStore{}

	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	proxy := store.NewProxyStore(
		logger, reg,
		func() []store.Client {
			return []store.Client{
				&storetestutil.TestClient{
					Name:        "test",
					StoreClient: storepb.ServerAsClient(captureStore, atomic.Bool{}),
					MinTime:     math.MinInt64,
					MaxTime:     math.MaxInt64,
				},
			}
		},
		component.Store,
		labels.EmptyLabels(),
		1*time.Minute,
		store.EagerRetrieval,
	)
	queryableCreator := query.NewQueryableCreator(logger, reg, proxy, 1, 1*time.Minute, dedup.AlgorithmPenalty, 1)
	remoteEndpointsCreator := query.NewRemoteEndpointsCreator(logger, func() []query.Client { return nil }, nil, 1*time.Minute, true, true)
	lookbackDeltaFunc := func(i int64) time.Duration { return 5 * time.Minute }
	grpcAPI := NewGRPCAPI(time.Now, nil, queryableCreator, remoteEndpointsCreator, queryFactory, querypb.EngineType_thanos, lookbackDeltaFunc, 0)

	t.Run("QueryRange", func(t *testing.T) {
		captureStore.capturedMaxResolution = 0
		err := grpcAPI.QueryRange(&querypb.QueryRangeRequest{
			Query:                "metric",
			StartTimeSeconds:     0,
			IntervalSeconds:      10,
			EndTimeSeconds:       300,
			MaxResolutionSeconds: resolutionSeconds,
		}, newQueryRangeServer(context.Background()))
		testutil.Ok(t, err)
		testutil.Equals(t, expectedMillis, captureStore.capturedMaxResolution)
	})

	t.Run("Query", func(t *testing.T) {
		captureStore.capturedMaxResolution = 0
		err := grpcAPI.Query(&querypb.QueryRequest{
			Query:                "metric",
			MaxResolutionSeconds: resolutionSeconds,
		}, newQueryServer(context.Background()))
		testutil.Ok(t, err)
		testutil.Equals(t, expectedMillis, captureStore.capturedMaxResolution)
	})
}

type resolutionCapturingStore struct {
	storepb.UnimplementedStoreServer
	capturedMaxResolution int64
}

func (s *resolutionCapturingStore) Series(req *storepb.SeriesRequest, _ storepb.Store_SeriesServer) error {
	s.capturedMaxResolution = req.MaxResolutionWindow
	return nil
}
