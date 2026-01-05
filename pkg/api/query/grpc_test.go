// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"context"
	"testing"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/logicalplan"
	equery "github.com/thanos-io/promql-engine/query"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/extpromql"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/store"
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
	err   error
	warns annotations.Annotations
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
	return queryStub{err: qs.err, warns: qs.warns}, nil
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
	return queryStub{err: qs.err, warns: qs.warns}, nil
}

type queryStub struct {
	promql.Query
	err   error
	warns annotations.Annotations
}

func (q queryStub) Close() {}

func (q queryStub) Exec(context.Context) *promql.Result {
	return &promql.Result{Err: q.err, Warnings: q.warns}
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
