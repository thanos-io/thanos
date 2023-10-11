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
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/store"
)

func TestGRPCQueryAPIErrorHandling(t *testing.T) {
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	proxy := store.NewProxyStore(logger, reg, func() []store.Client { return nil }, component.Store, nil, 1*time.Minute, store.LazyRetrieval)
	queryableCreator := query.NewQueryableCreator(logger, reg, proxy, 1, 1*time.Minute)
	lookbackDeltaFunc := func(i int64) time.Duration { return 5 * time.Minute }
	tests := []struct {
		name   string
		engine *engineStub
	}{
		{
			name: "error response",
			engine: &engineStub{
				err: errors.New("error stub"),
			},
		},
		{
			name: "error response",
			engine: &engineStub{
				warns: annotations.New().Add(errors.New("warn stub")),
			},
		},
	}

	for _, test := range tests {
		engineFactory := &QueryEngineFactory{
			prometheusEngine: test.engine,
		}
		api := NewGRPCAPI(time.Now, nil, queryableCreator, engineFactory, querypb.EngineType_prometheus, lookbackDeltaFunc, 0)
		t.Run("range_query", func(t *testing.T) {
			rangeRequest := &querypb.QueryRangeRequest{
				Query:            "metric",
				StartTimeSeconds: 0,
				IntervalSeconds:  10,
				EndTimeSeconds:   300,
			}
			srv := newQueryRangeServer(context.Background())
			err := api.QueryRange(rangeRequest, srv)

			if test.engine.err != nil {
				testutil.NotOk(t, err)
				return
			}
			if len(test.engine.warns) > 0 {
				testutil.Ok(t, err)
				for i, resp := range srv.responses {
					testutil.Equals(t, test.engine.warns.AsErrors()[i].Error(), resp.GetWarnings())
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
			if test.engine.err != nil {
				testutil.NotOk(t, err)
				return
			}
			if len(test.engine.warns) > 0 {
				testutil.Ok(t, err)
				for i, resp := range srv.responses {
					testutil.Equals(t, test.engine.warns.AsErrors()[i].Error(), resp.GetWarnings())
				}
			}
		})
	}
}

type engineStub struct {
	v1.QueryEngine
	err   error
	warns annotations.Annotations
}

func (e engineStub) NewInstantQuery(_ context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	return &queryStub{err: e.err, warns: e.warns}, nil
}

func (e engineStub) NewRangeQuery(_ context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	return &queryStub{err: e.err, warns: e.warns}, nil
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
