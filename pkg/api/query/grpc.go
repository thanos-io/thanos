// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"google.golang.org/grpc"
)

type GRPCAPI struct {
	now                         func() time.Time
	replicaLabels               []string
	queryableCreate             query.QueryableCreator
	queryEngine                 func(int64) *promql.Engine
	defaultMaxResolutionSeconds time.Duration
}

func NewGRPCAPI(now func() time.Time, replicaLabels []string, creator query.QueryableCreator, queryEngine func(int64) *promql.Engine, defaultMaxResolutionSeconds time.Duration) *GRPCAPI {
	return &GRPCAPI{
		now:                         now,
		replicaLabels:               replicaLabels,
		queryableCreate:             creator,
		queryEngine:                 queryEngine,
		defaultMaxResolutionSeconds: defaultMaxResolutionSeconds,
	}
}

func RegisterQueryServer(queryServer querypb.QueryServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		querypb.RegisterQueryServer(s, queryServer)
	}
}

func (g *GRPCAPI) Query(request *querypb.QueryRequest, server querypb.Query_QueryServer) error {
	ctx := server.Context()
	var ts time.Time
	if request.TimeSeconds == 0 {
		ts = g.now()
	} else {
		ts = time.Unix(request.TimeSeconds, 0)
	}

	if request.TimeoutSeconds != 0 {
		var cancel context.CancelFunc
		timeout := time.Duration(request.TimeoutSeconds) * time.Second
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	maxResolution := request.MaxResolutionSeconds
	if request.MaxResolutionSeconds == 0 {
		maxResolution = g.defaultMaxResolutionSeconds.Milliseconds() / 1000
	}

	storeMatchers, err := querypb.StoreMatchersToLabelMatchers(request.StoreMatchers)
	if err != nil {
		return err
	}

	replicaLabels := g.replicaLabels
	if len(request.ReplicaLabels) != 0 {
		replicaLabels = request.ReplicaLabels
	}
	qe := g.queryEngine(request.MaxResolutionSeconds)
	queryable := g.queryableCreate(
		request.EnableDedup,
		replicaLabels,
		storeMatchers,
		maxResolution,
		request.EnablePartialResponse,
		request.EnableQueryPushdown,
		false,
	)
	qry, err := qe.NewInstantQuery(queryable, &promql.QueryOpts{}, request.Query, ts)
	if err != nil {
		return err
	}
	defer qry.Close()

	result := qry.Exec(ctx)
	if err := server.Send(querypb.NewQueryWarningsResponse(result.Warnings)); err != nil {
		return nil
	}

	switch vector := result.Value.(type) {
	case promql.Scalar:
		series := &prompb.TimeSeries{
			Samples: []prompb.Sample{{Value: vector.V, Timestamp: vector.T}},
		}
		if err := server.Send(querypb.NewQueryResponse(series)); err != nil {
			return err
		}
	case promql.Vector:
		for _, sample := range vector {
			series := &prompb.TimeSeries{
				Labels:  labelpb.ZLabelsFromPromLabels(sample.Metric),
				Samples: prompb.SamplesFromPromqlPoints([]promql.Point{sample.Point}),
			}
			if err := server.Send(querypb.NewQueryResponse(series)); err != nil {
				return err
			}
		}

		return nil
	}

	return nil
}

func (g *GRPCAPI) QueryRange(request *querypb.QueryRangeRequest, srv querypb.Query_QueryRangeServer) error {
	ctx := srv.Context()
	if request.TimeoutSeconds != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(request.TimeoutSeconds))
		defer cancel()
	}

	maxResolution := request.MaxResolutionSeconds
	if request.MaxResolutionSeconds == 0 {
		maxResolution = g.defaultMaxResolutionSeconds.Milliseconds() / 1000
	}

	storeMatchers, err := querypb.StoreMatchersToLabelMatchers(request.StoreMatchers)
	if err != nil {
		return err
	}

	replicaLabels := g.replicaLabels
	if len(request.ReplicaLabels) != 0 {
		replicaLabels = request.ReplicaLabels
	}
	qe := g.queryEngine(request.MaxResolutionSeconds)
	queryable := g.queryableCreate(
		request.EnableDedup,
		replicaLabels,
		storeMatchers,
		maxResolution,
		request.EnablePartialResponse,
		request.EnableQueryPushdown,
		false,
	)

	startTime := time.Unix(request.StartTimeSeconds, 0)
	endTime := time.Unix(request.EndTimeSeconds, 0)
	interval := time.Duration(request.IntervalSeconds) * time.Second

	qry, err := qe.NewRangeQuery(queryable, &promql.QueryOpts{}, request.Query, startTime, endTime, interval)
	if err != nil {
		return err
	}
	defer qry.Close()

	result := qry.Exec(ctx)
	if err := srv.Send(querypb.NewQueryRangeWarningsResponse(result.Warnings)); err != nil {
		return err
	}

	switch matrix := result.Value.(type) {
	case promql.Matrix:
		for _, series := range matrix {
			series := &prompb.TimeSeries{
				Labels:  labelpb.ZLabelsFromPromLabels(series.Metric),
				Samples: prompb.SamplesFromPromqlPoints(series.Points),
			}
			if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
				return err
			}
		}

		return nil
	}

	return nil
}
