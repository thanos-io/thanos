// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

type GRPCAPI struct {
	now                         func() time.Time
	replicaLabels               []string
	queryableCreate             query.QueryableCreator
	queryEngine                 v1.QueryEngine
	lookbackDeltaCreate         func(int64) time.Duration
	defaultMaxResolutionSeconds time.Duration
}

func NewGRPCAPI(
	now func() time.Time,
	replicaLabels []string,
	creator query.QueryableCreator,
	queryEngine v1.QueryEngine,
	lookbackDeltaCreate func(int64) time.Duration,
	defaultMaxResolutionSeconds time.Duration,
) *GRPCAPI {
	return &GRPCAPI{
		now:                         now,
		replicaLabels:               replicaLabels,
		queryableCreate:             creator,
		queryEngine:                 queryEngine,
		lookbackDeltaCreate:         lookbackDeltaCreate,
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

	lookbackDelta := g.lookbackDeltaCreate(maxResolution * 1000)
	if request.LookbackDeltaSeconds > 0 {
		lookbackDelta = time.Duration(request.LookbackDeltaSeconds) * time.Second
	}

	storeMatchers, err := querypb.StoreMatchersToLabelMatchers(request.StoreMatchers)
	if err != nil {
		return err
	}

	replicaLabels := g.replicaLabels
	if len(request.ReplicaLabels) != 0 {
		replicaLabels = request.ReplicaLabels
	}

	queryable := g.queryableCreate(
		request.EnableDedup,
		replicaLabels,
		storeMatchers,
		maxResolution,
		request.EnablePartialResponse,
		request.EnableQueryPushdown,
		false,
		request.ShardInfo,
		query.NoopSeriesStatsReporter,
	)
	qry, err := g.queryEngine.NewInstantQuery(queryable, &promql.QueryOpts{LookbackDelta: lookbackDelta}, request.Query, ts)
	if err != nil {
		return err
	}
	defer qry.Close()

	result := qry.Exec(ctx)
	if result.Err != nil {
		return status.Error(codes.Aborted, result.Err.Error())
	}

	if len(result.Warnings) != 0 {
		if err := server.Send(querypb.NewQueryWarningsResponse(result.Warnings...)); err != nil {
			return err
		}
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
			floats, histograms := prompb.SamplesFromPromqlPoints(sample.Point)
			series := &prompb.TimeSeries{
				Labels:     labelpb.ZLabelsFromPromLabels(sample.Metric),
				Samples:    floats,
				Histograms: histograms,
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
		timeout := time.Duration(request.TimeoutSeconds) * time.Second
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	maxResolution := request.MaxResolutionSeconds
	if request.MaxResolutionSeconds == 0 {
		maxResolution = g.defaultMaxResolutionSeconds.Milliseconds() / 1000
	}

	lookbackDelta := g.lookbackDeltaCreate(maxResolution * 1000)
	if request.LookbackDeltaSeconds > 0 {
		lookbackDelta = time.Duration(request.LookbackDeltaSeconds) * time.Second
	}

	storeMatchers, err := querypb.StoreMatchersToLabelMatchers(request.StoreMatchers)
	if err != nil {
		return err
	}

	replicaLabels := g.replicaLabels
	if len(request.ReplicaLabels) != 0 {
		replicaLabels = request.ReplicaLabels
	}

	queryable := g.queryableCreate(
		request.EnableDedup,
		replicaLabels,
		storeMatchers,
		maxResolution,
		request.EnablePartialResponse,
		request.EnableQueryPushdown,
		false,
		request.ShardInfo,
		query.NoopSeriesStatsReporter,
	)

	startTime := time.Unix(request.StartTimeSeconds, 0)
	endTime := time.Unix(request.EndTimeSeconds, 0)
	interval := time.Duration(request.IntervalSeconds) * time.Second

	qry, err := g.queryEngine.NewRangeQuery(queryable, &promql.QueryOpts{LookbackDelta: lookbackDelta}, request.Query, startTime, endTime, interval)
	if err != nil {
		return err
	}
	defer qry.Close()

	result := qry.Exec(ctx)
	if result.Err != nil {
		return status.Error(codes.Aborted, result.Err.Error())
	}

	if len(result.Warnings) != 0 {
		if err := srv.Send(querypb.NewQueryRangeWarningsResponse(result.Warnings...)); err != nil {
			return err
		}
	}

	switch value := result.Value.(type) {
	case promql.Matrix:
		for _, series := range value {
			floats, histograms := prompb.SamplesFromPromqlPoints(series.Points...)
			series := &prompb.TimeSeries{
				Labels:     labelpb.ZLabelsFromPromLabels(series.Metric),
				Samples:    floats,
				Histograms: histograms,
			}
			if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
				return err
			}
		}
	case promql.Vector:
		for _, sample := range value {
			point := promql.Point{T: sample.T, V: sample.V, H: sample.H}
			floats, histograms := prompb.SamplesFromPromqlPoints(point)
			series := &prompb.TimeSeries{
				Labels:     labelpb.ZLabelsFromPromLabels(sample.Metric),
				Samples:    floats,
				Histograms: histograms,
			}
			if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
				return err
			}
		}
		return nil
	case promql.Scalar:
		series := &prompb.TimeSeries{
			Samples: []prompb.Sample{{Value: value.V, Timestamp: value.T}},
		}
		return srv.Send(querypb.NewQueryRangeResponse(series))
	}

	return nil
}
