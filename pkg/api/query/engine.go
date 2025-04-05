// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This package is a modified copy from
// github.com/prometheus/prometheus/web/api/v1@2121b4628baa7d9d9406aa468712a6a332e77aff.

package v1

import (
	"context"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/logutil"
)

type PromqlEngineType string

const (
	PromqlEnginePrometheus PromqlEngineType = "prometheus"
	PromqlEngineThanos     PromqlEngineType = "thanos"
)

type PromqlQueryMode string

const (
	PromqlQueryModeLocal       PromqlQueryMode = "local"
	PromqlQueryModeDistributed PromqlQueryMode = "distributed"
)

type queryCreator interface {
	makeInstantQuery(
		ctx context.Context,
		t PromqlEngineType,
		q storage.Queryable,
		e api.RemoteEndpoints,
		qry planOrQuery,
		opts *engine.QueryOpts,
		ts time.Time,
	) (res promql.Query, err error)
	makeRangeQuery(
		ctx context.Context,
		t PromqlEngineType,
		q storage.Queryable,
		e api.RemoteEndpoints,
		qry planOrQuery,
		opts *engine.QueryOpts,
		start time.Time,
		end time.Time,
		step time.Duration,
	) (res promql.Query, err error)
}

type QueryFactory struct {
	mode PromqlQueryMode

	prometheus        *promql.Engine
	thanosLocal       *engine.Engine
	thanosDistributed *engine.DistributedEngine
}

func NewQueryFactory(
	reg *prometheus.Registry,
	logger log.Logger,
	queryTimeout time.Duration,
	lookbackDelta time.Duration,
	evaluationInterval time.Duration,
	enableXFunctions bool,
	enableQueryExperimentalFunctions bool,
	activeQueryTracker *promql.ActiveQueryTracker,
	mode PromqlQueryMode,
) *QueryFactory {
	makeOpts := func(registry prometheus.Registerer) engine.Opts {
		opts := engine.Opts{
			EngineOpts: promql.EngineOpts{
				Logger: logutil.GoKitLogToSlog(logger),
				Reg:    registry,
				// TODO(bwplotka): Expose this as a flag: https://github.com/thanos-io/thanos/issues/703.
				MaxSamples:    math.MaxInt32,
				Timeout:       queryTimeout,
				LookbackDelta: lookbackDelta,
				NoStepSubqueryIntervalFn: func(int64) int64 {
					return evaluationInterval.Milliseconds()
				},
				EnableNegativeOffset: true,
				EnableAtModifier:     true,
			},
			EnableXFunctions:           		enableXFunctions,
			EnableQueryExperimentalFunctions:   enableQueryExperimentalFunctions,
			EnableAnalysis:              		true,
		}
		if activeQueryTracker != nil {
			opts.ActiveQueryTracker = activeQueryTracker
		}
		return opts
	}

	promEngine := promql.NewEngine(makeOpts(extprom.WrapRegistererWith(
		map[string]string{
			"mode":   string(PromqlQueryModeLocal),
			"engine": string(PromqlEnginePrometheus)}, reg)).EngineOpts)

	thanosLocal := engine.New(makeOpts(extprom.WrapRegistererWith(
		map[string]string{
			"mode":   string(PromqlQueryModeLocal),
			"engine": string(PromqlEngineThanos)}, reg)))
	thanosDistributed := engine.NewDistributedEngine(makeOpts(extprom.WrapRegistererWith(
		map[string]string{
			"mode":   string(PromqlQueryModeDistributed),
			"engine": string(PromqlEngineThanos)}, reg)))

	return &QueryFactory{
		mode:              mode,
		prometheus:        promEngine,
		thanosLocal:       thanosLocal,
		thanosDistributed: thanosDistributed,
	}
}

// Always has query, sometimes already has a plan.
type planOrQuery struct {
	query string
	plan  logicalplan.Node
}

func (f *QueryFactory) makeInstantQuery(
	ctx context.Context,
	t PromqlEngineType,
	q storage.Queryable,
	e api.RemoteEndpoints,
	qry planOrQuery,
	opts *engine.QueryOpts,
	ts time.Time,
) (res promql.Query, err error) {
	if t == PromqlEngineThanos && f.mode == PromqlQueryModeLocal {
		if qry.plan != nil {
			res, err = f.thanosLocal.MakeInstantQueryFromPlan(ctx, q, opts, qry.plan, ts)
		} else {
			res, err = f.thanosLocal.MakeInstantQuery(ctx, q, opts, qry.query, ts)
		}
		if err != nil {
			if engine.IsUnimplemented(err) {
				// fallback to prometheus
				return f.prometheus.NewInstantQuery(ctx, q, opts, qry.query, ts)
			}
			return nil, err
		}
		return res, nil
	}
	if t == PromqlEngineThanos && f.mode == PromqlQueryModeDistributed {
		if qry.plan != nil {
			res, err = f.thanosDistributed.MakeInstantQueryFromPlan(ctx, q, e, opts, qry.plan, ts)
		} else {
			res, err = f.thanosDistributed.MakeInstantQuery(ctx, q, e, opts, qry.query, ts)
		}
		if err != nil {
			if engine.IsUnimplemented(err) {
				// fallback to prometheus
				return f.prometheus.NewInstantQuery(ctx, q, opts, qry.query, ts)
			}
			return nil, err
		}
		return res, nil
	}
	return f.prometheus.NewInstantQuery(ctx, q, opts, qry.query, ts)
}

func (f *QueryFactory) makeRangeQuery(
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
	if t == PromqlEngineThanos && f.mode == PromqlQueryModeLocal {
		if qry.plan != nil {
			res, err = f.thanosLocal.MakeRangeQueryFromPlan(ctx, q, opts, qry.plan, start, end, step)
		} else {
			res, err = f.thanosLocal.MakeRangeQuery(ctx, q, opts, qry.query, start, end, step)
		}
		if err != nil {
			if engine.IsUnimplemented(err) {
				// fallback to prometheus
				return f.prometheus.NewRangeQuery(ctx, q, opts, qry.query, start, end, step)
			}
			return nil, err
		}
		return res, nil
	}
	if t == PromqlEngineThanos && f.mode == PromqlQueryModeDistributed {
		if qry.plan != nil {
			res, err = f.thanosDistributed.MakeRangeQueryFromPlan(ctx, q, e, opts, qry.plan, start, end, step)
		} else {
			res, err = f.thanosDistributed.MakeRangeQuery(ctx, q, e, opts, qry.query, start, end, step)
		}
		if err != nil {
			if engine.IsUnimplemented(err) {
				// fallback to prometheus
				return f.prometheus.NewRangeQuery(ctx, q, opts, qry.query, start, end, step)
			}
			return nil, err
		}
		return res, nil
	}
	return f.prometheus.NewRangeQuery(ctx, q, opts, qry.query, start, end, step)
}
