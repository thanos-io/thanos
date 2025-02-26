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
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/logicalplan"
)

type Engine interface {
	MakeInstantQuery(ctx context.Context, q storage.Queryable, opts *engine.QueryOpts, qs string, ts time.Time) (promql.Query, error)
	MakeRangeQuery(ctx context.Context, q storage.Queryable, opts *engine.QueryOpts, qs string, start, end time.Time, step time.Duration) (promql.Query, error)
	MakeInstantQueryFromPlan(ctx context.Context, q storage.Queryable, opts *engine.QueryOpts, plan logicalplan.Node, ts time.Time) (promql.Query, error)
	MakeRangeQueryFromPlan(ctx context.Context, q storage.Queryable, opts *engine.QueryOpts, root logicalplan.Node, start, end time.Time, step time.Duration) (promql.Query, error)
}

type prometheusEngineAdapter struct {
	engine promql.QueryEngine
}

func (a *prometheusEngineAdapter) MakeInstantQuery(ctx context.Context, q storage.Queryable, opts *engine.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	return a.engine.NewInstantQuery(ctx, q, opts, qs, ts)
}

func (a *prometheusEngineAdapter) MakeRangeQuery(ctx context.Context, q storage.Queryable, opts *engine.QueryOpts, qs string, start, end time.Time, step time.Duration) (promql.Query, error) {
	return a.engine.NewRangeQuery(ctx, q, opts, qs, start, end, step)
}

func (a *prometheusEngineAdapter) MakeInstantQueryFromPlan(ctx context.Context, q storage.Queryable, opts *engine.QueryOpts, plan logicalplan.Node, ts time.Time) (promql.Query, error) {
	return a.engine.NewInstantQuery(ctx, q, opts, plan.String(), ts)
}

func (a *prometheusEngineAdapter) MakeRangeQueryFromPlan(ctx context.Context, q storage.Queryable, opts *engine.QueryOpts, plan logicalplan.Node, start, end time.Time, step time.Duration) (promql.Query, error) {
	return a.engine.NewRangeQuery(ctx, q, opts, plan.String(), start, end, step)
}

type QueryFactory struct {
	prometheus Engine
	thanos     Engine
}

func NewQueryFactory(
	engineOpts engine.Opts,
	remoteEngineEndpoints api.RemoteEndpoints,
) *QueryFactory {

	var thanosEngine Engine
	if remoteEngineEndpoints == nil {
		thanosEngine = engine.New(engineOpts)
	} else {
		thanosEngine = engine.NewDistributedEngine(engineOpts, remoteEngineEndpoints)
	}
	promEngine := promql.NewEngine(engineOpts.EngineOpts)
	return &QueryFactory{
		prometheus: &prometheusEngineAdapter{promEngine},
		thanos:     thanosEngine,
	}
}

// Always has query, sometimes already has a plan.
type planOrQuery struct {
	query string
	plan  logicalplan.Node
}

func (f *QueryFactory) makeInstantQuery(
	ctx context.Context,
	e PromqlEngineType,
	q storage.Queryable,
	qry planOrQuery,
	opts *engine.QueryOpts,
	ts time.Time,
) (res promql.Query, err error) {
	if e == PromqlEngineThanos {
		if qry.plan != nil {
			res, err = f.thanos.MakeInstantQueryFromPlan(ctx, q, opts, qry.plan, ts)
		} else {
			res, err = f.thanos.MakeInstantQuery(ctx, q, opts, qry.query, ts)
		}
		if err != nil {
			if engine.IsUnimplemented(err) {
				goto fallback
			}
			return nil, err
		}
		return res, nil
	}
fallback:
	return f.prometheus.MakeInstantQuery(ctx, q, opts, qry.query, ts)
}

func (f *QueryFactory) makeRangeQuery(
	ctx context.Context,
	e PromqlEngineType,
	q storage.Queryable,
	qry planOrQuery,
	opts *engine.QueryOpts,
	start time.Time,
	end time.Time,
	step time.Duration,
) (res promql.Query, err error) {
	if e == PromqlEngineThanos {
		if qry.plan != nil {
			res, err = f.thanos.MakeRangeQueryFromPlan(ctx, q, opts, qry.plan, start, end, step)
		} else {
			res, err = f.thanos.MakeRangeQuery(ctx, q, opts, qry.query, start, end, step)
		}
		if err != nil {
			if engine.IsUnimplemented(err) {
				goto fallback
			}
			return nil, err
		}
		return res, nil
	}
fallback:
	return f.prometheus.MakeRangeQuery(ctx, q, opts, qry.query, start, end, step)
}
