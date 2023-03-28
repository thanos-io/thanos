// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	"github.com/prometheus/common/model"

	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestDistributedQueryExecution(t *testing.T) {
	t.Parallel()

	// Build up.
	e, err := e2e.New(e2e.WithName("dist-query"))
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "prom1", e2ethanos.DefaultPromConfig("prom1", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "prom2", e2ethanos.DefaultPromConfig("prom2", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, prom2, sidecar1, sidecar2))

	qry1 := e2ethanos.NewQuerierBuilder(e, "1").WithStrictEndpoints(sidecar1.InternalEndpoint("grpc")).Init()
	qry2 := e2ethanos.NewQuerierBuilder(e, "2").WithStrictEndpoints(sidecar2.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(qry1, qry2))

	qryEndpoints := []string{qry1.InternalEndpoint("grpc"), qry2.InternalEndpoint("grpc")}
	fanoutQry := e2ethanos.NewQuerierBuilder(e, "3").WithStrictEndpoints(qryEndpoints...).Init()
	distQry := e2ethanos.NewQuerierBuilder(e, "4").WithStrictEndpoints(qryEndpoints...).
		WithEngine("thanos").
		WithQueryMode("distributed").
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(fanoutQry))
	testutil.Ok(t, e2e.StartAndWaitReady(distQry))

	// Use current time to make debugging through UI easier.
	now := time.Now()
	nowFunc := func() time.Time { return now }
	timeOffset := nowFunc().UnixNano()
	samples := []fakeMetricSample{
		{"i1", 1, timeOffset + 30*1000*1000}, {"i1", 2, timeOffset + 60*1000*1000}, {"i1", 3, timeOffset + 90*1000*1000},
		{"i2", 5, timeOffset + 30*1000*1000}, {"i2", 6, timeOffset + 60*1000*1000}, {"i2", 7, timeOffset + 90*1000*1000},
		{"i3", 9, timeOffset + 30*1000*1000}, {"i3", 10, timeOffset + 60*1000*1000}, {"i3", 11, timeOffset + 90*1000*1000},
	}
	ctx := context.Background()
	testutil.Ok(t, synthesizeFakeMetricSamples(ctx, prom1, samples))
	testutil.Ok(t, synthesizeFakeMetricSamples(ctx, prom2, samples))

	queryFunc := func() string { return "sum(sum_over_time(my_fake_metric[2m]))" }
	queryOpts := promclient.QueryOptions{
		Deduplicate: true,
	}

	// Test range query.
	var fanoutQryRangeResult, distQryRangeResult model.Matrix
	rangeQuery(t, ctx, fanoutQry.Endpoint("http"), queryFunc, nowFunc().UnixMilli(), nowFunc().Add(5*time.Minute).UnixMilli(), 30, queryOpts, func(res model.Matrix) error {
		fanoutQryRangeResult = res
		return nil
	})
	rangeQuery(t, ctx, fanoutQry.Endpoint("http"), queryFunc, nowFunc().UnixMilli(), nowFunc().Add(5*time.Minute).UnixMilli(), 30, queryOpts, func(res model.Matrix) error {
		distQryRangeResult = res
		return nil
	})
	testutil.Assert(t, len(fanoutQryRangeResult) != 0)
	testutil.Assert(t, len(distQryRangeResult) != 0)
	testutil.Equals(t, fanoutQryRangeResult, distQryRangeResult)

	// Test instant query.
	queryAndAssert(t, ctx, distQry.Endpoint("http"), queryFunc, nowFunc, queryOpts, model.Vector{
		{
			Metric:    map[model.LabelName]model.LabelValue{},
			Value:     30,
			Timestamp: 0,
		},
	})
}
