// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	"github.com/thanos-io/objstore/providers/s3"
	v1 "github.com/thanos-io/thanos/pkg/api/query"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
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

func TestDistributedEngineWithOverlappingIntervalsEnabled(t *testing.T) {
	t.Parallel()

	e, err := e2e.New(e2e.WithName("dist-disj-tsdbs"))
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	ctx := context.Background()
	l := log.NewLogfmtLogger(os.Stdout)
	now := time.Now()

	bucket1 := "dist-disj-tsdbs-test1"
	minio1 := e2edb.NewMinio(e, "1", bucket1, e2edb.WithMinioTLS())
	testutil.Ok(t, e2e.StartAndWaitReady(minio1))

	bkt1, err := s3.NewBucketWithConfig(l, e2ethanos.NewS3Config(bucket1, minio1.Endpoint("http"), minio1.Dir()), "test", nil)
	testutil.Ok(t, err)

	// Setup a storage GW with 2 blocks that have a gap to trigger distributed query MinT bug
	dir1 := filepath.Join(e.SharedDir(), "tmp1")
	testutil.Ok(t, os.MkdirAll(filepath.Join(e.SharedDir(), dir1), os.ModePerm))
	blockID1, err := e2eutil.CreateBlockWithBlockDelay(ctx,
		dir1,
		[]labels.Labels{labels.FromStrings("__name__", "foo", "instance", "foo_1")},
		1000,
		timestamp.FromTime(now.Add(-10*time.Hour)),
		timestamp.FromTime(now.Add(-8*time.Hour)),
		30*time.Minute,
		labels.FromStrings("prometheus", "p1", "replica", "0"),
		0,
		metadata.NoneFunc,
	)
	testutil.Ok(t, err)
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt1, path.Join(dir1, blockID1.String()), blockID1.String()))

	blockID2, err := e2eutil.CreateBlockWithBlockDelay(ctx,
		dir1,
		[]labels.Labels{labels.FromStrings("__name__", "foo", "instance", "foo_1")},
		1000,
		timestamp.FromTime(now.Add(-4*time.Hour)),
		timestamp.FromTime(now.Add(-2*time.Hour)),
		30*time.Minute,
		labels.FromStrings("prometheus", "p1", "replica", "0"),
		0,
		metadata.NoneFunc,
	)
	testutil.Ok(t, err)
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt1, path.Join(dir1, blockID2.String()), blockID2.String()))
	store1 := e2ethanos.NewStoreGW(
		e,
		"s1",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket1, minio1.InternalEndpoint("http"), minio1.InternalDir()),
		},
		"",
		"",
		nil,
	)
	testutil.Ok(t, e2e.StartAndWaitReady(store1))

	querierLeaf1 := e2ethanos.NewQuerierBuilder(e, "1", store1.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querierLeaf1))
	// We need another querier to circumvent the passthrough optimizer
	promConfig2 := e2ethanos.DefaultPromConfig("p2", 0, "", "", e2ethanos.LocalPrometheusTarget)
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "p2", promConfig2, "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom2, sidecar2))
	querierLeaf2 := e2ethanos.NewQuerierBuilder(e, "2", sidecar2.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querierLeaf2))
	querierDistributed := e2ethanos.NewQuerierBuilder(e, "3",
		querierLeaf1.InternalEndpoint("grpc"),
		querierLeaf2.InternalEndpoint("grpc"),
	).
		WithEngine(v1.PromqlEngineThanos).
		WithQueryMode("distributed").
		WithDistributedOverlap(true).
		Init()

	testutil.Ok(t, e2e.StartAndWaitReady(querierDistributed))

	// We would expect 2x2h ranges for the 2 blocks containing foo samples. That would be around 240 expected sample pairs in the result matrix.
	// We assert on more then 200 to reduce flakiness
	rangeQuery(t, ctx, querierDistributed.Endpoint("http"), func() string { return "foo" }, timestamp.FromTime(now.Add(-24*time.Hour)), timestamp.FromTime(now), 60, promclient.QueryOptions{}, func(res model.Matrix) error {
		if res.Len() < 1 {
			return errors.New("No result series returned")
		}
		if nvals := len(res[0].Values); nvals < 200 {
			return errors.Errorf("Too few values in result matrix, got %d, expected > 200", nvals)
		}
		return nil
	})
}

func TestDistributedEngineWithoutOverlappingIntervals(t *testing.T) {
	t.Skip("skipping test as this replicates a bug")
	t.Parallel()
	e, err := e2e.New(e2e.WithName("dist-disj-tsdbs2"))
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	ctx := context.Background()
	l := log.NewLogfmtLogger(os.Stdout)
	now := time.Now()

	bucket1 := "dist-disj-tsdbs2-test2"
	minio1 := e2edb.NewMinio(e, "1", bucket1, e2edb.WithMinioTLS())
	testutil.Ok(t, e2e.StartAndWaitReady(minio1))

	bkt1, err := s3.NewBucketWithConfig(l, e2ethanos.NewS3Config(bucket1, minio1.Endpoint("http"), minio1.Dir()), "test", nil)
	testutil.Ok(t, err)

	// Setup a storage GW with 2 blocks that have a gap to trigger distributed query MinT bug
	dir1 := filepath.Join(e.SharedDir(), "tmp1")
	testutil.Ok(t, os.MkdirAll(filepath.Join(e.SharedDir(), dir1), os.ModePerm))
	blockID1, err := e2eutil.CreateBlockWithBlockDelay(ctx,
		dir1,
		[]labels.Labels{labels.FromStrings("__name__", "foo", "instance", "foo_1")},
		1000,
		timestamp.FromTime(now.Add(-14*time.Hour)),
		timestamp.FromTime(now.Add(-12*time.Hour)),
		30*time.Minute,
		labels.FromStrings("prometheus", "p1", "replica", "0"),
		0,
		metadata.NoneFunc,
	)
	testutil.Ok(t, err)
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt1, path.Join(dir1, blockID1.String()), blockID1.String()))

	blockID2, err := e2eutil.CreateBlockWithBlockDelay(ctx,
		dir1,
		[]labels.Labels{labels.FromStrings("__name__", "foo", "instance", "foo_1")},
		1000,
		timestamp.FromTime(now.Add(-4*time.Hour)),
		timestamp.FromTime(now.Add(-2*time.Hour)),
		30*time.Minute,
		labels.FromStrings("prometheus", "p1", "replica", "0"),
		0,
		metadata.NoneFunc,
	)
	testutil.Ok(t, err)
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt1, path.Join(dir1, blockID2.String()), blockID2.String()))
	store1 := e2ethanos.NewStoreGW(
		e,
		"s1",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket1, minio1.InternalEndpoint("http"), minio1.InternalDir()),
		},
		"",
		"",
		nil,
	)
	testutil.Ok(t, e2e.StartAndWaitReady(store1))

	querierLeaf1 := e2ethanos.NewQuerierBuilder(e, "1", store1.InternalEndpoint("grpc")).Init()

	testutil.Ok(t, e2e.StartAndWaitReady(querierLeaf1))
	// We need another querier to circumvent the passthrough optimizer
	promConfig2 := e2ethanos.DefaultPromConfig("p2", 0, "", "", e2ethanos.LocalPrometheusTarget)
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "p2", promConfig2, "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom2, sidecar2))
	querierLeaf2 := e2ethanos.NewQuerierBuilder(e, "2", sidecar2.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querierLeaf2))

	querierDistributed := e2ethanos.NewQuerierBuilder(e, "3",
		querierLeaf1.InternalEndpoint("grpc"),
		querierLeaf2.InternalEndpoint("grpc"),
	).
		WithEngine(v1.PromqlEngineThanos).
		WithQueryMode("distributed").
		Init()

	testutil.Ok(t, e2e.StartAndWaitReady(querierDistributed))

	// We would expect 2x2h ranges for the 2 blocks containing foo samples. That would be around 240 expected sample pairs in the result matrix.
	// We assert on more then 200 to reduce flakiness
	rangeQuery(t, ctx, querierDistributed.Endpoint("http"), func() string { return "foo" }, timestamp.FromTime(now.Add(-24*time.Hour)), timestamp.FromTime(now), 60, promclient.QueryOptions{}, func(res model.Matrix) error {
		if res.Len() < 1 {
			return errors.New("No result series returned")
		}
		if nvals := len(res[0].Values); nvals < 200 {
			return errors.Errorf("Too few values in result matrix, got %d, expected > 200", nvals)
		}

		return nil
	})
}
