// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	"github.com/efficientgo/e2e/matchers"
	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

// TODO(bwplotka): Extend this test to have multiple stores.
// TODO(bwplotka): Extend this test for downsampling.
func TestStoreGateway(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_store_gateway")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	const bucket = "store_gateway_test"
	m := e2ethanos.NewMinio(e, "thanos-minio", bucket)
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	memcached := e2ethanos.NewMemcached(e, "1")
	testutil.Ok(t, e2e.StartAndWaitReady(memcached))

	memcachedConfig := fmt.Sprintf(`type: MEMCACHED
config:
  addresses: [%s]
blocks_iter_ttl: 0s
metafile_exists_ttl: 0s
metafile_doesnt_exist_ttl: 0s
metafile_content_ttl: 0s`, memcached.InternalEndpoint("memcached"))

	s1, err := e2ethanos.NewStoreGW(
		e,
		"1",
		client.BucketConfig{
			Type: client.S3,
			Config: s3.Config{
				Bucket:    bucket,
				AccessKey: e2edb.MinioAccessKey,
				SecretKey: e2edb.MinioSecretKey,
				Endpoint:  m.InternalEndpoint("http"),
				Insecure:  true,
			},
		},
		memcachedConfig,
		relabel.Config{
			Action:       relabel.Drop,
			Regex:        relabel.MustNewRegexp("value2"),
			SourceLabels: model.LabelNames{"ext1"},
		},
	)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(s1))
	// Ensure bucket UI.
	ensureGETStatusCode(t, http.StatusOK, "http://"+path.Join(s1.Endpoint("http"), "loaded"))

	q, err := e2ethanos.NewQuerierBuilder(e, "1", s1.InternalEndpoint("grpc")).WithEnabledFeatures([]string{"promql-negative-offset", "promql-at-modifier"}).Build()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	dir := filepath.Join(e.SharedDir(), "tmp")
	testutil.Ok(t, os.MkdirAll(filepath.Join(e.SharedDir(), dir), os.ModePerm))

	series := []labels.Labels{labels.FromStrings("a", "1", "b", "2")}
	extLset := labels.FromStrings("ext1", "value1", "replica", "1")
	extLset2 := labels.FromStrings("ext1", "value1", "replica", "2")
	extLset3 := labels.FromStrings("ext1", "value2", "replica", "3")
	extLset4 := labels.FromStrings("ext1", "value1", "replica", "3")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	now := time.Now()
	id1, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset, 0, metadata.NoneFunc)
	testutil.Ok(t, err)
	id2, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset2, 0, metadata.NoneFunc)
	testutil.Ok(t, err)
	id3, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset3, 0, metadata.NoneFunc)
	testutil.Ok(t, err)
	id4, err := e2eutil.CreateBlock(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), extLset, 0, metadata.NoneFunc)
	testutil.Ok(t, err)
	l := log.NewLogfmtLogger(os.Stdout)
	bkt, err := s3.NewBucketWithConfig(l, s3.Config{
		Bucket:    bucket,
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
		Endpoint:  m.Endpoint("http"), // We need separate client config, when connecting to minio from outside.
		Insecure:  true,
	}, "test-feed")
	testutil.Ok(t, err)

	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id1.String()), id1.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id2.String()), id2.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id3.String()), id3.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id4.String()), id4.String()))

	// Wait for store to sync blocks.
	// thanos_blocks_meta_synced: 2x loadedMeta 1x labelExcludedMeta 1x TooFreshMeta.
	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(4), "thanos_blocks_meta_synced"))
	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(2), "thanos_bucket_store_blocks_loaded"))
	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_bucket_store_block_drops_total"))
	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_bucket_store_block_load_failures_total"))

	t.Run("query works", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), "{a=\"1\"} @ end()",
			time.Now, promclient.QueryOptions{
				Deduplicate: false,
			},
			[]model.Metric{
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "1",
				},
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "2",
				},
			},
		)

		// 2 x postings, 2 x series, 2x chunks.
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(6), "thanos_bucket_store_series_data_touched"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(6), "thanos_bucket_store_series_data_fetched"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(2), "thanos_bucket_store_series_blocks_queried"))

		queryAndAssertSeries(t, ctx, q.Endpoint("http"), "{a=\"1\"}",
			time.Now, promclient.QueryOptions{
				Deduplicate: true,
			},
			[]model.Metric{
				{
					"a":    "1",
					"b":    "2",
					"ext1": "value1",
				},
			},
		)

		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(12), "thanos_bucket_store_series_data_touched"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(8), "thanos_bucket_store_series_data_fetched"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(2+2), "thanos_bucket_store_series_blocks_queried"))
	})
	t.Run("remove meta.json from id1 block", func(t *testing.T) {
		testutil.Ok(t, bkt.Delete(ctx, filepath.Join(id1.String(), block.MetaFilename)))

		// Wait for store to sync blocks.
		// thanos_blocks_meta_synced: 1x loadedMeta 1x labelExcludedMeta 1x TooFreshMeta 1x noMeta.
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(4), "thanos_blocks_meta_synced"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(1), "thanos_bucket_store_blocks_loaded"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(1), "thanos_bucket_store_block_drops_total"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_bucket_store_block_load_failures_total"))

		// TODO(bwplotka): Entries are still in LRU cache.
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), "{a=\"1\"}",
			time.Now, promclient.QueryOptions{
				Deduplicate: false,
			},
			[]model.Metric{
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "2",
				},
			},
		)
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(4+1), "thanos_bucket_store_series_blocks_queried"))
	})
	t.Run("upload block id5, similar to id1", func(t *testing.T) {
		id5, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset4, 0, metadata.NoneFunc)
		testutil.Ok(t, err)
		testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id5.String()), id5.String()))

		// Wait for store to sync blocks.
		// thanos_blocks_meta_synced: 2x loadedMeta 1x labelExcludedMeta 1x TooFreshMeta 1x noMeta.
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(5), "thanos_blocks_meta_synced"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(2), "thanos_bucket_store_blocks_loaded"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(1), "thanos_bucket_store_block_drops_total"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_bucket_store_block_load_failures_total"))

		queryAndAssertSeries(t, ctx, q.Endpoint("http"), "{a=\"1\"}",
			time.Now, promclient.QueryOptions{
				Deduplicate: false,
			},
			[]model.Metric{
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "2",
				},
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "3", // New block.
				},
			},
		)
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(5+2), "thanos_bucket_store_series_blocks_queried"))
	})
	t.Run("delete whole id2 block #yolo", func(t *testing.T) {
		testutil.Ok(t, block.Delete(ctx, l, bkt, id2))

		// Wait for store to sync blocks.
		// thanos_blocks_meta_synced: 1x loadedMeta 1x labelExcludedMeta 1x TooFreshMeta 1x noMeta.
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(4), "thanos_blocks_meta_synced"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(1), "thanos_bucket_store_blocks_loaded"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(1+1), "thanos_bucket_store_block_drops_total"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_bucket_store_block_load_failures_total"))

		queryAndAssertSeries(t, ctx, q.Endpoint("http"), "{a=\"1\"}",
			time.Now, promclient.QueryOptions{
				Deduplicate: false,
			},
			[]model.Metric{
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "3",
				},
			},
		)
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(7+1), "thanos_bucket_store_series_blocks_queried"))
	})

	t.Run("negative offset should work", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), "{a=\"1\"} offset -4h",
			func() time.Time { return time.Now().Add(-4 * time.Hour) }, promclient.QueryOptions{
				Deduplicate: false,
			},
			[]model.Metric{
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "3",
				},
			},
		)
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(7+2), "thanos_bucket_store_series_blocks_queried"))
	})

	// TODO(khyati) Let's add some case for compaction-meta.json once the PR will be merged: https://github.com/thanos-io/thanos/pull/2136.
}

func TestStoreGatewayMemcachedCache(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_store_gateway_memcached_cache")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	const bucket = "store_gateway_memcached_cache_test"
	m := e2ethanos.NewMinio(e, "thanos-minio", bucket)
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	memcached := e2ethanos.NewMemcached(e, "1")
	testutil.Ok(t, e2e.StartAndWaitReady(memcached))

	memcachedConfig := fmt.Sprintf(`type: MEMCACHED
config:
  addresses: [%s]
blocks_iter_ttl: 0s`, memcached.InternalEndpoint("memcached"))

	s1, err := e2ethanos.NewStoreGW(
		e,
		"1",
		client.BucketConfig{
			Type: client.S3,
			Config: s3.Config{
				Bucket:    bucket,
				AccessKey: e2edb.MinioAccessKey,
				SecretKey: e2edb.MinioSecretKey,
				Endpoint:  m.InternalEndpoint("http"),
				Insecure:  true,
			},
		},
		memcachedConfig,
	)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(s1))

	q, err := e2ethanos.NewQuerierBuilder(e, "1", s1.InternalEndpoint("grpc")).Build()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	dir := filepath.Join(e.SharedDir(), "tmp")
	testutil.Ok(t, os.MkdirAll(dir, os.ModePerm))

	series := []labels.Labels{labels.FromStrings("a", "1", "b", "2")}
	extLset := labels.FromStrings("ext1", "value1", "replica", "1")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	now := time.Now()
	id, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset, 0, metadata.NoneFunc)
	testutil.Ok(t, err)

	l := log.NewLogfmtLogger(os.Stdout)
	bkt, err := s3.NewBucketWithConfig(l, s3.Config{
		Bucket:    bucket,
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
		Endpoint:  m.Endpoint("http"), // We need separate client config, when connecting to minio from outside.
		Insecure:  true,
	}, "test-feed")
	testutil.Ok(t, err)

	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id.String()), id.String()))

	// Wait for store to sync blocks.
	// thanos_blocks_meta_synced: 1x loadedMeta 0x labelExcludedMeta 0x TooFreshMeta.
	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(1), "thanos_blocks_meta_synced"))
	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(1), "thanos_bucket_store_blocks_loaded"))
	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_bucket_store_block_drops_total"))
	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_bucket_store_block_load_failures_total"))

	t.Run("query with cache miss", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), "{a=\"1\"}",
			time.Now, promclient.QueryOptions{
				Deduplicate: false,
			},
			[]model.Metric{
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "1",
				},
			},
		)

		testutil.Ok(t, s1.WaitSumMetricsWithOptions(e2e.Equals(0), []string{`thanos_store_bucket_cache_operation_hits_total`}, e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "config", "chunks"))))
	})

	t.Run("query with cache hit", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), "{a=\"1\"}",
			time.Now, promclient.QueryOptions{
				Deduplicate: false,
			},
			[]model.Metric{
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "1",
				},
			},
		)

		testutil.Ok(t, s1.WaitSumMetricsWithOptions(e2e.Greater(0), []string{`thanos_store_bucket_cache_operation_hits_total`}, e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "config", "chunks"))))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Greater(0), "thanos_cache_memcached_hits_total"))
	})

}
