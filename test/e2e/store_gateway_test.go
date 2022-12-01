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
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/efficientgo/e2e/monitoring/matchers"
	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/thanos-io/objstore/providers/s3"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

const testQuery = "{a=\"1\"}"

// TODO(bwplotka): Extend this test to have multiple stores.
func TestStoreGateway(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("store-gateway")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	const bucket = "store-gateway-test"
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

	s1 := e2ethanos.NewStoreGW(
		e,
		"1",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("https"), m.InternalDir()),
		},
		memcachedConfig,
		nil,
		relabel.Config{
			Action:       relabel.Drop,
			Regex:        relabel.MustNewRegexp("value2"),
			SourceLabels: model.LabelNames{"ext1"},
		},
	)
	testutil.Ok(t, e2e.StartAndWaitReady(s1))
	// Ensure bucket UI.
	ensureGETStatusCode(t, http.StatusOK, "http://"+path.Join(s1.Endpoint("http"), "loaded"))

	q := e2ethanos.NewQuerierBuilder(e, "1", s1.InternalEndpoint("grpc")).WithEnabledFeatures([]string{"promql-negative-offset", "promql-at-modifier"}).Init()
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
	bkt, err := s3.NewBucketWithConfig(l,
		e2ethanos.NewS3Config(bucket, m.Endpoint("https"), m.Dir()), "test-feed")
	testutil.Ok(t, err)

	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id1.String()), id1.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id2.String()), id2.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id3.String()), id3.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id4.String()), id4.String()))

	// Wait for store to sync blocks.
	// thanos_blocks_meta_synced: 2x loadedMeta 1x labelExcludedMeta 1x TooFreshMeta.
	testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(4), "thanos_blocks_meta_synced"))
	testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_sync_failures_total"))

	testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(2), "thanos_bucket_store_blocks_loaded"))
	testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(0), "thanos_bucket_store_block_drops_total"))
	testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(0), "thanos_bucket_store_block_load_failures_total"))

	t.Run("query works", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string { return fmt.Sprintf("%s @ end()", testQuery) },
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
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(6), "thanos_bucket_store_series_data_touched"))
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(6), "thanos_bucket_store_series_data_fetched"))
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(2), "thanos_bucket_store_series_blocks_queried"))

		queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string { return testQuery },
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

		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(12), "thanos_bucket_store_series_data_touched"))
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(8), "thanos_bucket_store_series_data_fetched"))
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(2+2), "thanos_bucket_store_series_blocks_queried"))
	})
	t.Run("remove meta.json from id1 block", func(t *testing.T) {
		testutil.Ok(t, bkt.Delete(ctx, filepath.Join(id1.String(), block.MetaFilename)))

		// Wait for store to sync blocks.
		// thanos_blocks_meta_synced: 1x loadedMeta 1x labelExcludedMeta 1x TooFreshMeta 1x noMeta.
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(4), "thanos_blocks_meta_synced"))
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_sync_failures_total"))

		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(1), "thanos_bucket_store_blocks_loaded"))
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(1), "thanos_bucket_store_block_drops_total"))
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(0), "thanos_bucket_store_block_load_failures_total"))

		// TODO(bwplotka): Entries are still in LRU cache.
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string { return testQuery },
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
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(4+1), "thanos_bucket_store_series_blocks_queried"))
	})
	t.Run("upload block id5, similar to id1", func(t *testing.T) {
		id5, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset4, 0, metadata.NoneFunc)
		testutil.Ok(t, err)
		testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id5.String()), id5.String()))

		// Wait for store to sync blocks.
		// thanos_blocks_meta_synced: 2x loadedMeta 1x labelExcludedMeta 1x TooFreshMeta 1x noMeta.
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(5), "thanos_blocks_meta_synced"))
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_sync_failures_total"))

		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(2), "thanos_bucket_store_blocks_loaded"))
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(1), "thanos_bucket_store_block_drops_total"))
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(0), "thanos_bucket_store_block_load_failures_total"))

		queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string { return testQuery },
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
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(5+2), "thanos_bucket_store_series_blocks_queried"))
	})
	t.Run("delete whole id2 block #yolo", func(t *testing.T) {
		testutil.Ok(t, block.Delete(ctx, l, bkt, id2))

		// Wait for store to sync blocks.
		// thanos_blocks_meta_synced: 1x loadedMeta 1x labelExcludedMeta 1x TooFreshMeta 1x noMeta.
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(4), "thanos_blocks_meta_synced"))
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_sync_failures_total"))

		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(1), "thanos_bucket_store_blocks_loaded"))
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(1+1), "thanos_bucket_store_block_drops_total"))
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(0), "thanos_bucket_store_block_load_failures_total"))

		queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string { return testQuery },
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
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(7+1), "thanos_bucket_store_series_blocks_queried"))
	})

	t.Run("negative offset should work", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string { return "{a=\"1\"} offset -4h" },
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
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(7+2), "thanos_bucket_store_series_blocks_queried"))
	})

	// TODO(khyati) Let's add some case for compaction-meta.json once the PR will be merged: https://github.com/thanos-io/thanos/pull/2136.
}

func TestStoreGatewayMemcachedCache(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("store-memcached")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	const bucket = "store-gateway-memcached-cache-test"
	m := e2ethanos.NewMinio(e, "thanos-minio", bucket)
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	memcached := e2ethanos.NewMemcached(e, "1")
	testutil.Ok(t, e2e.StartAndWaitReady(memcached))

	memcachedConfig := fmt.Sprintf(`type: MEMCACHED
config:
  addresses: [%s]
blocks_iter_ttl: 0s`, memcached.InternalEndpoint("memcached"))

	s1 := e2ethanos.NewStoreGW(
		e,
		"1",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("https"), m.InternalDir()),
		},
		memcachedConfig,
		nil,
	)
	testutil.Ok(t, e2e.StartAndWaitReady(s1))

	q := e2ethanos.NewQuerierBuilder(e, "1", s1.InternalEndpoint("grpc")).Init()
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
	bkt, err := s3.NewBucketWithConfig(l,
		e2ethanos.NewS3Config(bucket, m.Endpoint("https"), m.Dir()), "test-feed")
	testutil.Ok(t, err)

	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id.String()), id.String()))

	// Wait for store to sync blocks.
	// thanos_blocks_meta_synced: 1x loadedMeta 0x labelExcludedMeta 0x TooFreshMeta.
	testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(1), "thanos_blocks_meta_synced"))
	testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_sync_failures_total"))

	testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(1), "thanos_bucket_store_blocks_loaded"))
	testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(0), "thanos_bucket_store_block_drops_total"))
	testutil.Ok(t, s1.WaitSumMetrics(e2emon.Equals(0), "thanos_bucket_store_block_load_failures_total"))

	t.Run("query with cache miss", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string { return testQuery },
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

		testutil.Ok(t, s1.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{`thanos_store_bucket_cache_operation_hits_total`}, e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "config", "chunks"))))
	})

	t.Run("query with cache hit", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string { return testQuery },
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

		testutil.Ok(t, s1.WaitSumMetricsWithOptions(e2emon.Greater(0), []string{`thanos_store_bucket_cache_operation_hits_total`}, e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "config", "chunks"))))
		testutil.Ok(t, s1.WaitSumMetrics(e2emon.Greater(0), "thanos_cache_memcached_hits_total"))
	})

}

func TestStoreGatewayGroupCache(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("store-groupcache")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	const bucket = "store-gateway-groupcache-test"
	m := e2ethanos.NewMinio(e, "thanos-minio", bucket)
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	groupcacheConfig := `type: GROUPCACHE
config:
  self_url: http://e2e-test-store-gateway-groupcache-store-gw-%d:8080
  peers:
    - http://e2e-test-store-gateway-groupcache-store-gw-1:8080
    - http://e2e-test-store-gateway-groupcache-store-gw-2:8080
    - http://e2e-test-store-gateway-groupcache-store-gw-3:8080
  groupcache_group: groupcache_test_group
  dns_interval: 1s
blocks_iter_ttl: 0s
metafile_exists_ttl: 0s
metafile_doesnt_exist_ttl: 0s
metafile_content_ttl: 0s`

	store1 := e2ethanos.NewStoreGW(
		e,
		"1",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("https"), m.InternalDir()),
		},
		fmt.Sprintf(groupcacheConfig, 1),
		nil,
	)
	store2 := e2ethanos.NewStoreGW(
		e,
		"2",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("https"), m.InternalDir()),
		},
		fmt.Sprintf(groupcacheConfig, 2),
		nil,
	)
	store3 := e2ethanos.NewStoreGW(
		e,
		"3",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("https"), m.InternalDir()),
		},
		fmt.Sprintf(groupcacheConfig, 3),
		nil,
	)
	testutil.Ok(t, e2e.StartAndWaitReady(store1, store2, store3))

	q := e2ethanos.NewQuerierBuilder(e, "1",
		store1.InternalEndpoint("grpc"),
		store2.InternalEndpoint("grpc"),
		store3.InternalEndpoint("grpc"),
	).Init()
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
	bkt, err := s3.NewBucketWithConfig(l, e2ethanos.NewS3Config(bucket, m.Endpoint("https"), m.Dir()), "test-feed")
	testutil.Ok(t, err)

	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id.String()), id.String()))

	// Wait for store to sync blocks.
	// thanos_blocks_meta_synced: 1x loadedMeta 0x labelExcludedMeta 0x TooFreshMeta.
	for _, st := range []*e2emon.InstrumentedRunnable{store1, store2, store3} {
		t.Run(st.Name(), func(t *testing.T) {
			testutil.Ok(t, st.WaitSumMetrics(e2emon.Equals(1), "thanos_blocks_meta_synced"))
			testutil.Ok(t, st.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_sync_failures_total"))

			testutil.Ok(t, st.WaitSumMetrics(e2emon.Equals(1), "thanos_bucket_store_blocks_loaded"))
			testutil.Ok(t, st.WaitSumMetrics(e2emon.Equals(0), "thanos_bucket_store_block_drops_total"))
			testutil.Ok(t, st.WaitSumMetrics(e2emon.Equals(0), "thanos_bucket_store_block_load_failures_total"))
		})
	}

	t.Run("query with groupcache loading from object storage", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, q.Endpoint("http"), func() string { return testQuery },
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

		for _, st := range []*e2emon.InstrumentedRunnable{store1, store2, store3} {
			testutil.Ok(t, st.WaitSumMetricsWithOptions(e2emon.Greater(0), []string{`thanos_cache_groupcache_loads_total`}))
			testutil.Ok(t, st.WaitSumMetricsWithOptions(e2emon.Greater(0), []string{`thanos_store_bucket_cache_operation_hits_total`}, e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "config", "chunks"))))
		}
	})

	t.Run("try to load file with slashes", func(t *testing.T) {
		resp, err := http.Get(fmt.Sprintf("http://%s/_galaxycache/groupcache_test_group/content:%s/meta.json", store1.Endpoint("http"), id.String()))
		testutil.Ok(t, err)
		testutil.Equals(t, 200, resp.StatusCode)
	})
}

func TestStoreGatewayBytesLimit(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("store-limit")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	const bucket = "store-gateway-test"
	m := e2ethanos.NewMinio(e, "thanos-minio", bucket)
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	store1 := e2ethanos.NewStoreGW(
		e,
		"1",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("https"), m.InternalDir()),
		},
		"",
		[]string{"--store.grpc.downloaded-bytes-limit=1B"},
	)

	store2 := e2ethanos.NewStoreGW(
		e,
		"2",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("https"), m.InternalDir()),
		},
		"",
		[]string{"--store.grpc.downloaded-bytes-limit=100B"},
	)
	store3 := e2ethanos.NewStoreGW(
		e,
		"3",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("https"), m.InternalDir()),
		},
		"",
		[]string{"--store.grpc.downloaded-bytes-limit=196627B"},
	)

	testutil.Ok(t, e2e.StartAndWaitReady(store1, store2, store3))

	q1 := e2ethanos.NewQuerierBuilder(e, "1", store1.InternalEndpoint("grpc")).Init()
	q2 := e2ethanos.NewQuerierBuilder(e, "2", store2.InternalEndpoint("grpc")).Init()
	q3 := e2ethanos.NewQuerierBuilder(e, "3", store3.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q1, q2, q3))

	dir := filepath.Join(e.SharedDir(), "tmp")
	testutil.Ok(t, os.MkdirAll(filepath.Join(e.SharedDir(), dir), os.ModePerm))

	series := []labels.Labels{labels.FromStrings("a", "1", "b", "2")}
	extLset := labels.FromStrings("ext1", "value1", "replica", "1")
	extLset2 := labels.FromStrings("ext1", "value1", "replica", "2")
	extLset3 := labels.FromStrings("ext1", "value2", "replica", "3")

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
	bkt, err := s3.NewBucketWithConfig(l,
		e2ethanos.NewS3Config(bucket, m.Endpoint("https"), m.Dir()), "test-feed")
	testutil.Ok(t, err)

	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id1.String()), id1.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id2.String()), id2.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id3.String()), id3.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id4.String()), id4.String()))

	// Wait for store to sync blocks.
	testutil.Ok(t, store1.WaitSumMetrics(e2emon.Equals(4), "thanos_blocks_meta_synced"))
	testutil.Ok(t, store2.WaitSumMetrics(e2emon.Equals(4), "thanos_blocks_meta_synced"))
	testutil.Ok(t, store3.WaitSumMetrics(e2emon.Equals(4), "thanos_blocks_meta_synced"))

	t.Run("Series() limits", func(t *testing.T) {

		testutil.Ok(t, runutil.RetryWithLog(log.NewLogfmtLogger(os.Stdout), 5*time.Second, ctx.Done(), func() error {
			_, err := simpleInstantQuery(t,
				ctx,
				q1.Endpoint("http"),
				func() string { return testQuery },
				time.Now,
				promclient.QueryOptions{Deduplicate: true}, 0)
			if err != nil {
				if strings.Contains(err.Error(), "expanded matching posting: get postings: bytes limit exceeded while fetching postings: limit 1 violated") {
					return nil
				}
				return err
			}
			return fmt.Errorf("expected an error")
		}))

		testutil.Ok(t, runutil.RetryWithLog(log.NewLogfmtLogger(os.Stdout), 5*time.Second, ctx.Done(), func() error {
			_, err := simpleInstantQuery(t,
				ctx,
				q2.Endpoint("http"),
				func() string { return testQuery },
				time.Now,
				promclient.QueryOptions{Deduplicate: true}, 0)
			if err != nil {
				if strings.Contains(err.Error(), "preload series: exceeded bytes limit while fetching series: limit 100 violated") {
					return nil
				}
				return err
			}
			return fmt.Errorf("expected an error")
		}))

		testutil.Ok(t, runutil.RetryWithLog(log.NewLogfmtLogger(os.Stdout), 5*time.Second, ctx.Done(), func() error {
			_, err := simpleInstantQuery(t,
				ctx,
				q3.Endpoint("http"),
				func() string { return testQuery },
				time.Now,
				promclient.QueryOptions{Deduplicate: true}, 0)
			if err != nil {
				if strings.Contains(err.Error(), "load chunks: bytes limit exceeded while fetching chunks: limit 196627 violated") {
					return nil
				}
				return err
			}
			return fmt.Errorf("expected an error")
		}))
	})
}
