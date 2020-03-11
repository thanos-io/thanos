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

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestCompact(t *testing.T) {
	t.Parallel()
	l := log.NewLogfmtLogger(os.Stdout)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	s, err := e2e.NewScenario("e2e_test_compact")
	testutil.Ok(t, err)
	defer s.Close()

	const bucket = "thanos"

	m := e2edb.NewMinio(80, bucket)
	testutil.Ok(t, s.StartAndWaitReady(m))

	bkt, err := s3.NewBucketWithConfig(l, s3.Config{
		Bucket:    bucket,
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
		Endpoint:  m.HTTPEndpoint(), // We need separate client config, when connecting to minio from outside.
		Insecure:  true,
	}, "test-feed")
	testutil.Ok(t, err)

	dir := filepath.Join(s.SharedDir(), "tmp")
	testutil.Ok(t, os.MkdirAll(filepath.Join(s.SharedDir(), dir), os.ModePerm))

	series := []labels.Labels{labels.FromStrings("a", "1", "b", "2")}
	extLset1 := labels.FromStrings("ext1", "value1", "replica", "1")
	extLset2 := labels.FromStrings("ext1", "value1", "replica", "2")
	extLset3 := labels.FromStrings("ext1", "value1", "rule_replica", "1")

	now := time.Now()
	id1, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset1, 0)
	testutil.Ok(t, err)
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id1.String()), id1.String()))

	id2, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset2, 0)
	testutil.Ok(t, err)
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id2.String()), id2.String()))

	id3, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset3, 0)
	testutil.Ok(t, err)
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id3.String()), id3.String()))

	cmpt, err := e2ethanos.NewCompactor(s.SharedDir(), "1", client.BucketConfig{
		Type: client.S3,
		Config: s3.Config{
			Bucket:    bucket,
			AccessKey: e2edb.MinioAccessKey,
			SecretKey: e2edb.MinioSecretKey,
			Endpoint:  m.NetworkHTTPEndpoint(),
			Insecure:  true,
		},
	},
		"--deduplication.replica-label=replica",
		"--deduplication.replica-label=rule_replica",
	)

	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(cmpt))

	testutil.Ok(t, cmpt.WaitSumMetrics(e2e.Equals(3), "thanos_blocks_meta_synced"))
	testutil.Ok(t, cmpt.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))
	testutil.Ok(t, cmpt.WaitSumMetrics(e2e.Equals(3), "thanos_blocks_meta_modified"))

	str, err := e2ethanos.NewStoreGW(s.SharedDir(), "1", client.BucketConfig{
		Type: client.S3,
		Config: s3.Config{
			Bucket:    bucket,
			AccessKey: e2edb.MinioAccessKey,
			SecretKey: e2edb.MinioSecretKey,
			Endpoint:  m.NetworkHTTPEndpoint(),
			Insecure:  true,
		},
	})
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(str))

	testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(1), "thanos_blocks_meta_synced"))
	testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

	q, err := e2ethanos.NewQuerier(s.SharedDir(), "1", []string{str.GRPCNetworkEndpoint()}, nil)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	t.Run("query works", func(t *testing.T) {
		queryAndAssert(t, ctx, q.HTTPEndpoint(), "{a=\"1\"}",
			promclient.QueryOptions{
				Deduplicate: false, // This should be false, so that we can be sure deduplication was offline.
			},
			[]model.Metric{
				{
					"a":    "1",
					"b":    "2",
					"ext1": "value1",
				},
			},
		)

		// Make sure only necessary amount of blocks fetched from store, to observe affects of offline deduplication.
		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(3), "thanos_bucket_store_series_data_touched"))
		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(3), "thanos_bucket_store_series_data_fetched"))
		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(1), "thanos_bucket_store_series_blocks_queried"))
	})
}
