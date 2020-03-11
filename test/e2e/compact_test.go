// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/thanos-io/thanos/pkg/block"
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

	// blockDesc describes a recipe to generate blocks from the given series and external labels.
	type blockDesc struct {
		series                   []labels.Labels
		extLsets                 []labels.Labels
		numberOfSamplesPerSeries int
	}

	for i, tcase := range []struct {
		name          string
		blocks        []blockDesc
		replicaLabels []string
		query         string

		expected               []model.Metric
		numberOfModifiedBlocks float64
		numberOfBlocks         uint64
		numberOfSamples        uint64
		numberOfSeries         uint64
		numberOfChunks         uint64
	}{
		{
			name: "overlapping blocks with matching replica labels",
			blocks: []blockDesc{
				{
					series: []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLsets: []labels.Labels{
						labels.FromStrings("ext1", "value1", "replica", "1"),
						labels.FromStrings("ext1", "value1", "replica", "2"),
						labels.FromStrings("ext1", "value1", "rule_replica", "1"),
					},
					numberOfSamplesPerSeries: 10,
				},
			},
			replicaLabels: []string{"replica", "rule_replica"},
			query:         "{a=\"1\"}",

			expected: []model.Metric{
				{
					"a":    "1",
					"b":    "2",
					"ext1": "value1",
				},
			},
			numberOfModifiedBlocks: 3,
			numberOfBlocks:         1,
			numberOfSamples:        10,
			numberOfSeries:         1,
			numberOfChunks:         1,
		},
		{
			name: "overlapping blocks with non-matching replica labels",
			blocks: []blockDesc{
				{
					series: []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLsets: []labels.Labels{
						labels.FromStrings("ext1", "value1"),
						labels.FromStrings("ext1", "value2"),
					},
					numberOfSamplesPerSeries: 2,
				},
			},
			replicaLabels: []string{"replica"},
			query:         "{a=\"1\"}",

			expected: []model.Metric{
				{
					"a":    "1",
					"b":    "2",
					"ext1": "value1",
				},
				{
					"a":    "1",
					"b":    "2",
					"ext1": "value2",
				},
			},
			numberOfModifiedBlocks: 0,
			numberOfBlocks:         2,
			numberOfSamples:        4,
			numberOfSeries:         2,
			numberOfChunks:         2,
		},
	} {
		i := i
		tcase := tcase
		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()

			s, err := e2e.NewScenario("e2e_test_compact_" + strconv.Itoa(i))
			testutil.Ok(t, err)
			defer s.Close()

			dir := filepath.Join(s.SharedDir(), "tmp")
			testutil.Ok(t, os.MkdirAll(filepath.Join(s.SharedDir(), dir), os.ModePerm))

			bucket := "thanos_" + strconv.Itoa(i)

			m := e2edb.NewMinio(8080+i, bucket)
			testutil.Ok(t, s.StartAndWaitReady(m))

			bkt, err := s3.NewBucketWithConfig(l, s3.Config{
				Bucket:    bucket,
				AccessKey: e2edb.MinioAccessKey,
				SecretKey: e2edb.MinioSecretKey,
				Endpoint:  m.HTTPEndpoint(), // We need separate client config, when connecting to minio from outside.
				Insecure:  true,
			}, "test-feed")
			testutil.Ok(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()

			now := time.Now()

			var rawBlockIds []ulid.ULID
			for _, b := range tcase.blocks {
				for _, extLset := range b.extLsets {
					id, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, b.series, b.numberOfSamplesPerSeries, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset, 0)
					testutil.Ok(t, err)
					testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id.String()), id.String()))
					rawBlockIds = append(rawBlockIds, id)
				}
			}

			dedupFlags := make([]string, 0, len(tcase.replicaLabels))
			for _, l := range tcase.replicaLabels {
				dedupFlags = append(dedupFlags, "--deduplication.replica-label="+l)
			}

			cmpt, err := e2ethanos.NewCompactor(s.SharedDir(), strconv.Itoa(i), client.BucketConfig{
				Type: client.S3,
				Config: s3.Config{
					Bucket:    bucket,
					AccessKey: e2edb.MinioAccessKey,
					SecretKey: e2edb.MinioSecretKey,
					Endpoint:  m.NetworkHTTPEndpoint(),
					Insecure:  true,
				},
			},
				dedupFlags...,
			)

			testutil.Ok(t, err)
			testutil.Ok(t, s.StartAndWaitReady(cmpt))

			testutil.Ok(t, cmpt.WaitSumMetrics(e2e.Equals(float64(len(rawBlockIds))), "thanos_blocks_meta_synced"))
			testutil.Ok(t, cmpt.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))
			testutil.Ok(t, cmpt.WaitSumMetrics(e2e.Equals(tcase.numberOfModifiedBlocks), "thanos_blocks_meta_modified"))

			str, err := e2ethanos.NewStoreGW(s.SharedDir(), "compact_"+strconv.Itoa(i), client.BucketConfig{
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

			testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(float64(tcase.numberOfBlocks)), "thanos_blocks_meta_synced"))
			testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

			q, err := e2ethanos.NewQuerier(s.SharedDir(), "compact_"+strconv.Itoa(i), []string{str.GRPCNetworkEndpoint()}, nil)
			testutil.Ok(t, err)
			testutil.Ok(t, s.StartAndWaitReady(q))

			ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			queryAndAssert(t, ctx, q.HTTPEndpoint(),
				tcase.query,
				promclient.QueryOptions{
					Deduplicate: false, // This should be false, so that we can be sure deduplication was offline.
				},
				tcase.expected,
			)

			var numberOfBlocks uint64
			var numberOfSamples uint64
			var numberOfSeries uint64
			var numberOfChunks uint64
			var sources []ulid.ULID

			testutil.Ok(t, bkt.Iter(ctx, "", func(n string) error {
				id, ok := block.IsBlockDir(n)
				if !ok {
					return nil
				}

				numberOfBlocks += 1

				meta, err := block.DownloadMeta(ctx, l, bkt, id)
				if err != nil {
					return err
				}

				numberOfSamples += meta.Stats.NumSamples
				numberOfSeries += meta.Stats.NumSeries
				numberOfChunks += meta.Stats.NumChunks
				sources = append(sources, meta.Compaction.Sources...)

				return nil
			}))

			// Make sure only necessary amount of blocks fetched from store, to observe affects of offline deduplication.
			testutil.Equals(t, tcase.numberOfBlocks, numberOfBlocks)
			if len(rawBlockIds) < int(tcase.numberOfBlocks) { // check sources only if compacted.
				testutil.Equals(t, rawBlockIds, sources)
			}

			testutil.Equals(t, tcase.numberOfSamples, numberOfSamples)
			testutil.Equals(t, tcase.numberOfSeries, numberOfSeries)
			testutil.Equals(t, tcase.numberOfChunks, numberOfChunks)
		})
	}
}
