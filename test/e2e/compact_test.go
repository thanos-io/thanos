// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/efficientgo/e2e/monitoring/matchers"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	"github.com/thanos-io/objstore/providers/s3"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func isEmptyDir(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

type blockDesc struct {
	series  []labels.Labels
	extLset labels.Labels
	mint    int64
	maxt    int64

	markedForNoCompact bool
	hashFunc           metadata.HashFunc
}

func (b *blockDesc) Create(ctx context.Context, dir string, delay time.Duration, hf metadata.HashFunc, numSamples int) (ulid.ULID, error) {
	if delay == 0*time.Second {
		return e2eutil.CreateBlock(ctx, dir, b.series, numSamples, b.mint, b.maxt, b.extLset, 0, hf)
	}
	return e2eutil.CreateBlockWithBlockDelay(ctx, dir, b.series, numSamples, b.mint, b.maxt, delay, b.extLset, 0, hf)
}

func TestCompactWithStoreGateway(t *testing.T) {
	testCompactWithStoreGateway(t, false)
}

func TestCompactWithStoreGatewayWithPenaltyDedup(t *testing.T) {
	testCompactWithStoreGateway(t, true)
}

func testCompactWithStoreGateway(t *testing.T, penaltyDedup bool) {
	t.Parallel()
	logger := log.NewLogfmtLogger(os.Stdout)

	justAfterConsistencyDelay := 30 * time.Minute
	// Make sure to take realistic timestamp for start. This is to align blocks as if they would be aligned on Prometheus.
	// To have deterministic compaction, let's have fixed date:
	now, err := time.Parse(time.RFC3339, "2020-03-24T08:00:00Z")
	testutil.Ok(t, err)

	var blocksWithHashes []ulid.ULID

	// Simulate real scenario, including more complex cases like overlaps if needed.
	// TODO(bwplotka): Test delayed delete.
	blocks := []blockDesc{
		// Non overlapping blocks, not ready for compaction.
		{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("case", "no-compaction", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("case", "no-compaction", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("case", "no-compaction", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},
	}
	blocks = append(blocks,
		// Non overlapping blocks, ready for compaction.
		blockDesc{
			series:   []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset:  labels.FromStrings("case", "compaction-ready", "replica", "1"),
			mint:     timestamp.FromTime(now),
			maxt:     timestamp.FromTime(now.Add(2 * time.Hour)),
			hashFunc: metadata.SHA256Func,
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "3")},
			extLset: labels.FromStrings("case", "compaction-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "4")},
			extLset: labels.FromStrings("case", "compaction-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "5")},
			extLset: labels.FromStrings("case", "compaction-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(6 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(8 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "6")},
			extLset: labels.FromStrings("case", "compaction-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(8 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(10 * time.Hour)),
		},
		// Non overlapping blocks, ready for compaction, with one blocked marked for no-compact (no-compact-mark.json)
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("case", "compaction-ready-one-block-marked-for-no-compact", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "3")},
			extLset: labels.FromStrings("case", "compaction-ready-one-block-marked-for-no-compact", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "4")},
			extLset: labels.FromStrings("case", "compaction-ready-one-block-marked-for-no-compact", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),

			markedForNoCompact: true,
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "5")},
			extLset: labels.FromStrings("case", "compaction-ready-one-block-marked-for-no-compact", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(6 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(8 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "6")},
			extLset: labels.FromStrings("case", "compaction-ready-one-block-marked-for-no-compact", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(8 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(10 * time.Hour)),
		},

		// Non overlapping blocks, ready for compaction, only after deduplication.
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("case", "compaction-ready-after-dedup", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "3")},
			extLset: labels.FromStrings("case", "compaction-ready-after-dedup", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "4")},
			extLset: labels.FromStrings("case", "compaction-ready-after-dedup", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "5")},
			extLset: labels.FromStrings("case", "compaction-ready-after-dedup", "replica", "2"),
			mint:    timestamp.FromTime(now.Add(6 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(8 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "6")},
			extLset: labels.FromStrings("case", "compaction-ready-after-dedup", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(8 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(10 * time.Hour)),
		},

		// Replica partial overlapping blocks, not ready for compaction, among no-overlapping blocks.
		// NOTE: We put a- in front to make sure this will be compacted as first one (:
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "1", "b", "2"),
			},
			extLset: labels.FromStrings("case", "a-partial-overlap-dedup-ready", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "3"),
			},
			extLset: labels.FromStrings("case", "a-partial-overlap-dedup-ready", "replica", "2"),
			mint:    timestamp.FromTime(now.Add(1 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "4"),
			},
			extLset: labels.FromStrings("case", "a-partial-overlap-dedup-ready", "replica", "3"),
			mint:    timestamp.FromTime(now.Add(3 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		// Extra.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "5"),
			},
			extLset: labels.FromStrings("case", "a-partial-overlap-dedup-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},

		// Multi-Replica partial overlapping blocks, not ready for compaction, among no-overlapping blocks.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "1", "b", "2"),
			},
			extLset: labels.FromStrings("case", "partial-multi-replica-overlap-dedup-ready", "rule_replica", "1", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "3"),
			},
			extLset: labels.FromStrings("case", "partial-multi-replica-overlap-dedup-ready", "rule_replica", "2", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(1 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "4"),
			},
			// TODO(bwplotka): This is wrong, but let's fix in next PR. We should error out in this case as we should
			// never support overlaps before we modify dedup labels. This probably means another check in fetcher.
			extLset: labels.FromStrings("case", "partial-multi-replica-overlap-dedup-ready", "rule_replica", "1", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(1 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		// Extra.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "5"),
			},
			extLset: labels.FromStrings("case", "partial-multi-replica-overlap-dedup-ready", "rule_replica", "1", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},

		// Replica full overlapping blocks, not ready for compaction, among no-overlapping blocks.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "1", "b", "2"),
			},
			extLset: labels.FromStrings("case", "full-replica-overlap-dedup-ready", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "3"),
			},
			extLset: labels.FromStrings("case", "full-replica-overlap-dedup-ready", "replica", "2"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		// Extra.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "4"),
			},
			extLset: labels.FromStrings("case", "full-replica-overlap-dedup-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "5"),
			},
			extLset: labels.FromStrings("case", "full-replica-overlap-dedup-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},
	)

	name := "e2e-test-compact"
	if penaltyDedup {
		name = "compact-dedup"
	}
	e, err := e2e.NewDockerEnvironment(name)
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	dir := filepath.Join(e.SharedDir(), "tmp")
	testutil.Ok(t, os.MkdirAll(dir, os.ModePerm))

	const bucket = "compact-test"
	m := e2edb.NewMinio(e, "minio", bucket, e2edb.WithMinioTLS())
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	bkt, err := s3.NewBucketWithConfig(logger,
		e2ethanos.NewS3Config(bucket, m.Endpoint("http"), m.Dir()), "test-feed")
	testutil.Ok(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	rawBlockIDs := map[ulid.ULID]struct{}{}
	for _, b := range blocks {
		id, err := b.Create(ctx, dir, justAfterConsistencyDelay, b.hashFunc, 120)
		testutil.Ok(t, err)
		testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
			return objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String())
		}))

		rawBlockIDs[id] = struct{}{}
		if b.markedForNoCompact {
			testutil.Ok(t, block.MarkForNoCompact(ctx, logger, bkt, id, metadata.ManualNoCompactReason, "why not", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))
		}

		if b.hashFunc != metadata.NoneFunc {
			blocksWithHashes = append(blocksWithHashes, id)
		}
	}
	// Block that will be downsampled.
	downsampledBase := blockDesc{
		series: []labels.Labels{
			labels.FromStrings("z", "1", "b", "2"),
			labels.FromStrings("z", "1", "b", "5"),
		},
		extLset: labels.FromStrings("case", "block-about-to-be-downsampled"),
		mint:    timestamp.FromTime(now),
		maxt:    timestamp.FromTime(now.Add(10 * 24 * time.Hour)),
	}
	// New block that will be downsampled.
	downsampledRawID, err := downsampledBase.Create(ctx, dir, justAfterConsistencyDelay, metadata.NoneFunc, 1200)
	testutil.Ok(t, err)
	testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, downsampledRawID.String()), downsampledRawID.String()))

	{
		// On top of that, add couple of other tricky cases with different meta.
		malformedBase := blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("case", "malformed-things", "replica", "101"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		}

		// New Partial block.
		id, err := malformedBase.Create(ctx, dir, 0*time.Second, metadata.NoneFunc, 120)
		testutil.Ok(t, err)
		testutil.Ok(t, os.Remove(path.Join(dir, id.String(), metadata.MetaFilename)))
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))

		// New Partial block + deletion mark.
		id, err = malformedBase.Create(ctx, dir, 0*time.Second, metadata.NoneFunc, 120)
		testutil.Ok(t, err)
		testutil.Ok(t, os.Remove(path.Join(dir, id.String(), metadata.MetaFilename)))
		testutil.Ok(t, block.MarkForDeletion(ctx, logger, bkt, id, "", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))

		// Partial block after consistency delay.
		id, err = malformedBase.Create(ctx, dir, justAfterConsistencyDelay, metadata.NoneFunc, 120)
		testutil.Ok(t, err)
		testutil.Ok(t, os.Remove(path.Join(dir, id.String(), metadata.MetaFilename)))
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))

		// Partial block after consistency delay + deletion mark.
		id, err = malformedBase.Create(ctx, dir, justAfterConsistencyDelay, metadata.NoneFunc, 120)
		testutil.Ok(t, err)
		testutil.Ok(t, os.Remove(path.Join(dir, id.String(), metadata.MetaFilename)))
		testutil.Ok(t, block.MarkForDeletion(ctx, logger, bkt, id, "", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))

		// Partial block after consistency delay + old deletion mark ready to be deleted.
		id, err = malformedBase.Create(ctx, dir, justAfterConsistencyDelay, metadata.NoneFunc, 120)
		testutil.Ok(t, err)
		testutil.Ok(t, os.Remove(path.Join(dir, id.String(), metadata.MetaFilename)))
		deletionMark, err := json.Marshal(metadata.DeletionMark{
			ID: id,
			// Deletion threshold is usually 2 days.
			DeletionTime: time.Now().Add(-50 * time.Hour).Unix(),
			Version:      metadata.DeletionMarkVersion1,
		})
		testutil.Ok(t, err)
		testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), metadata.DeletionMarkFilename), bytes.NewBuffer(deletionMark)))
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))

		// Partial block after delete threshold.
		id, err = malformedBase.Create(ctx, dir, 50*time.Hour, metadata.NoneFunc, 120)
		testutil.Ok(t, err)
		testutil.Ok(t, os.Remove(path.Join(dir, id.String(), metadata.MetaFilename)))
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))

		// Partial block after delete threshold + deletion mark.
		id, err = malformedBase.Create(ctx, dir, 50*time.Hour, metadata.NoneFunc, 120)
		testutil.Ok(t, err)
		testutil.Ok(t, os.Remove(path.Join(dir, id.String(), metadata.MetaFilename)))
		testutil.Ok(t, block.MarkForDeletion(ctx, logger, bkt, id, "", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))
	}

	bktConfig := client.BucketConfig{
		Type:   client.S3,
		Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("http"), m.InternalDir()),
	}

	// Crank down the deletion mark delay since deduplication can miss blocks in the presence of replica labels it doesn't know about.
	str := e2ethanos.NewStoreGW(e, "1", bktConfig, "", "", []string{"--ignore-deletion-marks-delay=2s"})
	testutil.Ok(t, e2e.StartAndWaitReady(str))
	testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(float64(len(rawBlockIDs)+8)), "thanos_blocks_meta_synced"))
	testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_sync_failures_total"))
	testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_modified"))

	q := e2ethanos.NewQuerierBuilder(e, "1", str.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	// Check if query detects current series, even if overlapped.
	queryAndAssert(t, ctx, q.Endpoint("http"),
		func() string {
			return fmt.Sprintf(`count_over_time({a="1"}[13h] offset %ds)`, int64(time.Since(now.Add(12*time.Hour)).Seconds()))
		},
		time.Now,
		promclient.QueryOptions{
			Deduplicate: false, // This should be false, so that we can be sure deduplication was offline.
		},
		model.Vector{
			{Value: 360, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "no-compaction", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready-after-dedup", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready-after-dedup", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready-after-dedup", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready-after-dedup", "replica": "2"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready-after-dedup", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "a-partial-overlap-dedup-ready", "replica": "1"}},
			{Value: 240, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "a-partial-overlap-dedup-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "a-partial-overlap-dedup-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "a-partial-overlap-dedup-ready", "replica": "2"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "a-partial-overlap-dedup-ready", "replica": "2"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "a-partial-overlap-dedup-ready", "replica": "3"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "a-partial-overlap-dedup-ready", "replica": "3"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "1"}},
			{Value: 320, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "2"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "2"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
			{Value: 360, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "full-replica-overlap-dedup-ready", "replica": "2"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "full-replica-overlap-dedup-ready", "replica": "2"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
		},
	)
	// Store view:
	testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(float64(len(rawBlockIDs)+8)), "thanos_blocks_meta_synced"))
	testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_sync_failures_total"))
	testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_modified"))

	expectedEndVector := model.Vector{
		// NOTE(bwplotka): Even after deduplication some series has still replica labels. This is because those blocks did not overlap yet with anything.
		// This is fine as querier deduplication will remove it if needed.
		{Value: 360, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "no-compaction", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready-one-block-marked-for-no-compact"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready-one-block-marked-for-no-compact"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready-after-dedup"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready-after-dedup"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready-after-dedup"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready-after-dedup"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready-after-dedup", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "a-partial-overlap-dedup-ready"}},
		{Value: 360, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "a-partial-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "a-partial-overlap-dedup-ready", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "a-partial-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "a-partial-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "a-partial-overlap-dedup-ready", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "partial-multi-replica-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "1"}},
		{Value: 240, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "partial-multi-replica-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "partial-multi-replica-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "partial-multi-replica-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "full-replica-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "full-replica-overlap-dedup-ready"}},
		{Value: 240, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "full-replica-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
	}

	if penaltyDedup {
		expectedEndVector = model.Vector{
			// NOTE(bwplotka): Even after deduplication some series has still replica labels. This is because those blocks did not overlap yet with anything.
			// This is fine as querier deduplication will remove it if needed.
			{Value: 360, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "no-compaction", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready-one-block-marked-for-no-compact"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready-one-block-marked-for-no-compact"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready-after-dedup"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready-after-dedup"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready-after-dedup"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready-after-dedup"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready-after-dedup", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "a-partial-overlap-dedup-ready"}},
			// If no penalty dedup enabled, the value should be 360.
			{Value: 200, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "a-partial-overlap-dedup-ready"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "a-partial-overlap-dedup-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "a-partial-overlap-dedup-ready"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "a-partial-overlap-dedup-ready"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "a-partial-overlap-dedup-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "partial-multi-replica-overlap-dedup-ready"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "1"}},
			// If no penalty dedup enabled, the value should be 240.
			{Value: 195, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "partial-multi-replica-overlap-dedup-ready"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "partial-multi-replica-overlap-dedup-ready"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "partial-multi-replica-overlap-dedup-ready"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "full-replica-overlap-dedup-ready"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "full-replica-overlap-dedup-ready"}},
			{Value: 240, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "full-replica-overlap-dedup-ready"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
		}
	}

	// No replica label with overlaps should halt compactor. This test is sequential
	// because we do not want two Thanos Compact instances deleting the same partially
	// uploaded blocks and blocks with deletion marks. We also check that Thanos Compactor
	// deletes directories inside of a compaction group that do not belong there.
	{
		cFuture := e2ethanos.NewCompactorBuilder(e, "expect-to-halt")

		// Precreate a directory. It should be deleted.
		// In a hypothetical scenario, the directory could be a left-over from
		// a compaction that had crashed.
		testutil.Assert(t, len(blocksWithHashes) > 0)

		m, err := block.DownloadMeta(ctx, logger, bkt, blocksWithHashes[0])
		testutil.Ok(t, err)

		randBlockDir := filepath.Join(cFuture.Dir(), "compact", m.Thanos.GroupKey(), "ITISAVERYRANDULIDFORTESTS0")
		testutil.Ok(t, os.MkdirAll(randBlockDir, os.ModePerm))

		f, err := os.Create(filepath.Join(randBlockDir, "index"))
		testutil.Ok(t, err)
		testutil.Ok(t, f.Close())

		c := cFuture.Init(bktConfig, nil)
		testutil.Ok(t, e2e.StartAndWaitReady(c))

		// Expect compactor halted and one cleanup iteration to happen.
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(1), "thanos_compact_halted"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(1), "thanos_compact_block_cleanup_loops_total"))

		testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(float64(len(rawBlockIDs)+6)), "thanos_blocks_meta_synced"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_sync_failures_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_modified"))

		// The compact directory is still there.
		empty, err := isEmptyDir(c.Dir())
		testutil.Ok(t, err)
		testutil.Equals(t, false, empty, "directory %e should not be empty", c.Dir())

		// We expect no ops.
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(0), "thanos_compact_iterations_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(0), "thanos_compact_block_cleanup_failures_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(0), "thanos_compact_blocks_marked_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(0), "thanos_compact_group_compactions_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(0), "thanos_compact_group_vertical_compactions_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(1), "thanos_compact_group_compactions_failures_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(2), "thanos_compact_group_compaction_runs_started_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(1), "thanos_compact_group_compaction_runs_completed_total"))

		// However, the blocks have been cleaned because that happens concurrently.
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(2), "thanos_compact_aborted_partial_uploads_deletion_attempts_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(2), "thanos_compact_blocks_cleaned_total"))

		// Ensure bucket UI.
		ensureGETStatusCode(t, http.StatusOK, "http://"+path.Join(c.Endpoint("http"), "global"))
		ensureGETStatusCode(t, http.StatusOK, "http://"+path.Join(c.Endpoint("http"), "loaded"))

		testutil.Ok(t, c.Stop())

		_, err = os.Stat(randBlockDir)
		testutil.NotOk(t, err)
		testutil.Assert(t, os.IsNotExist(err))
	}

	// Sequential because we want to check that Thanos Compactor does not
	// touch files it does not need to.
	// Dedup enabled; compactor should work as expected.
	{

		cFuture := e2ethanos.NewCompactorBuilder(e, "working")

		// Predownload block dirs with hashes. We should not try downloading them again.
		for _, id := range blocksWithHashes {
			m, err := block.DownloadMeta(ctx, logger, bkt, id)
			testutil.Ok(t, err)

			delete(m.Thanos.Labels, "replica")
			testutil.Ok(t, block.Download(ctx, logger, bkt, id, filepath.Join(cFuture.Dir(), "compact", m.Thanos.GroupKey(), id.String())))
		}

		extArgs := []string{"--deduplication.replica-label=replica", "--deduplication.replica-label=rule_replica"}
		if penaltyDedup {
			extArgs = append(extArgs, "--deduplication.func=penalty")
		}

		// We expect 2x 4-block compaction, 2-block vertical compaction, 2x 3-block compaction.
		c := cFuture.Init(bktConfig, nil, extArgs...)
		testutil.Ok(t, e2e.StartAndWaitReady(c))

		// NOTE: We cannot assert on intermediate `thanos_blocks_meta_` metrics as those are gauge and change dynamically due to many
		// compaction groups. Wait for at least first compaction iteration (next is in 5m).
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Greater(0), "thanos_compact_iterations_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(0), "thanos_compact_blocks_cleaned_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(0), "thanos_compact_block_cleanup_failures_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(2*4+2+2*3+2), "thanos_compact_blocks_marked_total")) // 18.
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(0), "thanos_compact_aborted_partial_uploads_deletion_attempts_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(6), "thanos_compact_group_compactions_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(3), "thanos_compact_group_vertical_compactions_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(0), "thanos_compact_group_compactions_failures_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(14), "thanos_compact_group_compaction_runs_started_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(14), "thanos_compact_group_compaction_runs_completed_total"))

		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(2), "thanos_compact_downsample_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(0), "thanos_compact_downsample_failures_total"))

		testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(float64(
			len(rawBlockIDs)+8+
				2+ // Downsampled one block into two new ones - 5m/1h.
				6+ // 6 compactions, 6 newly added blocks.
				-2, // Partial block removed.
		)), "thanos_blocks_meta_synced"))
		testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_sync_failures_total"))

		testutil.Ok(t, c.WaitSumMetrics(e2emon.Equals(0), "thanos_compact_halted"))

		bucketMatcher, err := matchers.NewMatcher(matchers.MatchEqual, "bucket", bucket)
		testutil.Ok(t, err)
		operationMatcher, err := matchers.NewMatcher(matchers.MatchEqual, "operation", "get")
		testutil.Ok(t, err)
		testutil.Ok(t, c.WaitSumMetricsWithOptions(
			e2emon.Between(0, 1000),
			[]string{"thanos_objstore_bucket_operations_total"}, e2emon.WithLabelMatchers(
				bucketMatcher,
				operationMatcher,
			),
			e2emon.WaitMissingMetrics(),
		))

		// Make sure compactor does not modify anything else over time.
		testutil.Ok(t, c.Stop())

		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		// Check if query detects new blocks.
		queryAndAssert(t, ctx, q.Endpoint("http"),
			func() string {
				return fmt.Sprintf(`count_over_time({a="1"}[13h] offset %ds)`, int64(time.Since(now.Add(12*time.Hour)).Seconds()))
			},
			time.Now,
			promclient.QueryOptions{
				Deduplicate: false, // This should be false, so that we can be sure deduplication was offline.
			},
			expectedEndVector,
		)
		// Store view:
		testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(float64(len(rawBlockIDs)+8+6-2+2)), "thanos_blocks_meta_synced"))
		testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_sync_failures_total"))
		testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_modified"))
	}

	// dedup enabled; no delete delay; compactor should work and remove things as expected.
	{
		extArgs := []string{"--deduplication.replica-label=replica", "--deduplication.replica-label=rule_replica", "--delete-delay=0s"}
		if penaltyDedup {
			extArgs = append(extArgs, "--deduplication.func=penalty")
		}
		c := e2ethanos.NewCompactorBuilder(e, "working-dedup").Init(bktConfig, nil, extArgs...)
		testutil.Ok(t, e2e.StartAndWaitReady(c))

		// NOTE: We cannot assert on intermediate `thanos_blocks_meta_` metrics as those are gauge and change dynamically due to many
		// compaction groups. Wait for at least first compaction iteration (next is in 5m).
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Greater(0), []string{"thanos_compact_iterations_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(19), []string{"thanos_compact_blocks_cleaned_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_compact_block_cleanup_failures_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_compact_aborted_partial_uploads_deletion_attempts_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_compact_group_compactions_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_compact_group_vertical_compactions_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_compact_group_compactions_failures_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(7), []string{"thanos_compact_group_compaction_runs_started_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(7), []string{"thanos_compact_group_compaction_runs_completed_total"}, e2emon.WaitMissingMetrics()))

		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(2), []string{"thanos_compact_downsample_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_compact_downsample_failures_total"}, e2emon.WaitMissingMetrics()))

		testutil.Ok(t, str.WaitSumMetricsWithOptions(
			e2emon.Equals(21),
			[]string{"thanos_blocks_meta_synced"},
			e2emon.WaitMissingMetrics(),
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "state", "loaded")),
		))
		testutil.Ok(t, str.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_blocks_meta_sync_failures_total"}, e2emon.WaitMissingMetrics()))

		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_compact_halted"}, e2emon.WaitMissingMetrics()))
		// Make sure compactor does not modify anything else over time.
		testutil.Ok(t, c.Stop())

		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		// Check if query detects new blocks.
		queryAndAssert(t, ctx, q.Endpoint("http"),
			func() string {
				return fmt.Sprintf(`count_over_time({a="1"}[13h] offset %ds)`, int64(time.Since(now.Add(12*time.Hour)).Seconds()))
			},
			time.Now,
			promclient.QueryOptions{
				Deduplicate: false, // This should be false, so that we can be sure deduplication was offline.
			},
			expectedEndVector,
		)

		// Store view:
		testutil.Ok(t, str.WaitSumMetricsWithOptions(
			e2emon.Equals(21),
			[]string{"thanos_blocks_meta_synced"},
			e2emon.WaitMissingMetrics(),
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "state", "loaded")),
		))
		testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_sync_failures_total"))
		testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_modified"))
	}

	// Ensure that querying downsampled blocks works. Then delete the raw block and try querying again.
	{
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		// Just to have a consistent result.
		checkQuery := func() string {
			return `last_over_time({z="1"}[2h]) - last_over_time({z="1"}[2h])`
		}

		queryAndAssert(t, ctx, q.Endpoint("http"),
			checkQuery,
			func() time.Time { return now.Add(10 * 24 * time.Hour) },
			promclient.QueryOptions{
				Deduplicate: true,
			},
			model.Vector{
				{Value: 0, Metric: map[model.LabelName]model.LabelValue{"b": "2", "case": "block-about-to-be-downsampled", "z": "1"}},
				{Value: 0, Metric: map[model.LabelName]model.LabelValue{"b": "5", "case": "block-about-to-be-downsampled", "z": "1"}},
			},
		)

		// Find out whether querying still works after deleting the raw data. After this,
		// pre-aggregated sum/count should be used.
		testutil.Ok(t, block.Delete(ctx, log.NewNopLogger(), bkt, downsampledRawID))

		testutil.Ok(t, str.Stop())
		testutil.Ok(t, e2e.StartAndWaitReady(str))
		testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
			return str.WaitSumMetricsWithOptions(
				e2emon.Equals(20),
				[]string{"thanos_blocks_meta_synced"},
				e2emon.WaitMissingMetrics(),
				e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "state", "loaded")),
			)
		}))
		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics(), e2emon.WithLabelMatchers(
			matchers.MustNewMatcher(matchers.MatchEqual, "store_type", "store"),
		)))

		queryAndAssert(t, ctx, q.Endpoint("http"),
			checkQuery,
			func() time.Time { return now.Add(10 * 24 * time.Hour) },
			promclient.QueryOptions{
				Deduplicate:         true,
				MaxSourceResolution: "1h",
			},
			model.Vector{
				{Value: 0, Metric: map[model.LabelName]model.LabelValue{"b": "2", "case": "block-about-to-be-downsampled", "z": "1"}},
				{Value: 0, Metric: map[model.LabelName]model.LabelValue{"b": "5", "case": "block-about-to-be-downsampled", "z": "1"}},
			},
		)
	}
}

func ensureGETStatusCode(t testing.TB, code int, url string) {
	t.Helper()

	r, err := http.Get(url)
	testutil.Ok(t, err)
	testutil.Equals(t, code, r.StatusCode)
}

func TestCompactorDownsampleIgnoresMarked(t *testing.T) {
	now, err := time.Parse(time.RFC3339, "2020-03-24T08:00:00Z")
	testutil.Ok(t, err)

	logger := log.NewLogfmtLogger(os.Stderr)
	e, err := e2e.NewDockerEnvironment("downsample-mrkd")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	dir := filepath.Join(e.SharedDir(), "tmp")
	testutil.Ok(t, os.MkdirAll(dir, os.ModePerm))

	const bucket = "compact-test"
	m := e2edb.NewMinio(e, "minio", bucket, e2edb.WithMinioTLS())
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	bktCfg := e2ethanos.NewS3Config(bucket, m.Endpoint("http"), m.Dir())
	bkt, err := s3.NewBucketWithConfig(logger, bktCfg, "test")
	testutil.Ok(t, err)

	downsampledBase := blockDesc{
		series: []labels.Labels{
			labels.FromStrings("z", "1", "b", "2"),
			labels.FromStrings("z", "1", "b", "5"),
		},
		extLset: labels.FromStrings("case", "block-about-to-be-downsampled"),
		mint:    timestamp.FromTime(now),
		maxt:    timestamp.FromTime(now.Add(10 * 24 * time.Hour)),
	}
	// New block that will be downsampled.
	justAfterConsistencyDelay := 30 * time.Minute

	downsampledRawID, err := downsampledBase.Create(context.Background(), dir, justAfterConsistencyDelay, metadata.NoneFunc, 1200)
	testutil.Ok(t, err)
	testutil.Ok(t, objstore.UploadDir(context.Background(), logger, bkt, path.Join(dir, downsampledRawID.String()), downsampledRawID.String()))
	testutil.Ok(t, block.MarkForNoDownsample(context.Background(), logger, bkt, downsampledRawID, metadata.ManualNoDownsampleReason, "why not", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))

	c := e2ethanos.NewCompactorBuilder(e, "working").Init(client.BucketConfig{
		Type:   client.S3,
		Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("http"), m.Dir()),
	}, nil)
	testutil.Ok(t, e2e.StartAndWaitReady(c))
	testutil.NotOk(t, c.WaitSumMetricsWithOptions(e2emon.Greater(0), []string{"thanos_compact_downsample_total"}, e2emon.WaitMissingMetrics()))

}
