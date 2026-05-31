// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/thanos-io/objstore"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type flakyDeleteBucket struct {
	objstore.Bucket
	failuresBeforeSuccess int32
	deletesFailed         atomic.Int32
}

func (b *flakyDeleteBucket) Delete(ctx context.Context, name string) error {
	if b.deletesFailed.Load() < b.failuresBeforeSuccess {
		b.deletesFailed.Add(1)
		return errors.New("simulated transient object-store error")
	}
	return b.Bucket.Delete(ctx, name)
}

func uploadFakeBlock(t *testing.T, ctx context.Context, bkt objstore.Bucket, id ulid.ULID) {
	t.Helper()

	var meta metadata.Meta
	meta.Version = metadata.TSDBVersion1
	meta.ULID = id
	meta.Thanos.Labels = map[string]string{"replica": "test"}

	var metaBuf bytes.Buffer
	testutil.Ok(t, meta.Write(&metaBuf))
	testutil.Ok(t, bkt.Upload(ctx, id.String()+"/"+metadata.MetaFilename, &metaBuf))

	testutil.Ok(t, bkt.Upload(ctx, id.String()+"/chunks/000001", bytes.NewReader([]byte{0, 1, 2, 3})))
}

func TestBlocksCleaner_DeleteMarkedBlocks_RetriesTransientDeleteFailures(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := ulid.MustNew(uint64(time.Now().Add(-2*time.Hour).Unix()*1000), nil)

	mb := objstore.NewInMemBucket()
	bkt := objstore.WithNoopInstr(mb)
	uploadFakeBlock(t, ctx, bkt, id)

	logger := log.NewNopLogger()

	markCounter := promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test_block_marked"})
	testutil.Ok(t, block.MarkForDeletion(ctx, logger, bkt, id, "test", markCounter))

	// Fail 2 times before succeeding.
	flaky := &flakyDeleteBucket{Bucket: bkt, failuresBeforeSuccess: 2}

	df := block.NewIgnoreDeletionMarkFilter(logger, bkt, 48*time.Hour, 32)
	testutil.Ok(t, df.Filter(ctx, map[ulid.ULID]*metadata.Meta{id: nil}, nil, nil))

	cleanedMetric := promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test_blocks_cleaned"})
	failedMetric := promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test_block_cleanup_failures"})
	cleaner := NewBlocksCleaner(logger, flaky, df, 0*time.Second, cleanedMetric, failedMetric)

	deleted, err := cleaner.DeleteMarkedBlocks(ctx)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(deleted))

	testutil.Equals(t, 1.0, promtest.ToFloat64(cleanedMetric))
	testutil.Equals(t, 0.0, promtest.ToFloat64(failedMetric))
	testutil.Equals(t, int32(2), flaky.deletesFailed.Load())

	exists, err := bkt.Exists(ctx, id.String()+"/"+metadata.MetaFilename)
	testutil.Ok(t, err)
	testutil.Equals(t, false, exists)
}

// persistentlyFailingBucket fails every Delete call.
type persistentlyFailingBucket struct {
	objstore.Bucket
}

func (b *persistentlyFailingBucket) Delete(_ context.Context, _ string) error {
	return errors.New("persistent object-store error")
}

func TestBlocksCleaner_DeleteMarkedBlocks_PersistentFailureStillReturnsError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := ulid.MustNew(uint64(time.Now().Add(-2*time.Hour).Unix()*1000), nil)

	mb := objstore.NewInMemBucket()
	bkt := objstore.WithNoopInstr(mb)
	uploadFakeBlock(t, ctx, bkt, id)

	logger := log.NewNopLogger()

	markCounter := promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test_block_marked_2"})
	testutil.Ok(t, block.MarkForDeletion(ctx, logger, bkt, id, "test", markCounter))

	failing := &persistentlyFailingBucket{Bucket: bkt}

	df := block.NewIgnoreDeletionMarkFilter(logger, bkt, 48*time.Hour, 32)
	testutil.Ok(t, df.Filter(ctx, map[ulid.ULID]*metadata.Meta{id: nil}, nil, nil))

	cleanedMetric := promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test_blocks_cleaned_2"})
	failedMetric := promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test_block_cleanup_failures_2"})
	cleaner := NewBlocksCleaner(logger, failing, df, 0*time.Second, cleanedMetric, failedMetric)

	_, err := cleaner.DeleteMarkedBlocks(ctx)
	testutil.NotOk(t, err)
	testutil.Equals(t, 0.0, promtest.ToFloat64(cleanedMetric))
	testutil.Equals(t, 1.0, promtest.ToFloat64(failedMetric))
}
