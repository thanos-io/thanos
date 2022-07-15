// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package bucketindex

import (
	"bytes"
	"context"
	"path"
	"strings"
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block/metadata"

	cortex_testutil "github.com/thanos-io/thanos/internal/cortex/storage/tsdb/testutil"
)

func TestBlockDeletionMarkFilepath(t *testing.T) {
	id := ulid.MustNew(1, nil)

	assert.Equal(t, "markers/"+id.String()+"-deletion-mark.json", BlockDeletionMarkFilepath(id))
}

func TestIsBlockDeletionMarkFilename(t *testing.T) {
	expected := ulid.MustNew(1, nil)

	_, ok := IsBlockDeletionMarkFilename("xxx")
	assert.False(t, ok)

	_, ok = IsBlockDeletionMarkFilename("xxx-deletion-mark.json")
	assert.False(t, ok)

	_, ok = IsBlockDeletionMarkFilename("tenant-deletion-mark.json")
	assert.False(t, ok)

	actual, ok := IsBlockDeletionMarkFilename(expected.String() + "-deletion-mark.json")
	assert.True(t, ok)
	assert.Equal(t, expected, actual)
}

func TestMigrateBlockDeletionMarksToGlobalLocation(t *testing.T) {
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	ctx := context.Background()

	// Create some fixtures.
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	block3 := ulid.MustNew(3, nil)
	block4 := ulid.MustNew(4, nil)
	require.NoError(t, bkt.Upload(ctx, path.Join("user-1", block1.String(), metadata.DeletionMarkFilename), strings.NewReader("{}")))
	require.NoError(t, bkt.Upload(ctx, path.Join("user-1", block3.String(), metadata.DeletionMarkFilename), strings.NewReader("{}")))
	require.NoError(t, bkt.Upload(ctx, path.Join("user-1", block4.String(), metadata.NoCompactMarkFilename), strings.NewReader("{}")))

	t.Run("doesn't increase thanos_objstore_bucket_operation_failures_total for NotFound deletion markers", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		bkt = objstore.BucketWithMetrics("", bkt, reg)
		require.NoError(t, bkt.Upload(ctx, path.Join("user-1", block2.String(), metadata.MetaFilename), strings.NewReader("{}")))
		require.NoError(t, MigrateBlockDeletionMarksToGlobalLocation(ctx, bkt, "user-1", nil))

		assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP thanos_objstore_bucket_operation_failures_total Total number of operations against a bucket that failed, but were not expected to fail in certain way from caller perspective. Those errors have to be investigated.
		# TYPE thanos_objstore_bucket_operation_failures_total counter
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="attributes"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="delete"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="exists"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="get"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="get_range"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="iter"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="upload"} 0
	`,
		), "thanos_objstore_bucket_operation_failures_total"))
	})

	t.Run("works", func(t *testing.T) {
		require.NoError(t, MigrateBlockDeletionMarksToGlobalLocation(ctx, bkt, "user-1", nil))

		// Ensure deletion marks have been copied.
		for _, tc := range []struct {
			blockID            ulid.ULID
			expectedExists     bool
			globalFilePathFunc func(ulid.ULID) string
		}{
			{block1, true, BlockDeletionMarkFilepath},
			{block2, false, BlockDeletionMarkFilepath},
			{block3, true, BlockDeletionMarkFilepath},
			{block4, true, NoCompactMarkFilenameMarkFilepath},
		} {
			ok, err := bkt.Exists(ctx, path.Join("user-1", tc.globalFilePathFunc(tc.blockID)))
			require.NoError(t, err)
			require.Equal(t, tc.expectedExists, ok)
		}
	})
}
