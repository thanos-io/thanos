// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package querier

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/internal/cortex/storage/bucket"
	cortex_tsdb "github.com/thanos-io/thanos/internal/cortex/storage/tsdb"
	"github.com/thanos-io/thanos/internal/cortex/storage/tsdb/bucketindex"
	cortex_testutil "github.com/thanos-io/thanos/internal/cortex/storage/tsdb/testutil"
	"github.com/thanos-io/thanos/internal/cortex/util/services"
)

func TestBucketScanBlocksFinder_InitialScan(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, reg := prepareBucketScanBlocksFinder(t, prepareBucketScanBlocksFinderConfig())

	user1Block1 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 10, 20)
	user1Block2 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 20, 30)
	user2Block1 := cortex_testutil.MockStorageBlock(t, bucket, "user-2", 10, 20)
	user2Mark1 := bucketindex.BlockDeletionMarkFromThanosMarker(cortex_testutil.MockStorageDeletionMark(t, bucket, "user-2", user2Block1))

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	blocks, deletionMarks, err := s.GetBlocks(ctx, "user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 2, len(blocks))
	assert.Equal(t, user1Block2.ULID, blocks[0].ID)
	assert.Equal(t, user1Block1.ULID, blocks[1].ID)
	assert.WithinDuration(t, time.Now(), blocks[0].GetUploadedAt(), 5*time.Second)
	assert.WithinDuration(t, time.Now(), blocks[1].GetUploadedAt(), 5*time.Second)
	assert.Empty(t, deletionMarks)

	blocks, deletionMarks, err = s.GetBlocks(ctx, "user-2", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 1, len(blocks))
	assert.Equal(t, user2Block1.ULID, blocks[0].ID)
	assert.WithinDuration(t, time.Now(), blocks[0].GetUploadedAt(), 5*time.Second)
	assert.Equal(t, map[ulid.ULID]*bucketindex.BlockDeletionMark{
		user2Block1.ULID: user2Mark1,
	}, deletionMarks)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_blocks_meta_syncs_total Total blocks metadata synchronization attempts
		# TYPE cortex_blocks_meta_syncs_total counter
		cortex_blocks_meta_syncs_total{component="querier"} 2

		# HELP cortex_blocks_meta_sync_failures_total Total blocks metadata synchronization failures
		# TYPE cortex_blocks_meta_sync_failures_total counter
		cortex_blocks_meta_sync_failures_total{component="querier"} 0

		# HELP cortex_blocks_meta_sync_consistency_delay_seconds Configured consistency delay in seconds.
		# TYPE cortex_blocks_meta_sync_consistency_delay_seconds gauge
		cortex_blocks_meta_sync_consistency_delay_seconds{component="querier"} 0
	`),
		"cortex_blocks_meta_syncs_total",
		"cortex_blocks_meta_sync_failures_total",
		"cortex_blocks_meta_sync_consistency_delay_seconds",
	))

	assert.Greater(t, testutil.ToFloat64(s.scanLastSuccess), float64(0))
}

func TestBucketScanBlocksFinder_InitialScanFailure(t *testing.T) {
	ctx := context.Background()
	bucket := &bucket.ClientMock{}
	reg := prometheus.NewPedanticRegistry()

	cfg := prepareBucketScanBlocksFinderConfig()
	cfg.CacheDir = t.TempDir()

	s := NewBucketScanBlocksFinder(cfg, bucket, nil, log.NewNopLogger(), reg)
	defer func() {
		s.StopAsync()
		s.AwaitTerminated(context.Background()) // nolint: errcheck
	}()

	// Mock the storage to simulate a failure when reading objects.
	bucket.MockIter("", []string{"user-1"}, nil)
	bucket.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D"}, nil)
	bucket.MockExists(path.Join("user-1", cortex_tsdb.TenantDeletionMarkPath), false, nil)
	bucket.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", "invalid", errors.New("mocked error"))

	require.NoError(t, s.StartAsync(ctx))
	require.Error(t, s.AwaitRunning(ctx))

	blocks, deletionMarks, err := s.GetBlocks(ctx, "user-1", 0, 30)
	assert.Equal(t, errBucketScanBlocksFinderNotRunning, err)
	assert.Nil(t, blocks)
	assert.Nil(t, deletionMarks)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_blocks_meta_syncs_total Total blocks metadata synchronization attempts
		# TYPE cortex_blocks_meta_syncs_total counter
		cortex_blocks_meta_syncs_total{component="querier"} 3

		# HELP cortex_blocks_meta_sync_failures_total Total blocks metadata synchronization failures
		# TYPE cortex_blocks_meta_sync_failures_total counter
		cortex_blocks_meta_sync_failures_total{component="querier"} 3

		# HELP cortex_blocks_meta_sync_consistency_delay_seconds Configured consistency delay in seconds.
		# TYPE cortex_blocks_meta_sync_consistency_delay_seconds gauge
		cortex_blocks_meta_sync_consistency_delay_seconds{component="querier"} 0

		# HELP cortex_querier_blocks_last_successful_scan_timestamp_seconds Unix timestamp of the last successful blocks scan.
		# TYPE cortex_querier_blocks_last_successful_scan_timestamp_seconds gauge
		cortex_querier_blocks_last_successful_scan_timestamp_seconds 0
	`),
		"cortex_blocks_meta_syncs_total",
		"cortex_blocks_meta_sync_failures_total",
		"cortex_blocks_meta_sync_consistency_delay_seconds",
		"cortex_querier_blocks_last_successful_scan_timestamp_seconds",
	))
}

func TestBucketScanBlocksFinder_StopWhileRunningTheInitialScanOnManyTenants(t *testing.T) {
	tenantIDs := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}

	// Mock the bucket to introduce a 1s sleep while iterating each tenant in the bucket.
	bucket := &bucket.ClientMock{}
	bucket.MockIter("", tenantIDs, nil)
	for _, tenantID := range tenantIDs {
		bucket.MockIterWithCallback(tenantID+"/", []string{}, nil, func() {
			time.Sleep(time.Second)
		})
		bucket.MockExists(path.Join(tenantID, cortex_tsdb.TenantDeletionMarkPath), false, nil)
	}

	cfg := prepareBucketScanBlocksFinderConfig()
	cfg.CacheDir = t.TempDir()
	cfg.MetasConcurrency = 1
	cfg.TenantsConcurrency = 1

	s := NewBucketScanBlocksFinder(cfg, bucket, nil, log.NewLogfmtLogger(os.Stdout), nil)

	// Start the scanner, let it run for 1s and then issue a stop.
	require.NoError(t, s.StartAsync(context.Background()))
	time.Sleep(time.Second)

	stopTime := time.Now()
	_ = services.StopAndAwaitTerminated(context.Background(), s)

	// Expect to stop before having completed the full sync (which is expected to take
	// 1s for each tenant due to the delay introduced in the mock).
	assert.Less(t, time.Since(stopTime).Nanoseconds(), (3 * time.Second).Nanoseconds())
}

func TestBucketScanBlocksFinder_StopWhileRunningTheInitialScanOnManyBlocks(t *testing.T) {
	var blockPaths []string
	for i := 1; i <= 10; i++ {
		blockPaths = append(blockPaths, "user-1/"+ulid.MustNew(uint64(i), nil).String())
	}

	// Mock the bucket to introduce a 1s sleep while syncing each block in the bucket.
	bucket := &bucket.ClientMock{}
	bucket.MockIter("", []string{"user-1"}, nil)
	bucket.MockIter("user-1/", blockPaths, nil)
	bucket.On("Exists", mock.Anything, mock.Anything).Return(false, nil).Run(func(args mock.Arguments) {
		// We return the meta.json doesn't exist, but introduce a 1s delay for each call.
		time.Sleep(time.Second)
	})

	cfg := prepareBucketScanBlocksFinderConfig()
	cfg.CacheDir = t.TempDir()
	cfg.MetasConcurrency = 1
	cfg.TenantsConcurrency = 1

	s := NewBucketScanBlocksFinder(cfg, bucket, nil, log.NewLogfmtLogger(os.Stdout), nil)

	// Start the scanner, let it run for 1s and then issue a stop.
	require.NoError(t, s.StartAsync(context.Background()))
	time.Sleep(time.Second)

	stopTime := time.Now()
	_ = services.StopAndAwaitTerminated(context.Background(), s)

	// Expect to stop before having completed the full sync (which is expected to take
	// 1s for each block due to the delay introduced in the mock).
	assert.Less(t, time.Since(stopTime).Nanoseconds(), (3 * time.Second).Nanoseconds())
}

func TestBucketScanBlocksFinder_PeriodicScanFindsNewUser(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, _ := prepareBucketScanBlocksFinder(t, prepareBucketScanBlocksFinderConfig())

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	blocks, deletionMarks, err := s.GetBlocks(ctx, "user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 0, len(blocks))
	assert.Empty(t, deletionMarks)

	block1 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 10, 20)
	block2 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 20, 30)
	mark2 := bucketindex.BlockDeletionMarkFromThanosMarker(cortex_testutil.MockStorageDeletionMark(t, bucket, "user-1", block2))

	// Trigger a periodic sync
	require.NoError(t, s.scan(ctx))

	blocks, deletionMarks, err = s.GetBlocks(ctx, "user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 2, len(blocks))
	assert.Equal(t, block2.ULID, blocks[0].ID)
	assert.Equal(t, block1.ULID, blocks[1].ID)
	assert.WithinDuration(t, time.Now(), blocks[0].GetUploadedAt(), 5*time.Second)
	assert.WithinDuration(t, time.Now(), blocks[1].GetUploadedAt(), 5*time.Second)
	assert.Equal(t, map[ulid.ULID]*bucketindex.BlockDeletionMark{
		block2.ULID: mark2,
	}, deletionMarks)
}

func TestBucketScanBlocksFinder_PeriodicScanFindsNewBlock(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, _ := prepareBucketScanBlocksFinder(t, prepareBucketScanBlocksFinderConfig())

	block1 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 10, 20)

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	blocks, deletionMarks, err := s.GetBlocks(ctx, "user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 1, len(blocks))
	assert.Equal(t, block1.ULID, blocks[0].ID)
	assert.WithinDuration(t, time.Now(), blocks[0].GetUploadedAt(), 5*time.Second)
	assert.Empty(t, deletionMarks)

	block2 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 20, 30)

	// Trigger a periodic sync
	require.NoError(t, s.scan(ctx))

	blocks, deletionMarks, err = s.GetBlocks(ctx, "user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 2, len(blocks))
	assert.Equal(t, block2.ULID, blocks[0].ID)
	assert.Equal(t, block1.ULID, blocks[1].ID)
	assert.WithinDuration(t, time.Now(), blocks[0].GetUploadedAt(), 5*time.Second)
	assert.WithinDuration(t, time.Now(), blocks[1].GetUploadedAt(), 5*time.Second)
	assert.Empty(t, deletionMarks)
}

func TestBucketScanBlocksFinder_PeriodicScanFindsBlockMarkedForDeletion(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, _ := prepareBucketScanBlocksFinder(t, prepareBucketScanBlocksFinderConfig())

	block1 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 10, 20)
	block2 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 20, 30)

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	blocks, deletionMarks, err := s.GetBlocks(ctx, "user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 2, len(blocks))
	assert.Equal(t, block2.ULID, blocks[0].ID)
	assert.Equal(t, block1.ULID, blocks[1].ID)
	assert.Empty(t, deletionMarks)

	mark1 := bucketindex.BlockDeletionMarkFromThanosMarker(cortex_testutil.MockStorageDeletionMark(t, bucket, "user-1", block1))

	// Trigger a periodic sync
	require.NoError(t, s.scan(ctx))

	blocks, deletionMarks, err = s.GetBlocks(ctx, "user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 2, len(blocks))
	assert.Equal(t, block2.ULID, blocks[0].ID)
	assert.Equal(t, block1.ULID, blocks[1].ID)
	assert.Equal(t, map[ulid.ULID]*bucketindex.BlockDeletionMark{
		block1.ULID: mark1,
	}, deletionMarks)
}

func TestBucketScanBlocksFinder_PeriodicScanFindsDeletedBlock(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, _ := prepareBucketScanBlocksFinder(t, prepareBucketScanBlocksFinderConfig())

	block1 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 10, 20)
	block2 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 20, 30)

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	blocks, deletionMarks, err := s.GetBlocks(ctx, "user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 2, len(blocks))
	assert.Equal(t, block2.ULID, blocks[0].ID)
	assert.Equal(t, block1.ULID, blocks[1].ID)
	assert.Empty(t, deletionMarks)

	require.NoError(t, bucket.Delete(ctx, fmt.Sprintf("%s/%s", "user-1", block1.ULID.String())))

	// Trigger a periodic sync
	require.NoError(t, s.scan(ctx))

	blocks, deletionMarks, err = s.GetBlocks(ctx, "user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 1, len(blocks))
	assert.Equal(t, block2.ULID, blocks[0].ID)
	assert.Empty(t, deletionMarks)
}

func TestBucketScanBlocksFinder_PeriodicScanFindsDeletedUser(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, _ := prepareBucketScanBlocksFinder(t, prepareBucketScanBlocksFinderConfig())

	block1 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 10, 20)
	block2 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 20, 30)

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	blocks, deletionMarks, err := s.GetBlocks(ctx, "user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 2, len(blocks))
	assert.Equal(t, block2.ULID, blocks[0].ID)
	assert.Equal(t, block1.ULID, blocks[1].ID)
	assert.Empty(t, deletionMarks)

	require.NoError(t, bucket.Delete(ctx, "user-1"))

	// Trigger a periodic sync
	require.NoError(t, s.scan(ctx))

	blocks, deletionMarks, err = s.GetBlocks(ctx, "user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 0, len(blocks))
	assert.Empty(t, deletionMarks)
}

func TestBucketScanBlocksFinder_PeriodicScanFindsUserWhichWasPreviouslyDeleted(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, _ := prepareBucketScanBlocksFinder(t, prepareBucketScanBlocksFinderConfig())

	block1 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 10, 20)
	block2 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 20, 30)

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	blocks, deletionMarks, err := s.GetBlocks(ctx, "user-1", 0, 40)
	require.NoError(t, err)
	require.Equal(t, 2, len(blocks))
	assert.Equal(t, block2.ULID, blocks[0].ID)
	assert.Equal(t, block1.ULID, blocks[1].ID)
	assert.Empty(t, deletionMarks)

	require.NoError(t, bucket.Delete(ctx, "user-1"))

	// Trigger a periodic sync
	require.NoError(t, s.scan(ctx))

	blocks, deletionMarks, err = s.GetBlocks(ctx, "user-1", 0, 40)
	require.NoError(t, err)
	require.Equal(t, 0, len(blocks))
	assert.Empty(t, deletionMarks)

	block3 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 30, 40)

	// Trigger a periodic sync
	require.NoError(t, s.scan(ctx))

	blocks, deletionMarks, err = s.GetBlocks(ctx, "user-1", 0, 40)
	require.NoError(t, err)
	require.Equal(t, 1, len(blocks))
	assert.Equal(t, block3.ULID, blocks[0].ID)
	assert.Empty(t, deletionMarks)
}

func TestBucketScanBlocksFinder_GetBlocks(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, _ := prepareBucketScanBlocksFinder(t, prepareBucketScanBlocksFinderConfig())

	block1 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 10, 15)
	block2 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 12, 20)
	block3 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 20, 30)
	block4 := cortex_testutil.MockStorageBlock(t, bucket, "user-1", 30, 40)
	mark3 := bucketindex.BlockDeletionMarkFromThanosMarker(cortex_testutil.MockStorageDeletionMark(t, bucket, "user-1", block3))

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	tests := map[string]struct {
		minT          int64
		maxT          int64
		expectedMetas []tsdb.BlockMeta
		expectedMarks map[ulid.ULID]*bucketindex.BlockDeletionMark
	}{
		"no matching block because the range is too low": {
			minT:          0,
			maxT:          5,
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{},
		},
		"no matching block because the range is too high": {
			minT:          50,
			maxT:          60,
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{},
		},
		"matching all blocks": {
			minT:          0,
			maxT:          60,
			expectedMetas: []tsdb.BlockMeta{block4, block3, block2, block1},
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{
				block3.ULID: mark3,
			},
		},
		"query range starting at a block maxT": {
			minT:          block3.MaxTime,
			maxT:          60,
			expectedMetas: []tsdb.BlockMeta{block4},
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{},
		},
		"query range ending at a block minT": {
			minT:          block3.MinTime,
			maxT:          block4.MinTime,
			expectedMetas: []tsdb.BlockMeta{block4, block3},
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{
				block3.ULID: mark3,
			},
		},
		"query range within a single block": {
			minT:          block3.MinTime + 2,
			maxT:          block3.MaxTime - 2,
			expectedMetas: []tsdb.BlockMeta{block3},
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{
				block3.ULID: mark3,
			},
		},
		"query range within multiple blocks": {
			minT:          13,
			maxT:          16,
			expectedMetas: []tsdb.BlockMeta{block2, block1},
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{},
		},
		"query range matching exactly a single block": {
			minT:          block3.MinTime,
			maxT:          block3.MaxTime - 1,
			expectedMetas: []tsdb.BlockMeta{block3},
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{
				block3.ULID: mark3,
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			metas, deletionMarks, err := s.GetBlocks(ctx, "user-1", testData.minT, testData.maxT)
			require.NoError(t, err)
			require.Equal(t, len(testData.expectedMetas), len(metas))
			require.Equal(t, testData.expectedMarks, deletionMarks)

			for i, expectedBlock := range testData.expectedMetas {
				assert.Equal(t, expectedBlock.ULID, metas[i].ID)
			}
		})
	}
}

func prepareBucketScanBlocksFinder(t *testing.T, cfg BucketScanBlocksFinderConfig) (*BucketScanBlocksFinder, objstore.Bucket, string, *prometheus.Registry) {
	bkt, storageDir := cortex_testutil.PrepareFilesystemBucket(t)

	reg := prometheus.NewPedanticRegistry()
	cfg.CacheDir = t.TempDir()
	s := NewBucketScanBlocksFinder(cfg, bkt, nil, log.NewNopLogger(), reg)

	t.Cleanup(func() {
		s.StopAsync()
		require.NoError(t, s.AwaitTerminated(context.Background()))
	})

	return s, bkt, storageDir, reg
}

func prepareBucketScanBlocksFinderConfig() BucketScanBlocksFinderConfig {
	return BucketScanBlocksFinderConfig{
		ScanInterval:             time.Minute,
		TenantsConcurrency:       10,
		MetasConcurrency:         10,
		IgnoreDeletionMarksDelay: time.Hour,
	}
}
