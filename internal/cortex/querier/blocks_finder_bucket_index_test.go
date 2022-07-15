// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package querier

import (
	"context"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/internal/cortex/storage/tsdb/bucketindex"
	cortex_testutil "github.com/thanos-io/thanos/internal/cortex/storage/tsdb/testutil"
	"github.com/thanos-io/thanos/internal/cortex/util/services"
)

func TestBucketIndexBlocksFinder_GetBlocks(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Mock a bucket index.
	block1 := &bucketindex.Block{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 15}
	block2 := &bucketindex.Block{ID: ulid.MustNew(2, nil), MinTime: 12, MaxTime: 20}
	block3 := &bucketindex.Block{ID: ulid.MustNew(3, nil), MinTime: 20, MaxTime: 30}
	block4 := &bucketindex.Block{ID: ulid.MustNew(4, nil), MinTime: 30, MaxTime: 40}
	block5 := &bucketindex.Block{ID: ulid.MustNew(5, nil), MinTime: 30, MaxTime: 40} // Time range overlaps with block4, but this block deletion mark is above the threshold.
	mark3 := &bucketindex.BlockDeletionMark{ID: block3.ID, DeletionTime: time.Now().Unix()}
	mark5 := &bucketindex.BlockDeletionMark{ID: block5.ID, DeletionTime: time.Now().Add(-2 * time.Hour).Unix()}

	require.NoError(t, bucketindex.WriteIndex(ctx, bkt, userID, nil, &bucketindex.Index{
		Version:            bucketindex.IndexVersion1,
		Blocks:             bucketindex.Blocks{block1, block2, block3, block4, block5},
		BlockDeletionMarks: bucketindex.BlockDeletionMarks{mark3, mark5},
		UpdatedAt:          time.Now().Unix(),
	}))

	finder := prepareBucketIndexBlocksFinder(t, bkt)

	tests := map[string]struct {
		minT           int64
		maxT           int64
		expectedBlocks bucketindex.Blocks
		expectedMarks  map[ulid.ULID]*bucketindex.BlockDeletionMark
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
			minT:           0,
			maxT:           60,
			expectedBlocks: bucketindex.Blocks{block4, block3, block2, block1},
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{
				block3.ID: mark3,
			},
		},
		"query range starting at a block maxT": {
			minT:           block3.MaxTime,
			maxT:           60,
			expectedBlocks: bucketindex.Blocks{block4},
			expectedMarks:  map[ulid.ULID]*bucketindex.BlockDeletionMark{},
		},
		"query range ending at a block minT": {
			minT:           block3.MinTime,
			maxT:           block4.MinTime,
			expectedBlocks: bucketindex.Blocks{block4, block3},
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{
				block3.ID: mark3,
			},
		},
		"query range within a single block": {
			minT:           block3.MinTime + 2,
			maxT:           block3.MaxTime - 2,
			expectedBlocks: bucketindex.Blocks{block3},
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{
				block3.ID: mark3,
			},
		},
		"query range within multiple blocks": {
			minT:           13,
			maxT:           16,
			expectedBlocks: bucketindex.Blocks{block2, block1},
			expectedMarks:  map[ulid.ULID]*bucketindex.BlockDeletionMark{},
		},
		"query range matching exactly a single block": {
			minT:           block3.MinTime,
			maxT:           block3.MaxTime - 1,
			expectedBlocks: bucketindex.Blocks{block3},
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{
				block3.ID: mark3,
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			blocks, deletionMarks, err := finder.GetBlocks(ctx, userID, testData.minT, testData.maxT)
			require.NoError(t, err)
			require.ElementsMatch(t, testData.expectedBlocks, blocks)
			require.Equal(t, testData.expectedMarks, deletionMarks)
		})
	}
}

func BenchmarkBucketIndexBlocksFinder_GetBlocks(b *testing.B) {
	const (
		numBlocks        = 1000
		numDeletionMarks = 100
		userID           = "user-1"
	)

	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(b)

	// Mock a bucket index.
	idx := &bucketindex.Index{
		Version:   bucketindex.IndexVersion1,
		UpdatedAt: time.Now().Unix(),
	}

	for i := 1; i <= numBlocks; i++ {
		id := ulid.MustNew(uint64(i), nil)
		minT := int64(i * 10)
		maxT := int64((i + 1) * 10)
		idx.Blocks = append(idx.Blocks, &bucketindex.Block{ID: id, MinTime: minT, MaxTime: maxT})
	}
	for i := 1; i <= numDeletionMarks; i++ {
		id := ulid.MustNew(uint64(i), nil)
		idx.BlockDeletionMarks = append(idx.BlockDeletionMarks, &bucketindex.BlockDeletionMark{ID: id, DeletionTime: time.Now().Unix()})
	}
	require.NoError(b, bucketindex.WriteIndex(ctx, bkt, userID, nil, idx))
	finder := prepareBucketIndexBlocksFinder(b, bkt)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		blocks, marks, err := finder.GetBlocks(ctx, userID, 100, 200)
		if err != nil || len(blocks) != 11 || len(marks) != 11 {
			b.Fail()
		}
	}
}

func TestBucketIndexBlocksFinder_GetBlocks_BucketIndexDoesNotExist(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	finder := prepareBucketIndexBlocksFinder(t, bkt)

	blocks, deletionMarks, err := finder.GetBlocks(ctx, userID, 10, 20)
	require.NoError(t, err)
	assert.Empty(t, blocks)
	assert.Empty(t, deletionMarks)
}

func TestBucketIndexBlocksFinder_GetBlocks_BucketIndexIsCorrupted(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	finder := prepareBucketIndexBlocksFinder(t, bkt)

	// Upload a corrupted bucket index.
	require.NoError(t, bkt.Upload(ctx, path.Join(userID, bucketindex.IndexCompressedFilename), strings.NewReader("invalid}!")))

	_, _, err := finder.GetBlocks(ctx, userID, 10, 20)
	require.Equal(t, bucketindex.ErrIndexCorrupted, err)
}

func TestBucketIndexBlocksFinder_GetBlocks_BucketIndexIsTooOld(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	finder := prepareBucketIndexBlocksFinder(t, bkt)

	require.NoError(t, bucketindex.WriteIndex(ctx, bkt, userID, nil, &bucketindex.Index{
		Version:            bucketindex.IndexVersion1,
		Blocks:             bucketindex.Blocks{},
		BlockDeletionMarks: bucketindex.BlockDeletionMarks{},
		UpdatedAt:          time.Now().Add(-2 * time.Hour).Unix(),
	}))

	_, _, err := finder.GetBlocks(ctx, userID, 10, 20)
	require.Equal(t, errBucketIndexTooOld, err)
}

func prepareBucketIndexBlocksFinder(t testing.TB, bkt objstore.Bucket) *BucketIndexBlocksFinder {
	ctx := context.Background()
	cfg := BucketIndexBlocksFinderConfig{
		IndexLoader: bucketindex.LoaderConfig{
			CheckInterval:         time.Minute,
			UpdateOnStaleInterval: time.Minute,
			UpdateOnErrorInterval: time.Minute,
			IdleTimeout:           time.Minute,
		},
		MaxStalePeriod:           time.Hour,
		IgnoreDeletionMarksDelay: time.Hour,
	}

	finder := NewBucketIndexBlocksFinder(cfg, bkt, nil, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, finder))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, finder))
	})

	return finder
}
