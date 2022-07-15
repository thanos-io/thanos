// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package bucketindex

import (
	"context"
	"path"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/internal/cortex/storage/tsdb/testutil"
	cortex_testutil "github.com/thanos-io/thanos/internal/cortex/storage/tsdb/testutil"
)

func TestReadIndex_ShouldReturnErrorIfIndexDoesNotExist(t *testing.T) {
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	idx, err := ReadIndex(context.Background(), bkt, "user-1", nil, log.NewNopLogger())
	require.Equal(t, ErrIndexNotFound, err)
	require.Nil(t, idx)
}

func TestReadIndex_ShouldReturnErrorIfIndexIsCorrupted(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Write a corrupted index.
	require.NoError(t, bkt.Upload(ctx, path.Join(userID, IndexCompressedFilename), strings.NewReader("invalid!}")))

	idx, err := ReadIndex(ctx, bkt, userID, nil, log.NewNopLogger())
	require.Equal(t, ErrIndexCorrupted, err)
	require.Nil(t, idx)
}

func TestReadIndex_ShouldReturnTheParsedIndexOnSuccess(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	logger := log.NewNopLogger()

	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Mock some blocks in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	testutil.MockStorageBlock(t, bkt, userID, 20, 30)
	testutil.MockStorageDeletionMark(t, bkt, userID, testutil.MockStorageBlock(t, bkt, userID, 30, 40))

	// Write the index.
	u := NewUpdater(bkt, userID, nil, logger)
	expectedIdx, _, _, err := u.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, WriteIndex(ctx, bkt, userID, nil, expectedIdx))

	// Read it back and compare.
	actualIdx, err := ReadIndex(ctx, bkt, userID, nil, logger)
	require.NoError(t, err)
	assert.Equal(t, expectedIdx, actualIdx)
}

func BenchmarkReadIndex(b *testing.B) {
	const (
		numBlocks             = 1000
		numBlockDeletionMarks = 100
		userID                = "user-1"
	)

	ctx := context.Background()
	logger := log.NewNopLogger()

	bkt, _ := cortex_testutil.PrepareFilesystemBucket(b)

	// Mock some blocks and deletion marks in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	for i := 0; i < numBlocks; i++ {
		minT := int64(i * 10)
		maxT := int64((i + 1) * 10)

		block := testutil.MockStorageBlock(b, bkt, userID, minT, maxT)

		if i < numBlockDeletionMarks {
			testutil.MockStorageDeletionMark(b, bkt, userID, block)
		}
	}

	// Write the index.
	u := NewUpdater(bkt, userID, nil, logger)
	idx, _, _, err := u.UpdateIndex(ctx, nil)
	require.NoError(b, err)
	require.NoError(b, WriteIndex(ctx, bkt, userID, nil, idx))

	// Read it back once just to make sure the index contains the expected data.
	idx, err = ReadIndex(ctx, bkt, userID, nil, logger)
	require.NoError(b, err)
	require.Len(b, idx.Blocks, numBlocks)
	require.Len(b, idx.BlockDeletionMarks, numBlockDeletionMarks)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, err := ReadIndex(ctx, bkt, userID, nil, logger)
		require.NoError(b, err)
	}
}

func TestDeleteIndex_ShouldNotReturnErrorIfIndexDoesNotExist(t *testing.T) {
	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	assert.NoError(t, DeleteIndex(ctx, bkt, "user-1", nil))
}
