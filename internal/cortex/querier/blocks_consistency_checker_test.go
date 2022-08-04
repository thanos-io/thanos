// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package querier

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/thanos-io/thanos/internal/cortex/storage/tsdb/bucketindex"
	"github.com/thanos-io/thanos/internal/cortex/util"
)

func TestBlocksConsistencyChecker_Check(t *testing.T) {
	now := time.Now()
	uploadGracePeriod := 10 * time.Minute
	deletionGracePeriod := 5 * time.Minute

	block1 := ulid.MustNew(uint64(util.TimeToMillis(now.Add(-uploadGracePeriod*2))), nil)
	block2 := ulid.MustNew(uint64(util.TimeToMillis(now.Add(-uploadGracePeriod*3))), nil)
	block3 := ulid.MustNew(uint64(util.TimeToMillis(now.Add(-uploadGracePeriod*4))), nil)

	tests := map[string]struct {
		knownBlocks           bucketindex.Blocks
		knownDeletionMarks    map[ulid.ULID]*bucketindex.BlockDeletionMark
		queriedBlocks         []ulid.ULID
		expectedMissingBlocks []ulid.ULID
	}{
		"no known blocks": {
			knownBlocks:        bucketindex.Blocks{},
			knownDeletionMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{},
			queriedBlocks:      []ulid.ULID{},
		},
		"all known blocks have been queried from a single store-gateway": {
			knownBlocks: bucketindex.Blocks{
				{ID: block1, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block2, UploadedAt: now.Add(-time.Hour).Unix()},
			},
			knownDeletionMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{},
			queriedBlocks:      []ulid.ULID{block1, block2},
		},
		"all known blocks have been queried from multiple store-gateway": {
			knownBlocks: bucketindex.Blocks{
				{ID: block1, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block2, UploadedAt: now.Add(-time.Hour).Unix()},
			},
			knownDeletionMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{},
			queriedBlocks:      []ulid.ULID{block1, block2},
		},
		"store-gateway has queried more blocks than expected": {
			knownBlocks: bucketindex.Blocks{
				{ID: block1, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block2, UploadedAt: now.Add(-time.Hour).Unix()},
			},
			knownDeletionMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{},
			queriedBlocks:      []ulid.ULID{block1, block2, block3},
		},
		"store-gateway has queried less blocks than expected": {
			knownBlocks: bucketindex.Blocks{
				{ID: block1, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block2, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block3, UploadedAt: now.Add(-time.Hour).Unix()},
			},
			knownDeletionMarks:    map[ulid.ULID]*bucketindex.BlockDeletionMark{},
			queriedBlocks:         []ulid.ULID{block1, block3},
			expectedMissingBlocks: []ulid.ULID{block2},
		},
		"store-gateway has queried less blocks than expected, but the missing block has been recently uploaded": {
			knownBlocks: bucketindex.Blocks{
				{ID: block1, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block2, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block3, UploadedAt: now.Add(-uploadGracePeriod).Add(time.Minute).Unix()},
			},
			knownDeletionMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{},
			queriedBlocks:      []ulid.ULID{block1, block2},
		},
		"store-gateway has queried less blocks than expected and the missing block has been recently marked for deletion": {
			knownBlocks: bucketindex.Blocks{
				{ID: block1, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block2, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block3, UploadedAt: now.Add(-time.Hour).Unix()},
			},
			knownDeletionMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{
				block3: {DeletionTime: now.Add(-deletionGracePeriod / 2).Unix()},
			},
			queriedBlocks:         []ulid.ULID{block1, block2},
			expectedMissingBlocks: []ulid.ULID{block3},
		},
		"store-gateway has queried less blocks than expected and the missing block has been marked for deletion long time ago": {
			knownBlocks: bucketindex.Blocks{
				{ID: block1, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block2, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block3, UploadedAt: now.Add(-time.Hour).Unix()},
			},
			knownDeletionMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{
				block3: {DeletionTime: now.Add(-deletionGracePeriod * 2).Unix()},
			},
			queriedBlocks: []ulid.ULID{block1, block2},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			c := NewBlocksConsistencyChecker(uploadGracePeriod, deletionGracePeriod, log.NewNopLogger(), reg)

			missingBlocks := c.Check(testData.knownBlocks, testData.knownDeletionMarks, testData.queriedBlocks)
			assert.Equal(t, testData.expectedMissingBlocks, missingBlocks)
			assert.Equal(t, float64(1), testutil.ToFloat64(c.checksTotal))

			if len(testData.expectedMissingBlocks) > 0 {
				assert.Equal(t, float64(1), testutil.ToFloat64(c.checksFailed))
			} else {
				assert.Equal(t, float64(0), testutil.ToFloat64(c.checksFailed))
			}
		})
	}
}
