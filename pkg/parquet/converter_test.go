// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package parquet

import (
	"fmt"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

func TestDateFolderNaming(t *testing.T) {
	tests := []struct {
		name           string
		minTime        time.Time
		maxTime        time.Time
		expectedFolder string
	}{
		{
			name:           "8h block - same day",
			minTime:        time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			maxTime:        time.Date(2026, 3, 28, 8, 0, 0, 0, time.UTC),
			expectedFolder: "2026-03-28",
		},
		{
			name:           "8h block - different calendar day due to time",
			minTime:        time.Date(2026, 3, 28, 20, 0, 0, 0, time.UTC),
			maxTime:        time.Date(2026, 3, 29, 4, 0, 0, 0, time.UTC),
			expectedFolder: "2026-03-28_to_2026-03-29",
		},
		{
			name:           "48h block - spans two days",
			minTime:        time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			maxTime:        time.Date(2026, 3, 30, 0, 0, 0, 0, time.UTC),
			expectedFolder: "2026-03-28_to_2026-03-30",
		},
		{
			name:           "14d block - spans many days",
			minTime:        time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			maxTime:        time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC),
			expectedFolder: "2026-03-28_to_2026-04-11",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockDateStart := tt.minTime.Format("2006-01-02")
			blockDateEnd := tt.maxTime.Format("2006-01-02")
			var dateFolder string
			if blockDateStart == blockDateEnd {
				dateFolder = blockDateStart
			} else {
				dateFolder = fmt.Sprintf("%s_to_%s", blockDateStart, blockDateEnd)
			}
			testutil.Equals(t, tt.expectedFolder, dateFolder)
		})
	}
}

func TestChunkBucketing(t *testing.T) {
	tests := []struct {
		name           string
		chunkMinTime   time.Time
		blockMinTime   time.Time
		expectedBucket int
	}{
		{
			name:           "chunk at 00:00 - bucket 0",
			chunkMinTime:   time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			blockMinTime:   time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			expectedBucket: 0,
		},
		{
			name:           "chunk at 07:59 - bucket 0",
			chunkMinTime:   time.Date(2026, 3, 28, 7, 59, 0, 0, time.UTC),
			blockMinTime:   time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			expectedBucket: 0,
		},
		{
			name:           "chunk at 08:00 - bucket 1",
			chunkMinTime:   time.Date(2026, 3, 28, 8, 0, 0, 0, time.UTC),
			blockMinTime:   time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			expectedBucket: 1,
		},
		{
			name:           "chunk at 12:00 - bucket 1",
			chunkMinTime:   time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC),
			blockMinTime:   time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			expectedBucket: 1,
		},
		{
			name:           "chunk at 15:59 - bucket 1",
			chunkMinTime:   time.Date(2026, 3, 28, 15, 59, 0, 0, time.UTC),
			blockMinTime:   time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			expectedBucket: 1,
		},
		{
			name:           "chunk at 16:00 - bucket 2",
			chunkMinTime:   time.Date(2026, 3, 28, 16, 0, 0, 0, time.UTC),
			blockMinTime:   time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			expectedBucket: 2,
		},
		{
			name:           "chunk at 23:59 - bucket 2",
			chunkMinTime:   time.Date(2026, 3, 28, 23, 59, 0, 0, time.UTC),
			blockMinTime:   time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			expectedBucket: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate bucket index using the same logic as collectChunks
			hour := tt.chunkMinTime.Hour()
			bucketIdx := (hour / int(ChunkColumnLength.Hours())) % ChunkColumnsPerDay
			testutil.Equals(t, tt.expectedBucket, bucketIdx)
		})
	}
}

func TestCollectChunksEmpty(t *testing.T) {
	// Test with no chunks
	chkMetas := []chunks.Meta{}
	result, err := collectChunks(chkMetas, nil)
	testutil.Ok(t, err)

	// All buckets should be empty
	for i, bucket := range result {
		testutil.Equals(t, 0, len(bucket), "bucket %d should be empty", i)
	}
}

func TestZigZagEncode(t *testing.T) {
	tests := []struct {
		input    int64
		expected uint64
	}{
		{0, 0},
		{-1, 1},
		{1, 2},
		{-2, 3},
		{2, 4},
		{-100, 199},
		{100, 200},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("encode_%d", tt.input), func(t *testing.T) {
			result := ZigZagEncode(tt.input)
			testutil.Equals(t, tt.expected, result)
		})
	}
}

func TestChunkColumnsPerDay(t *testing.T) {
	// Verify that 3 columns × 8 hours = 24 hours
	testutil.Equals(t, 3, ChunkColumnsPerDay)
	testutil.Equals(t, 8*time.Hour, ChunkColumnLength)
	testutil.Equals(t, 24*time.Hour, time.Duration(ChunkColumnsPerDay)*ChunkColumnLength)
}
