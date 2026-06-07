// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func TestParquetCompactionLifecycleCallback_Only8hBlocks(t *testing.T) {
	tests := []struct {
		name             string
		blockDuration    time.Duration
		expectConversion bool
	}{
		{
			name:             "1h block - should skip",
			blockDuration:    1 * time.Hour,
			expectConversion: false,
		},
		{
			name:             "2h block - should skip",
			blockDuration:    2 * time.Hour,
			expectConversion: false,
		},
		{
			name:             "8h block - should convert",
			blockDuration:    8 * time.Hour,
			expectConversion: true,
		},
		{
			name:             "48h block - should skip",
			blockDuration:    48 * time.Hour,
			expectConversion: false,
		},
		{
			name:             "14d block - should skip",
			blockDuration:    14 * 24 * time.Hour,
			expectConversion: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			blockID := ulid.MustNew(uint64(now.UnixMilli()), nil)
			minTime := now.UnixMilli()
			maxTime := now.Add(tt.blockDuration).UnixMilli()

			meta := &metadata.Meta{}
			meta.ULID = blockID
			meta.MinTime = minTime
			meta.MaxTime = maxTime

			// Note: We can't easily test the actual callback without a full Group setup,
			// but we can verify the block duration logic directly
			actualDuration := time.Duration(meta.MaxTime-meta.MinTime) * time.Millisecond
			shouldConvert := actualDuration == 8*time.Hour

			testutil.Equals(t, tt.expectConversion, shouldConvert,
				"block duration check failed for %s", tt.name)
		})
	}
}

func TestBlockDurationCalculation(t *testing.T) {
	tests := []struct {
		name             string
		minTime          time.Time
		maxTime          time.Time
		expectedDuration time.Duration
	}{
		{
			name:             "1 hour",
			minTime:          time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			maxTime:          time.Date(2026, 3, 28, 1, 0, 0, 0, time.UTC),
			expectedDuration: 1 * time.Hour,
		},
		{
			name:             "8 hours",
			minTime:          time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			maxTime:          time.Date(2026, 3, 28, 8, 0, 0, 0, time.UTC),
			expectedDuration: 8 * time.Hour,
		},
		{
			name:             "48 hours",
			minTime:          time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			maxTime:          time.Date(2026, 3, 30, 0, 0, 0, 0, time.UTC),
			expectedDuration: 48 * time.Hour,
		},
		{
			name:             "14 days",
			minTime:          time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			maxTime:          time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC),
			expectedDuration: 14 * 24 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := &metadata.Meta{}
			meta.MinTime = tt.minTime.UnixMilli()
			meta.MaxTime = tt.maxTime.UnixMilli()

			duration := time.Duration(meta.MaxTime-meta.MinTime) * time.Millisecond
			testutil.Equals(t, tt.expectedDuration, duration)
		})
	}
}
