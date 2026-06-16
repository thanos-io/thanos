// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package parquet

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestConfigValidate(t *testing.T) {
	mockBucket := newMockBucket()

	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: Config{
				Enabled:              true,
				TSDBBucket:           mockBucket,
				ParquetBucket:        mockBucket,
				BasePath:             "parquet/",
				TargetSeriesPerShard: 1000000,
				RowGroupSize:         1000000,
				DownloadConcurrency:  4,
				EncodingConcurrency:  4,
				UploadConcurrency:    4,
			},
			expectError: false,
		},
		{
			name: "missing TSDBBucket",
			config: Config{
				Enabled:              true,
				ParquetBucket:        mockBucket,
				TargetSeriesPerShard: 1000000,
				RowGroupSize:         1000000,
				DownloadConcurrency:  4,
				EncodingConcurrency:  4,
				UploadConcurrency:    4,
			},
			expectError: true,
			errorMsg:    "TSDBBucket is required",
		},
		{
			name: "missing ParquetBucket",
			config: Config{
				Enabled:              true,
				TSDBBucket:           mockBucket,
				TargetSeriesPerShard: 1000000,
				RowGroupSize:         1000000,
				DownloadConcurrency:  4,
				EncodingConcurrency:  4,
				UploadConcurrency:    4,
			},
			expectError: true,
			errorMsg:    "ParquetBucket is required",
		},
		{
			name: "invalid TargetSeriesPerShard",
			config: Config{
				Enabled:              true,
				TSDBBucket:           mockBucket,
				ParquetBucket:        mockBucket,
				TargetSeriesPerShard: 0,
				RowGroupSize:         1000000,
				DownloadConcurrency:  4,
				EncodingConcurrency:  4,
				UploadConcurrency:    4,
			},
			expectError: true,
			errorMsg:    "TargetSeriesPerShard must be positive",
		},
		{
			name: "invalid RowGroupSize",
			config: Config{
				Enabled:              true,
				TSDBBucket:           mockBucket,
				ParquetBucket:        mockBucket,
				TargetSeriesPerShard: 1000000,
				RowGroupSize:         0,
				DownloadConcurrency:  4,
				EncodingConcurrency:  4,
				UploadConcurrency:    4,
			},
			expectError: true,
			errorMsg:    "RowGroupSize must be positive",
		},
		{
			name: "invalid DownloadConcurrency",
			config: Config{
				Enabled:              true,
				TSDBBucket:           mockBucket,
				ParquetBucket:        mockBucket,
				TargetSeriesPerShard: 1000000,
				RowGroupSize:         1000000,
				DownloadConcurrency:  0,
				EncodingConcurrency:  4,
				UploadConcurrency:    4,
			},
			expectError: true,
			errorMsg:    "DownloadConcurrency must be positive",
		},
		{
			name: "invalid EncodingConcurrency",
			config: Config{
				Enabled:              true,
				TSDBBucket:           mockBucket,
				ParquetBucket:        mockBucket,
				TargetSeriesPerShard: 1000000,
				RowGroupSize:         1000000,
				DownloadConcurrency:  4,
				EncodingConcurrency:  0,
				UploadConcurrency:    4,
			},
			expectError: true,
			errorMsg:    "EncodingConcurrency must be positive",
		},
		{
			name: "invalid UploadConcurrency",
			config: Config{
				Enabled:              true,
				TSDBBucket:           mockBucket,
				ParquetBucket:        mockBucket,
				TargetSeriesPerShard: 1000000,
				RowGroupSize:         1000000,
				DownloadConcurrency:  4,
				EncodingConcurrency:  4,
				UploadConcurrency:    0,
			},
			expectError: true,
			errorMsg:    "UploadConcurrency must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				testutil.NotOk(t, err)
				testutil.Assert(t, err.Error() != "", "error should have message")
			} else {
				testutil.Ok(t, err)
			}
		})
	}
}
