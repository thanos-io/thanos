// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package parquet

import (
	"github.com/thanos-io/objstore"
)

// Config holds the configuration for Parquet writing.
type Config struct {
	// Enabled controls whether Parquet writing is active.
	Enabled bool

	// TSDBBucket is the object storage bucket to read TSDB blocks from.
	TSDBBucket objstore.Bucket

	// ParquetBucket is the object storage bucket to write Parquet files to.
	// Can be the same as TSDBBucket or separate.
	ParquetBucket objstore.Bucket

	// BasePath is the base path within the bucket for Parquet files (e.g., "parquet/").
	BasePath string

	// ShardingEnabled enables label-based sharding for high-cardinality data.
	ShardingEnabled bool

	// TargetSeriesPerShard is the target number of series per Parquet shard.
	TargetSeriesPerShard int

	// SortLabels is the list of labels to sort by in Parquet files.
	SortLabels []string

	// CompressionCodec is the compression codec to use ("zstd", "snappy", "none").
	CompressionCodec string

	// RowGroupSize is the number of rows per Parquet row group.
	RowGroupSize int

	// DownloadConcurrency controls parallel block downloads.
	DownloadConcurrency int

	// EncodingConcurrency controls parallel Parquet encoding.
	EncodingConcurrency int

	// UploadConcurrency controls parallel file uploads.
	UploadConcurrency int

	// TempDir is the directory to use for temporary files during conversion.
	// If empty, uses system default temp directory.
	TempDir string
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.TSDBBucket == nil {
		return ErrInvalidConfig("TSDBBucket is required")
	}
	if c.ParquetBucket == nil {
		return ErrInvalidConfig("ParquetBucket is required")
	}
	if c.TargetSeriesPerShard <= 0 {
		return ErrInvalidConfig("TargetSeriesPerShard must be positive")
	}
	if c.RowGroupSize <= 0 {
		return ErrInvalidConfig("RowGroupSize must be positive")
	}
	if c.DownloadConcurrency <= 0 {
		return ErrInvalidConfig("DownloadConcurrency must be positive")
	}
	if c.EncodingConcurrency <= 0 {
		return ErrInvalidConfig("EncodingConcurrency must be positive")
	}
	if c.UploadConcurrency <= 0 {
		return ErrInvalidConfig("UploadConcurrency must be positive")
	}
	return nil
}
