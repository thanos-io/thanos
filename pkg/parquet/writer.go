// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package parquet

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	ulidv2 "github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// Writer is the interface for writing TSDB blocks to Parquet format.
type Writer interface {
	// WriteBlock converts a TSDB block to Parquet format and uploads it to the configured bucket.
	WriteBlock(ctx context.Context, blockID ulidv2.ULID, meta *metadata.Meta) error
	// HasParquetFiles checks if Parquet files exist for a given block.
	HasParquetFiles(ctx context.Context, meta *metadata.Meta) (bool, error)
	// Reconcile scans blocks and converts any 8h blocks missing Parquet files.
	Reconcile(ctx context.Context, blocks map[ulidv2.ULID]*metadata.Meta) (converted int, err error)
	// Close closes the writer and releases resources.
	Close() error
}

// writer implements the Writer interface.
type writer struct {
	config  Config
	logger  log.Logger
	metrics *Metrics
}

// New creates a new Parquet writer.
func New(config Config, logger log.Logger, reg prometheus.Registerer) (Writer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &writer{
		config:  config,
		logger:  logger,
		metrics: NewMetrics(reg),
	}, nil
}

// WriteBlock converts a TSDB block to Parquet format.
func (w *writer) WriteBlock(ctx context.Context, blockID ulidv2.ULID, meta *metadata.Meta) error {
	if !w.config.Enabled {
		return nil
	}

	// Convert ulid/v2 to ulid for internal use
	blockIDV1 := ulid.ULID(blockID)

	err := ConvertTSDBBlock(
		ctx,
		w.logger,
		blockIDV1,
		meta,
		w.config.TSDBBucket,
		w.config.ParquetBucket,
		w.config.BasePath,
		w.config,
		w.metrics,
	)

	if err != nil {
		w.metrics.conversionsTotal.WithLabelValues("failure").Inc()
		return ErrConversion("failed to convert block", err)
	}

	return nil
}

// HasParquetFiles checks if Parquet files exist for a given block.
// It checks for the existence of any .parquet files in the expected date directory.
func (w *writer) HasParquetFiles(ctx context.Context, meta *metadata.Meta) (bool, error) {
	if !w.config.Enabled {
		return false, nil
	}

	// Determine the expected Parquet file path based on block's time range
	// Single day: 2026-03-28/  Multi-day: 2026-03-28_to_2026-03-30/
	blockDateStart := time.UnixMilli(meta.MinTime).UTC().Format("2006-01-02")
	blockDateEnd := time.UnixMilli(meta.MaxTime).UTC().Format("2006-01-02")
	var dateFolder string
	if blockDateStart == blockDateEnd {
		dateFolder = blockDateStart
	} else {
		dateFolder = fmt.Sprintf("%s_to_%s", blockDateStart, blockDateEnd)
	}
	parquetDir := filepath.Join(w.config.BasePath, dateFolder)

	// Check if any .parquet files exist in this directory
	// We look for both labels.parquet and chunks.parquet files
	// Since blocks can be sharded, we check for any shard (0.labels.parquet, 1.labels.parquet, etc.)
	found := false
	err := w.config.ParquetBucket.Iter(ctx, parquetDir, func(name string) error {
		if filepath.Ext(name) == ".parquet" {
			found = true
			return errors.New("found") // Stop iteration
		}
		return nil
	})

	// If we got "found" error, it means we found at least one parquet file
	if err != nil && err.Error() == "found" {
		return true, nil
	}

	if err != nil {
		return false, err
	}

	return found, nil
}

// Reconcile scans blocks and converts any 8h blocks that are missing Parquet files.
// This is used for disaster recovery when the compactor crashes after creating a block
// but before converting it to Parquet.
func (w *writer) Reconcile(ctx context.Context, blocks map[ulidv2.ULID]*metadata.Meta) (int, error) {
	if !w.config.Enabled {
		return 0, nil
	}

	var converted int
	var reconErrors []error

	for blockID, meta := range blocks {
		// Only check exactly 8-hour blocks (compaction level 2)
		blockDuration := time.Duration(meta.MaxTime-meta.MinTime) * time.Millisecond
		if blockDuration != 8*time.Hour {
			continue
		}

		// Check if Parquet files already exist
		hasParquet, err := w.HasParquetFiles(ctx, meta)
		if err != nil {
			reconErrors = append(reconErrors, errors.Wrapf(err, "check parquet files for block %s", blockID))
			continue
		}

		if hasParquet {
			// Already converted, skip
			continue
		}

		// Convert the block
		level.Info(w.logger).Log("msg", "reconciling unconverted block to Parquet", "block", blockID, "duration", blockDuration)
		if err := w.WriteBlock(ctx, blockID, meta); err != nil {
			reconErrors = append(reconErrors, errors.Wrapf(err, "convert block %s during reconciliation", blockID))
			continue
		}

		converted++
		level.Info(w.logger).Log("msg", "successfully reconciled block to Parquet", "block", blockID)
	}

	if len(reconErrors) > 0 {
		return converted, errors.Errorf("reconciliation completed with %d errors: %v", len(reconErrors), reconErrors)
	}

	return converted, nil
}

// Close closes the writer.
func (w *writer) Close() error {
	// No resources to clean up currently
	return nil
}
