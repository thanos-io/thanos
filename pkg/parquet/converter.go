// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package parquet

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	ulidv2 "github.com/oklog/ulid/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/logutil"
)

// ZigZagEncode encodes a signed int64 to an unsigned int64 using zigzag encoding.
func ZigZagEncode(x int64) uint64 {
	return uint64(uint64(x<<1) ^ uint64((int64(x) >> 63)))
}

// collectChunks collects and encodes chunks for a series into the 3 time buckets (8 hours each).
// Returns an array of 3 byte arrays, one for each 8-hour period.
func collectChunks(chkMetas []chunks.Meta, chunkReader tsdb.ChunkReader) ([ChunkColumnsPerDay][]byte, error) {
	var result [ChunkColumnsPerDay][]byte

	// Sort chunks by MinTime (they may come from different blocks)
	sort.Slice(chkMetas, func(i, j int) bool {
		return chkMetas[i].MinTime < chkMetas[j].MinTime
	})

	// Encode each chunk into the appropriate 8-hour bucket
	for _, chkMeta := range chkMetas {
		// Read the chunk data using the chunk reference
		chk, iterable, err := chunkReader.ChunkOrIterable(chkMeta)
		if err != nil {
			return result, fmt.Errorf("read chunk: %w", err)
		}

		// If we got an iterable, we need to encode it
		if iterable != nil {
			// Skip - we need the actual chunk bytes
			continue
		}

		if chk.NumSamples() == 0 {
			continue
		}

		// Determine which 8-hour bucket this chunk belongs to
		chunkTime := time.UnixMilli(chkMeta.MinTime).UTC()
		hour := chunkTime.Hour()
		bucketIdx := (hour / int(ChunkColumnLength.Hours())) % ChunkColumnsPerDay

		// Encode chunk: encoding type + min time + max time + length + data
		enc := chk.Encoding()
		bs := chk.Bytes()

		chkBytes := result[bucketIdx]
		chkBytes = binary.BigEndian.AppendUint32(chkBytes, uint32(enc))
		chkBytes = binary.BigEndian.AppendUint64(chkBytes, ZigZagEncode(chkMeta.MinTime))
		chkBytes = binary.BigEndian.AppendUint64(chkBytes, ZigZagEncode(chkMeta.MaxTime))
		chkBytes = binary.BigEndian.AppendUint32(chkBytes, uint32(len(bs)))
		chkBytes = append(chkBytes, bs...)
		result[bucketIdx] = chkBytes
	}

	return result, nil
}

// ConvertTSDBBlock converts a TSDB block to Parquet format and uploads it to the bucket.
func ConvertTSDBBlock(
	ctx context.Context,
	logger log.Logger,
	blockID ulid.ULID,
	meta *metadata.Meta,
	tsdbBucket objstore.Bucket,
	parquetBucket objstore.Bucket,
	basePath string,
	cfg Config,
	metrics *Metrics,
) error {
	startTime := time.Now()

	level.Info(logger).Log("msg", "starting Parquet conversion", "block", blockID.String())

	// Create a temporary directory for downloading the block
	// Use cfg.TempDir if specified, otherwise use system default
	tmpDir, err := os.MkdirTemp(cfg.TempDir, fmt.Sprintf("parquet-convert-%s-*", blockID.String()))
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			level.Warn(logger).Log("msg", "failed to cleanup temp dir", "dir", tmpDir, "err", err)
		}
	}()

	// Download the TSDB block
	level.Debug(logger).Log("msg", "downloading TSDB block", "block", blockID.String())
	// Convert ulid to ulid/v2
	blockIDV2 := ulidv2.ULID(blockID)
	if err := block.Download(ctx, logger, tsdbBucket, blockIDV2, tmpDir); err != nil {
		return fmt.Errorf("download block: %w", err)
	}

	// Open the TSDB block
	// Note: block.Download() downloads files directly into tmpDir, not into tmpDir/<blockID>/
	// Convert go-kit logger to slog
	slogger := logutil.GoKitLogToSlog(logger)
	blk, err := tsdb.OpenBlock(slogger, tmpDir, chunkenc.NewPool(), tsdb.DefaultPostingsDecoderFactory)
	if err != nil {
		return fmt.Errorf("open block: %w", err)
	}
	defer func() {
		if err := blk.Close(); err != nil {
			level.Warn(logger).Log("msg", "failed to close block", "err", err)
		}
	}()

	// Get block readers
	indexReader, err := blk.Index()
	if err != nil {
		return fmt.Errorf("get index reader: %w", err)
	}
	defer indexReader.Close()

	chunkReader, err := blk.Chunks()
	if err != nil {
		return fmt.Errorf("get chunk reader: %w", err)
	}
	defer chunkReader.Close()

	// Read all series from the block
	level.Debug(logger).Log("msg", "reading series from block")
	allPostingsKeyName, allPostingsKeyValue := index.AllPostingsKey()
	postings, err := indexReader.Postings(ctx, allPostingsKeyName, allPostingsKeyValue)
	if err != nil {
		return fmt.Errorf("get postings: %w", err)
	}

	var allSeries []labels.Labels
	var allChunks [][]chunks.Meta
	var builder labels.ScratchBuilder

	for postings.Next() {
		var chkMetas []chunks.Meta
		if err := indexReader.Series(postings.At(), &builder, &chkMetas); err != nil {
			return fmt.Errorf("read series: %w", err)
		}

		lbls := builder.Labels()
		allSeries = append(allSeries, lbls)
		allChunks = append(allChunks, chkMetas)
	}

	if err := postings.Err(); err != nil {
		return fmt.Errorf("postings iteration: %w", err)
	}

	level.Info(logger).Log("msg", "read series from block", "series_count", len(allSeries))

	if len(allSeries) == 0 {
		level.Warn(logger).Log("msg", "block has no series, skipping conversion")
		return nil
	}

	// Shard the series
	shards, err := ShardSeries(allSeries, cfg.TargetSeriesPerShard, cfg.SortLabels)
	if err != nil {
		return fmt.Errorf("shard series: %w", err)
	}

	level.Info(logger).Log("msg", "sharded series", "num_shards", len(shards))
	metrics.shardsCreated.Observe(float64(len(shards)))

	// Convert each shard to Parquet
	for _, shard := range shards {
		level.Debug(logger).Log("msg", "converting shard", "shard", shard.Index, "series_count", len(shard.Series))

		if err := convertShard(ctx, logger, blockID, meta, shard, allChunks, chunkReader, parquetBucket, basePath, cfg, metrics); err != nil {
			return fmt.Errorf("convert shard %d: %w", shard.Index, err)
		}

		metrics.seriesPerShard.Observe(float64(len(shard.Series)))
	}

	duration := time.Since(startTime).Seconds()
	metrics.conversionDuration.Observe(duration)
	metrics.conversionsTotal.WithLabelValues("success").Inc()

	level.Info(logger).Log("msg", "completed Parquet conversion", "block", blockID.String(), "duration", duration, "shards", len(shards))

	return nil
}

// convertShard converts a single shard to Parquet files (labels.parquet and chunks.parquet).
func convertShard(
	ctx context.Context,
	logger log.Logger,
	blockID ulid.ULID,
	meta *metadata.Meta,
	shard Shard,
	allChunks [][]chunks.Meta,
	chunkReader tsdb.ChunkReader,
	parquetBucket objstore.Bucket,
	basePath string,
	cfg Config,
	metrics *Metrics,
) error {
	// Build schema from shard's label names
	schema := BuildSchemaFromLabels(shard.LabelNames)
	compressedSchema := WithCompression(schema)

	// Create temporary files for labels and chunks
	// Use cfg.TempDir if specified, otherwise use system default
	labelsFile, err := os.CreateTemp(cfg.TempDir, fmt.Sprintf("labels-%s-shard%d-*.parquet", blockID.String(), shard.Index))
	if err != nil {
		return fmt.Errorf("create labels temp file: %w", err)
	}
	defer os.Remove(labelsFile.Name())
	defer labelsFile.Close()

	chunksFile, err := os.CreateTemp(cfg.TempDir, fmt.Sprintf("chunks-%s-shard%d-*.parquet", blockID.String(), shard.Index))
	if err != nil {
		return fmt.Errorf("create chunks temp file: %w", err)
	}
	defer os.Remove(chunksFile.Name())
	defer chunksFile.Close()

	// Create Parquet writers
	labelsSchema := LabelsProjection(compressedSchema)
	chunksSchema := ChunkProjection(compressedSchema)

	// Create labels writer (sorting handled during write, not at schema level)
	labelsWriter := parquet.NewGenericWriter[map[string]any](
		labelsFile,
		labelsSchema,
	)
	defer labelsWriter.Close()

	chunksWriter := parquet.NewGenericWriter[map[string]any](
		chunksFile,
		chunksSchema,
	)
	defer chunksWriter.Close()

	// Write each series in the shard
	for _, seriesWithLabels := range shard.Series {
		lbls := seriesWithLabels.Labels
		hash := lbls.Hash()

		// Build label row
		labelRow := make(map[string]any)
		labelRow[LabelHashColumn] = int64(hash)
		labelRow[LabelIndexColumn] = []byte{} // Simplified: empty index for now

		lbls.Range(func(l labels.Label) {
			labelRow[LabelNameToColumn(l.Name)] = l.Value
		})

		// Write label row
		if _, err := labelsWriter.Write([]map[string]any{labelRow}); err != nil {
			return fmt.Errorf("write label row: %w", err)
		}

		// Get chunks for this series using the OriginalIdx
		var chkMetas []chunks.Meta
		if seriesWithLabels.OriginalIdx < len(allChunks) {
			chkMetas = allChunks[seriesWithLabels.OriginalIdx]
		}

		// Collect and encode chunks
		chunkBuckets, err := collectChunks(chkMetas, chunkReader)
		if err != nil {
			return fmt.Errorf("collect chunks for series: %w", err)
		}

		// Build chunk row
		chunkRow := make(map[string]any)
		chunkRow[LabelHashColumn] = int64(hash)
		chunkRow[ChunksColumn0] = chunkBuckets[0]
		chunkRow[ChunksColumn1] = chunkBuckets[1]
		chunkRow[ChunksColumn2] = chunkBuckets[2]

		// Write chunk row
		if _, err := chunksWriter.Write([]map[string]any{chunkRow}); err != nil {
			return fmt.Errorf("write chunk row: %w", err)
		}
	}

	// Close writers to flush data
	if err := labelsWriter.Close(); err != nil {
		return fmt.Errorf("close labels writer: %w", err)
	}
	if err := chunksWriter.Close(); err != nil {
		return fmt.Errorf("close chunks writer: %w", err)
	}

	// Upload files to bucket
	// Path format: parquet/<date>/<shard>.labels.parquet or parquet/<date-range>/<shard>.labels.parquet
	// Single day (8h blocks): 2026-03-28/
	// Multi-day (48h/14d blocks): 2026-03-28_to_2026-03-30/
	blockDateStart := time.UnixMilli(meta.MinTime).UTC().Format("2006-01-02")
	blockDateEnd := time.UnixMilli(meta.MaxTime).UTC().Format("2006-01-02")
	var dateFolder string
	if blockDateStart == blockDateEnd {
		dateFolder = blockDateStart // Single day: just "2026-03-28"
	} else {
		dateFolder = fmt.Sprintf("%s_to_%s", blockDateStart, blockDateEnd) // Multi-day: "2026-03-28_to_2026-03-30"
	}
	labelsPath := filepath.Join(basePath, dateFolder, fmt.Sprintf("%d.labels.parquet", shard.Index))
	chunksPath := filepath.Join(basePath, dateFolder, fmt.Sprintf("%d.chunks.parquet", shard.Index))

	level.Debug(logger).Log("msg", "uploading Parquet files", "labels_path", labelsPath, "chunks_path", chunksPath)

	// Upload labels file
	if _, err := labelsFile.Seek(0, 0); err != nil {
		return fmt.Errorf("seek labels file: %w", err)
	}
	labelsStat, _ := labelsFile.Stat()
	labelsReader := bufio.NewReader(labelsFile)
	if err := parquetBucket.Upload(ctx, labelsPath, labelsReader); err != nil {
		return fmt.Errorf("upload labels file: %w", err)
	}
	metrics.parquetBytesWritten.Add(float64(labelsStat.Size()))
	metrics.parquetFilesUploaded.Inc()

	// Upload chunks file
	if _, err := chunksFile.Seek(0, 0); err != nil {
		return fmt.Errorf("seek chunks file: %w", err)
	}
	chunksStat, _ := chunksFile.Stat()
	chunksReader := bufio.NewReader(chunksFile)
	if err := parquetBucket.Upload(ctx, chunksPath, chunksReader); err != nil {
		return fmt.Errorf("upload chunks file: %w", err)
	}
	metrics.parquetBytesWritten.Add(float64(chunksStat.Size()))
	metrics.parquetFilesUploaded.Inc()

	level.Debug(logger).Log(
		"msg", "uploaded Parquet files",
		"shard", shard.Index,
		"labels_size_bytes", labelsStat.Size(),
		"chunks_size_bytes", chunksStat.Size(),
	)

	return nil
}
