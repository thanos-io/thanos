// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package parquet

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	// LabelColumnPrefix is the prefix for label columns in Parquet files.
	LabelColumnPrefix = "___cf_meta_label_"
	// LabelIndexColumn contains an encoded list of indexes for populated columns.
	LabelIndexColumn = "___cf_meta_index"
	// LabelHashColumn contains the hash of the label set for joining.
	LabelHashColumn = "___cf_meta_hash"
	// ChunksColumn0 contains chunks from 00:00-08:00.
	ChunksColumn0 = "___cf_meta_chunk_0"
	// ChunksColumn1 contains chunks from 08:00-16:00.
	ChunksColumn1 = "___cf_meta_chunk_1"
	// ChunksColumn2 contains chunks from 16:00-00:00.
	ChunksColumn2 = "___cf_meta_chunk_2"
)

const (
	// ChunkColumnLength is the duration covered by each chunk column (8 hours).
	ChunkColumnLength = 8 * time.Hour
	// ChunkColumnsPerDay is the number of chunk columns per day (3).
	ChunkColumnsPerDay = 3
)

// ChunkColumnName returns the column name for the given chunk column index.
func ChunkColumnName(i int) (string, bool) {
	switch i {
	case 0:
		return ChunksColumn0, true
	case 1:
		return ChunksColumn1, true
	case 2:
		return ChunksColumn2, true
	}
	return "", false
}

// LabelNameToColumn converts a label name to a Parquet column name.
func LabelNameToColumn(lbl string) string {
	return fmt.Sprintf("%s%s", LabelColumnPrefix, lbl)
}

// ColumnToLabelName converts a Parquet column name to a label name.
func ColumnToLabelName(col string) string {
	return strings.TrimPrefix(col, LabelColumnPrefix)
}

// ChunkColumnIndex returns the chunk column index for a given timestamp and block mint.
func ChunkColumnIndex(mint int64, t time.Time) (int, bool) {
	mints := time.UnixMilli(mint)

	colIdx := 0
	for cur := mints.Add(ChunkColumnLength); !t.Before(cur); cur = cur.Add(ChunkColumnLength) {
		colIdx++
	}
	return min(colIdx, ChunkColumnsPerDay-1), true
}

// BuildSchemaFromLabels creates a Parquet schema with columns for the given label names.
// The schema includes:
// - Label index column (delta-length byte array)
// - Label hash column (int64)
// - One dictionary-encoded column per label name
// - Three chunk columns for 8-hour time buckets.
func BuildSchemaFromLabels(lbls []string) *parquet.Schema {
	g := make(parquet.Group)

	// Index column for column projection optimization
	g[LabelIndexColumn] = parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.DeltaLengthByteArray)
	// Hash column for joining labels and chunks
	g[LabelHashColumn] = parquet.Encoded(parquet.Leaf(parquet.Int64Type), &parquet.Plain)

	// One column per label name, dictionary-encoded for efficiency
	for _, lbl := range lbls {
		g[LabelNameToColumn(lbl)] = parquet.Optional(parquet.Encoded(parquet.String(), &parquet.RLEDictionary))
	}

	// Three chunk columns (8 hours each)
	chunkNode := parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.DeltaLengthByteArray)
	g[ChunksColumn0] = chunkNode
	g[ChunksColumn1] = chunkNode
	g[ChunksColumn2] = chunkNode

	return parquet.NewSchema("tsdb", g)
}

// defaultZstdCodec is the default ZSTD compression codec.
var defaultZstdCodec = &zstd.Codec{Level: zstd.SpeedBetterCompression, Concurrency: 1}

// WithCompression applies ZSTD compression to all columns in the schema.
func WithCompression(s *parquet.Schema) *parquet.Schema {
	g := make(parquet.Group)

	for _, c := range s.Columns() {
		lc, _ := s.Lookup(c...)
		g[lc.Path[0]] = parquet.Compressed(lc.Node, defaultZstdCodec)
	}

	return parquet.NewSchema("compressed", g)
}

// ChunkColumns is the list of chunk-related columns.
var ChunkColumns = []string{LabelHashColumn, ChunksColumn0, ChunksColumn1, ChunksColumn2}

// ChunkProjection returns a schema with only chunk columns.
func ChunkProjection(s *parquet.Schema) *parquet.Schema {
	g := make(parquet.Group)

	for _, c := range ChunkColumns {
		lc, ok := s.Lookup(c)
		if !ok {
			continue
		}
		g[c] = lc.Node
	}
	return parquet.NewSchema("chunk-projection", g)
}

// LabelsProjection returns a schema with only label columns (excludes chunk columns).
func LabelsProjection(s *parquet.Schema) *parquet.Schema {
	g := make(parquet.Group)

	for _, c := range s.Columns() {
		if slices.Contains(ChunkColumns, c[0]) {
			continue
		}
		lc, ok := s.Lookup(c...)
		if !ok {
			continue
		}
		g[c[0]] = lc.Node
	}
	return parquet.NewSchema("labels-projection", g)
}

// BuildSortingColumns creates sorting columns for Parquet files.
func BuildSortingColumns(sortLabels []string) []parquet.SortingColumn {
	sortingColumns := make([]parquet.SortingColumn, 0, len(sortLabels))
	for _, lbl := range sortLabels {
		sortingColumns = append(sortingColumns, parquet.Ascending(LabelNameToColumn(lbl)))
	}
	return sortingColumns
}

// BuildBloomFilterColumns creates bloom filter columns for Parquet files.
func BuildBloomFilterColumns(filterLabels []string) []parquet.BloomFilterColumn {
	cols := make([]parquet.BloomFilterColumn, 0, len(filterLabels))
	for _, lbl := range filterLabels {
		cols = append(cols, parquet.SplitBlockFilter(10, LabelNameToColumn(lbl)))
	}
	return cols
}

// ExtractUniqueLabelNames extracts all unique label names from a set of series.
func ExtractUniqueLabelNames(seriesLabels []labels.Labels) []string {
	labelSet := make(map[string]struct{})
	for _, lbls := range seriesLabels {
		lbls.Range(func(l labels.Label) {
			labelSet[l.Name] = struct{}{}
		})
	}

	uniqueLabels := make([]string, 0, len(labelSet))
	for name := range labelSet {
		uniqueLabels = append(uniqueLabels, name)
	}
	slices.Sort(uniqueLabels)
	return uniqueLabels
}
