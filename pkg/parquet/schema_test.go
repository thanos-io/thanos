// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package parquet

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"
)

func TestLabelNameToColumn(t *testing.T) {
	tests := []struct {
		labelName      string
		expectedColumn string
	}{
		{"__name__", "___cf_meta_label___name__"},
		{"job", "___cf_meta_label_job"},
		{"instance", "___cf_meta_label_instance"},
		{"region", "___cf_meta_label_region"},
	}

	for _, tt := range tests {
		t.Run(tt.labelName, func(t *testing.T) {
			result := LabelNameToColumn(tt.labelName)
			testutil.Equals(t, tt.expectedColumn, result)
		})
	}
}

func TestColumnToLabelName(t *testing.T) {
	tests := []struct {
		column            string
		expectedLabelName string
	}{
		{"___cf_meta_label___name__", "__name__"},
		{"___cf_meta_label_job", "job"},
		{"___cf_meta_label_instance", "instance"},
		{"___cf_meta_label_region", "region"},
	}

	for _, tt := range tests {
		t.Run(tt.column, func(t *testing.T) {
			result := ColumnToLabelName(tt.column)
			testutil.Equals(t, tt.expectedLabelName, result)
		})
	}
}

func TestChunkColumnName(t *testing.T) {
	tests := []struct {
		index          int
		expectedColumn string
		expectedOk     bool
	}{
		{0, ChunksColumn0, true},
		{1, ChunksColumn1, true},
		{2, ChunksColumn2, true},
		{3, "", false},
		{-1, "", false},
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.index)), func(t *testing.T) {
			column, ok := ChunkColumnName(tt.index)
			testutil.Equals(t, tt.expectedOk, ok)
			testutil.Equals(t, tt.expectedColumn, column)
		})
	}
}

func TestBuildSchemaFromLabels(t *testing.T) {
	labelNames := []string{"__name__", "job", "instance"}
	schema := BuildSchemaFromLabels(labelNames)

	testutil.Assert(t, schema != nil, "schema should not be nil")

	// Check that all expected columns exist
	expectedColumns := []string{
		LabelIndexColumn,
		LabelHashColumn,
		"___cf_meta_label___name__",
		"___cf_meta_label_job",
		"___cf_meta_label_instance",
		ChunksColumn0,
		ChunksColumn1,
		ChunksColumn2,
	}

	columns := schema.Columns()
	testutil.Equals(t, len(expectedColumns), len(columns), "should have correct number of columns")

	for _, expected := range expectedColumns {
		found := false
		for _, col := range columns {
			if len(col) > 0 && col[0] == expected {
				found = true
				break
			}
		}
		testutil.Assert(t, found, "column %s should exist in schema", expected)
	}
}

func TestExtractUniqueLabelNames(t *testing.T) {
	tests := []struct {
		name          string
		seriesLabels  []labels.Labels
		expectedNames []string
	}{
		{
			name: "single series",
			seriesLabels: []labels.Labels{
				labels.FromStrings("__name__", "metric1", "job", "api"),
			},
			expectedNames: []string{"__name__", "job"},
		},
		{
			name: "multiple series with overlapping labels",
			seriesLabels: []labels.Labels{
				labels.FromStrings("__name__", "metric1", "job", "api"),
				labels.FromStrings("__name__", "metric2", "job", "worker", "region", "us-east"),
			},
			expectedNames: []string{"__name__", "job", "region"},
		},
		{
			name:          "empty series",
			seriesLabels:  []labels.Labels{},
			expectedNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractUniqueLabelNames(tt.seriesLabels)
			testutil.Equals(t, len(tt.expectedNames), len(result))

			// Check all expected names exist (order may vary)
			for _, expected := range tt.expectedNames {
				found := false
				for _, actual := range result {
					if actual == expected {
						found = true
						break
					}
				}
				testutil.Assert(t, found, "expected label %s not found", expected)
			}
		})
	}
}

func TestChunkColumns(t *testing.T) {
	// Verify ChunkColumns constant contains all chunk columns
	testutil.Equals(t, 4, len(ChunkColumns), "should have 4 chunk-related columns")

	expectedColumns := []string{LabelHashColumn, ChunksColumn0, ChunksColumn1, ChunksColumn2}
	for i, expected := range expectedColumns {
		testutil.Equals(t, expected, ChunkColumns[i])
	}
}

func TestWithCompression(t *testing.T) {
	labelNames := []string{"__name__", "job"}
	schema := BuildSchemaFromLabels(labelNames)
	compressedSchema := WithCompression(schema)

	testutil.Assert(t, compressedSchema != nil, "compressed schema should not be nil")

	// Compressed schema should have same number of columns
	testutil.Equals(t, len(schema.Columns()), len(compressedSchema.Columns()))
}
