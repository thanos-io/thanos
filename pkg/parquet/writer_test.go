// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package parquet

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// mockBucket is a simple in-memory bucket for testing.
type mockBucket struct {
	objstore.Bucket
	files map[string][]byte
}

func newMockBucket() *mockBucket {
	return &mockBucket{
		files: make(map[string][]byte),
	}
}

func (m *mockBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	for name := range m.files {
		if len(dir) == 0 || len(name) >= len(dir) && name[:len(dir)] == dir {
			if err := f(name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *mockBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.files[name] = data
	return nil
}

func (m *mockBucket) Exists(ctx context.Context, name string) (bool, error) {
	_, exists := m.files[name]
	return exists, nil
}

func TestHasParquetFiles(t *testing.T) {
	ctx := context.Background()
	mockBkt := newMockBucket()

	writer := &writer{
		config: Config{
			Enabled:       true,
			ParquetBucket: mockBkt,
			BasePath:      "parquet/",
		},
		logger: log.NewNopLogger(),
	}

	// Helper function to create test metadata
	createMeta := func(minTime, maxTime time.Time) *metadata.Meta {
		m := &metadata.Meta{}
		m.MinTime = minTime.UnixMilli()
		m.MaxTime = maxTime.UnixMilli()
		return m
	}

	tests := []struct {
		name           string
		meta           *metadata.Meta
		existingFiles  map[string][]byte
		expectedExists bool
	}{
		{
			name: "single day block with parquet files",
			meta: createMeta(
				time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 3, 28, 8, 0, 0, 0, time.UTC),
			),
			existingFiles: map[string][]byte{
				"parquet/2026-03-28/0.labels.parquet": []byte("data"),
				"parquet/2026-03-28/0.chunks.parquet": []byte("data"),
			},
			expectedExists: true,
		},
		{
			name: "single day block without parquet files",
			meta: createMeta(
				time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 3, 28, 8, 0, 0, 0, time.UTC),
			),
			existingFiles:  map[string][]byte{},
			expectedExists: false,
		},
		{
			name: "multi-day block with parquet files",
			meta: createMeta(
				time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 3, 30, 0, 0, 0, 0, time.UTC),
			),
			existingFiles: map[string][]byte{
				"parquet/2026-03-28_to_2026-03-30/0.labels.parquet": []byte("data"),
			},
			expectedExists: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockBkt.files = tt.existingFiles

			exists, err := writer.HasParquetFiles(ctx, tt.meta)
			testutil.Ok(t, err)
			testutil.Equals(t, tt.expectedExists, exists)
		})
	}
}

func TestReconcile(t *testing.T) {
	ctx := context.Background()
	mockBkt := newMockBucket()

	writer := &writer{
		config: Config{
			Enabled:              true,
			TSDBBucket:           mockBkt,
			ParquetBucket:        mockBkt,
			BasePath:             "parquet/",
			ShardingEnabled:      false,
			TargetSeriesPerShard: 1000000,
			CompressionCodec:     "zstd",
			RowGroupSize:         1000000,
		},
		logger:  log.NewNopLogger(),
		metrics: NewMetrics(prometheus.NewRegistry()),
	}

	// Helper function to create test metadata
	createReconcileMeta := func(minTime, maxTime time.Time) *metadata.Meta {
		m := &metadata.Meta{}
		m.MinTime = minTime.UnixMilli()
		m.MaxTime = maxTime.UnixMilli()
		return m
	}

	tests := []struct {
		name              string
		blocks            map[ulid.ULID]*metadata.Meta
		existingFiles     map[string][]byte
		expectedConverted int
		expectError       bool
	}{
		{
			name:              "no blocks to reconcile",
			blocks:            map[ulid.ULID]*metadata.Meta{},
			expectedConverted: 0,
			expectError:       false,
		},
		{
			name: "skip 2h blocks",
			blocks: map[ulid.ULID]*metadata.Meta{
				ulid.MustNew(1, nil): createReconcileMeta(
					time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
					time.Date(2026, 3, 28, 2, 0, 0, 0, time.UTC),
				),
			},
			expectedConverted: 0,
			expectError:       false,
		},
		{
			name: "skip 48h blocks",
			blocks: map[ulid.ULID]*metadata.Meta{
				ulid.MustNew(1, nil): createReconcileMeta(
					time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
					time.Date(2026, 3, 30, 0, 0, 0, 0, time.UTC),
				),
			},
			expectedConverted: 0,
			expectError:       false,
		},
		{
			name: "skip 8h blocks that already have parquet files",
			blocks: map[ulid.ULID]*metadata.Meta{
				ulid.MustNew(1, nil): createReconcileMeta(
					time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
					time.Date(2026, 3, 28, 8, 0, 0, 0, time.UTC),
				),
			},
			existingFiles: map[string][]byte{
				"parquet/2026-03-28/0.labels.parquet": []byte("data"),
			},
			expectedConverted: 0,
			expectError:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockBkt.files = tt.existingFiles

			converted, err := writer.Reconcile(ctx, tt.blocks)
			if tt.expectError {
				testutil.NotOk(t, err)
			} else {
				testutil.Ok(t, err)
			}
			testutil.Equals(t, tt.expectedConverted, converted)
		})
	}
}

func TestWriterDisabled(t *testing.T) {
	ctx := context.Background()
	mockBkt := newMockBucket()

	writer := &writer{
		config: Config{
			Enabled:       false,
			ParquetBucket: mockBkt,
			BasePath:      "parquet/",
		},
		logger: log.NewNopLogger(),
	}

	// HasParquetFiles should return false when disabled
	exists, err := writer.HasParquetFiles(ctx, &metadata.Meta{})
	testutil.Ok(t, err)
	testutil.Equals(t, false, exists)

	// Reconcile should return 0 when disabled
	converted, err := writer.Reconcile(ctx, map[ulid.ULID]*metadata.Meta{})
	testutil.Ok(t, err)
	testutil.Equals(t, 0, converted)
}
