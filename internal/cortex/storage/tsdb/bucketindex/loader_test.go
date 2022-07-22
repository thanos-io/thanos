// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package bucketindex

import (
	"bytes"
	"context"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortex_testutil "github.com/thanos-io/thanos/internal/cortex/storage/tsdb/testutil"
	"github.com/thanos-io/thanos/internal/cortex/util/services"
	"github.com/thanos-io/thanos/internal/cortex/util/test"
)

func TestLoader_GetIndex_ShouldLazyLoadBucketIndex(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Create a bucket index.
	idx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{
			{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20},
		},
		BlockDeletionMarks: nil,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", nil, idx))

	// Create the loader.
	loader := NewLoader(prepareLoaderConfig(), bkt, nil, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	// Ensure no index has been loaded yet.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_load_failures_total Total number of bucket index loading failures.
		# TYPE cortex_bucket_index_load_failures_total counter
		cortex_bucket_index_load_failures_total 0
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 0
		# HELP cortex_bucket_index_loads_total Total number of bucket index loading attempts.
		# TYPE cortex_bucket_index_loads_total counter
		cortex_bucket_index_loads_total 0
	`),
		"cortex_bucket_index_loads_total",
		"cortex_bucket_index_load_failures_total",
		"cortex_bucket_index_loaded",
	))

	// Request the index multiple times.
	for i := 0; i < 10; i++ {
		actualIdx, err := loader.GetIndex(ctx, "user-1")
		require.NoError(t, err)
		assert.Equal(t, idx, actualIdx)
	}

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_load_failures_total Total number of bucket index loading failures.
		# TYPE cortex_bucket_index_load_failures_total counter
		cortex_bucket_index_load_failures_total 0
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 1
		# HELP cortex_bucket_index_loads_total Total number of bucket index loading attempts.
		# TYPE cortex_bucket_index_loads_total counter
		cortex_bucket_index_loads_total 1
	`),
		"cortex_bucket_index_loads_total",
		"cortex_bucket_index_load_failures_total",
		"cortex_bucket_index_loaded",
	))
}

func TestLoader_GetIndex_ShouldCacheError(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Create the loader.
	loader := NewLoader(prepareLoaderConfig(), bkt, nil, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	// Write a corrupted index.
	require.NoError(t, bkt.Upload(ctx, path.Join("user-1", IndexCompressedFilename), strings.NewReader("invalid!}")))

	// Request the index multiple times.
	for i := 0; i < 10; i++ {
		_, err := loader.GetIndex(ctx, "user-1")
		require.Equal(t, ErrIndexCorrupted, err)
	}

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_load_failures_total Total number of bucket index loading failures.
		# TYPE cortex_bucket_index_load_failures_total counter
		cortex_bucket_index_load_failures_total 1
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 0
		# HELP cortex_bucket_index_loads_total Total number of bucket index loading attempts.
		# TYPE cortex_bucket_index_loads_total counter
		cortex_bucket_index_loads_total 1
	`),
		"cortex_bucket_index_loads_total",
		"cortex_bucket_index_load_failures_total",
		"cortex_bucket_index_loaded",
	))
}

func TestLoader_GetIndex_ShouldCacheIndexNotFoundError(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Create the loader.
	loader := NewLoader(prepareLoaderConfig(), bkt, nil, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	// Request the index multiple times.
	for i := 0; i < 10; i++ {
		_, err := loader.GetIndex(ctx, "user-1")
		require.Equal(t, ErrIndexNotFound, err)
	}

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_load_failures_total Total number of bucket index loading failures.
		# TYPE cortex_bucket_index_load_failures_total counter
		cortex_bucket_index_load_failures_total 0
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 0
		# HELP cortex_bucket_index_loads_total Total number of bucket index loading attempts.
		# TYPE cortex_bucket_index_loads_total counter
		cortex_bucket_index_loads_total 1
	`),
		"cortex_bucket_index_loads_total",
		"cortex_bucket_index_load_failures_total",
		"cortex_bucket_index_loaded",
	))
}

func TestLoader_ShouldUpdateIndexInBackgroundOnPreviousLoadSuccess(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Create a bucket index.
	idx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{
			{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20},
		},
		BlockDeletionMarks: nil,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", nil, idx))

	// Create the loader.
	cfg := LoaderConfig{
		CheckInterval:         time.Second,
		UpdateOnStaleInterval: time.Second,
		UpdateOnErrorInterval: time.Hour, // Intentionally high to not hit it.
		IdleTimeout:           time.Hour, // Intentionally high to not hit it.
	}

	loader := NewLoader(cfg, bkt, nil, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	actualIdx, err := loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Update the bucket index.
	idx.Blocks = append(idx.Blocks, &Block{ID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 30})
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", nil, idx))

	// Wait until the index has been updated in background.
	test.Poll(t, 3*time.Second, 2, func() interface{} {
		actualIdx, err := loader.GetIndex(ctx, "user-1")
		if err != nil {
			return 0
		}
		return len(actualIdx.Blocks)
	})

	actualIdx, err = loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_load_failures_total Total number of bucket index loading failures.
		# TYPE cortex_bucket_index_load_failures_total counter
		cortex_bucket_index_load_failures_total 0
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 1
	`),
		"cortex_bucket_index_load_failures_total",
		"cortex_bucket_index_loaded",
	))
}

func TestLoader_ShouldUpdateIndexInBackgroundOnPreviousLoadFailure(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Write a corrupted index.
	require.NoError(t, bkt.Upload(ctx, path.Join("user-1", IndexCompressedFilename), strings.NewReader("invalid!}")))

	// Create the loader.
	cfg := LoaderConfig{
		CheckInterval:         time.Second,
		UpdateOnStaleInterval: time.Hour, // Intentionally high to not hit it.
		UpdateOnErrorInterval: time.Second,
		IdleTimeout:           time.Hour, // Intentionally high to not hit it.
	}

	loader := NewLoader(cfg, bkt, nil, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	_, err := loader.GetIndex(ctx, "user-1")
	assert.Equal(t, ErrIndexCorrupted, err)

	// Upload the bucket index.
	idx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{
			{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20},
		},
		BlockDeletionMarks: nil,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", nil, idx))

	// Wait until the index has been updated in background.
	test.Poll(t, 3*time.Second, nil, func() interface{} {
		_, err := loader.GetIndex(ctx, "user-1")
		return err
	})

	actualIdx, err := loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 1
	`),
		"cortex_bucket_index_loaded",
	))
}

func TestLoader_ShouldUpdateIndexInBackgroundOnPreviousIndexNotFound(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Create the loader.
	cfg := LoaderConfig{
		CheckInterval:         time.Second,
		UpdateOnStaleInterval: time.Second,
		UpdateOnErrorInterval: time.Hour, // Intentionally high to not hit it.
		IdleTimeout:           time.Hour, // Intentionally high to not hit it.
	}

	loader := NewLoader(cfg, bkt, nil, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	_, err := loader.GetIndex(ctx, "user-1")
	assert.Equal(t, ErrIndexNotFound, err)

	// Upload the bucket index.
	idx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{
			{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20},
		},
		BlockDeletionMarks: nil,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", nil, idx))

	// Wait until the index has been updated in background.
	test.Poll(t, 3*time.Second, nil, func() interface{} {
		_, err := loader.GetIndex(ctx, "user-1")
		return err
	})

	actualIdx, err := loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 1
	`),
		"cortex_bucket_index_loaded",
	))
}

func TestLoader_ShouldNotCacheCriticalErrorOnBackgroundUpdates(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Create a bucket index.
	idx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{
			{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20},
		},
		BlockDeletionMarks: nil,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", nil, idx))

	// Create the loader.
	cfg := LoaderConfig{
		CheckInterval:         time.Second,
		UpdateOnStaleInterval: time.Second,
		UpdateOnErrorInterval: time.Second,
		IdleTimeout:           time.Hour, // Intentionally high to not hit it.
	}

	loader := NewLoader(cfg, bkt, nil, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	actualIdx, err := loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Write a corrupted index.
	require.NoError(t, bkt.Upload(ctx, path.Join("user-1", IndexCompressedFilename), strings.NewReader("invalid!}")))

	// Wait until the first failure has been tracked.
	test.Poll(t, 3*time.Second, true, func() interface{} {
		return testutil.ToFloat64(loader.loadFailures) > 0
	})

	actualIdx, err = loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 1
	`),
		"cortex_bucket_index_loaded",
	))
}

func TestLoader_ShouldCacheIndexNotFoundOnBackgroundUpdates(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Create a bucket index.
	idx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{
			{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20},
		},
		BlockDeletionMarks: nil,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", nil, idx))

	// Create the loader.
	cfg := LoaderConfig{
		CheckInterval:         time.Second,
		UpdateOnStaleInterval: time.Second,
		UpdateOnErrorInterval: time.Second,
		IdleTimeout:           time.Hour, // Intentionally high to not hit it.
	}

	loader := NewLoader(cfg, bkt, nil, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	actualIdx, err := loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Delete the bucket index.
	require.NoError(t, DeleteIndex(ctx, bkt, "user-1", nil))

	// Wait until the next index load attempt occurs.
	prevLoads := testutil.ToFloat64(loader.loadAttempts)
	test.Poll(t, 3*time.Second, true, func() interface{} {
		return testutil.ToFloat64(loader.loadAttempts) > prevLoads
	})

	// We expect the bucket index is not considered loaded because of the error.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
			# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
			# TYPE cortex_bucket_index_loaded gauge
			cortex_bucket_index_loaded 0
		`),
		"cortex_bucket_index_loaded",
	))

	// Try to get the index again. We expect no load attempt because the error has been cached.
	prevLoads = testutil.ToFloat64(loader.loadAttempts)
	actualIdx, err = loader.GetIndex(ctx, "user-1")
	assert.Equal(t, ErrIndexNotFound, err)
	assert.Nil(t, actualIdx)
	assert.Equal(t, prevLoads, testutil.ToFloat64(loader.loadAttempts))
}

func TestLoader_ShouldOffloadIndexIfNotFoundDuringBackgroundUpdates(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Create a bucket index.
	idx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{
			{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20},
		},
		BlockDeletionMarks: nil,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", nil, idx))

	// Create the loader.
	cfg := LoaderConfig{
		CheckInterval:         time.Second,
		UpdateOnStaleInterval: time.Second,
		UpdateOnErrorInterval: time.Second,
		IdleTimeout:           time.Hour, // Intentionally high to not hit it.
	}

	loader := NewLoader(cfg, bkt, nil, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	actualIdx, err := loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Delete the index
	require.NoError(t, DeleteIndex(ctx, bkt, "user-1", nil))

	// Wait until the index is offloaded.
	test.Poll(t, 3*time.Second, float64(0), func() interface{} {
		return testutil.ToFloat64(loader.loaded)
	})

	_, err = loader.GetIndex(ctx, "user-1")
	require.Equal(t, ErrIndexNotFound, err)

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 0
	`),
		"cortex_bucket_index_loaded",
	))
}

func TestLoader_ShouldOffloadIndexIfIdleTimeoutIsReachedDuringBackgroundUpdates(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Create a bucket index.
	idx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{
			{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20},
		},
		BlockDeletionMarks: nil,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", nil, idx))

	// Create the loader.
	cfg := LoaderConfig{
		CheckInterval:         time.Second,
		UpdateOnStaleInterval: time.Second,
		UpdateOnErrorInterval: time.Second,
		IdleTimeout:           0, // Offload at first check.
	}

	loader := NewLoader(cfg, bkt, nil, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	actualIdx, err := loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Wait until the index is offloaded.
	test.Poll(t, 3*time.Second, float64(0), func() interface{} {
		return testutil.ToFloat64(loader.loaded)
	})

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 0
		# HELP cortex_bucket_index_loads_total Total number of bucket index loading attempts.
		# TYPE cortex_bucket_index_loads_total counter
		cortex_bucket_index_loads_total 1
	`),
		"cortex_bucket_index_loaded",
		"cortex_bucket_index_loads_total",
	))

	// Load it again.
	actualIdx, err = loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_loads_total Total number of bucket index loading attempts.
		# TYPE cortex_bucket_index_loads_total counter
		cortex_bucket_index_loads_total 2
	`),
		"cortex_bucket_index_loads_total",
	))
}

func prepareLoaderConfig() LoaderConfig {
	return LoaderConfig{
		CheckInterval:         time.Minute,
		UpdateOnStaleInterval: 15 * time.Minute,
		UpdateOnErrorInterval: time.Minute,
		IdleTimeout:           time.Hour,
	}
}
