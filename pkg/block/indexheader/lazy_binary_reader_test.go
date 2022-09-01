// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestNewLazyBinaryReader_ShouldFailIfUnableToBuildIndexHeader(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	_, err = NewLazyBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, ulid.MustNew(0, nil), 3, NewLazyBinaryReaderMetrics(nil), nil)
	testutil.NotOk(t, err)
}

func TestNewLazyBinaryReader_ShouldBuildIndexHeaderFromBucket(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	// Create block.
	blockID, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124, metadata.NoneFunc)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	m := NewLazyBinaryReaderMetrics(nil)
	r, err := NewLazyBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, m, nil)
	testutil.Ok(t, err)
	testutil.Assert(t, r.reader == nil)
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadCount))

	// Should lazy load the index upon first usage.
	v, err := r.IndexVersion()
	testutil.Ok(t, err)
	testutil.Equals(t, 2, v)
	testutil.Assert(t, r.reader != nil)
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadCount))

	labelNames, err := r.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"a"}, labelNames)
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadCount))
}

func TestNewLazyBinaryReader_ShouldRebuildCorruptedIndexHeader(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	// Create block.
	blockID, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124, metadata.NoneFunc)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	// Write a corrupted index-header for the block.
	headerFilename := filepath.Join(tmpDir, blockID.String(), block.IndexHeaderFilename)
	testutil.Ok(t, os.WriteFile(headerFilename, []byte("xxx"), os.ModePerm))

	m := NewLazyBinaryReaderMetrics(nil)
	r, err := NewLazyBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, m, nil)
	testutil.Ok(t, err)
	testutil.Assert(t, r.reader == nil)
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadFailedCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadCount))

	// Ensure it can read data.
	labelNames, err := r.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"a"}, labelNames)
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadFailedCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadCount))
}

func TestLazyBinaryReader_ShouldReopenOnUsageAfterClose(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	// Create block.
	blockID, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124, metadata.NoneFunc)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	m := NewLazyBinaryReaderMetrics(nil)
	r, err := NewLazyBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, m, nil)
	testutil.Ok(t, err)
	testutil.Assert(t, r.reader == nil)

	// Should lazy load the index upon first usage.
	labelNames, err := r.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"a"}, labelNames)
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadFailedCount))

	// Close it.
	testutil.Ok(t, r.Close())
	testutil.Assert(t, r.reader == nil)
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(m.unloadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadFailedCount))

	// Should lazy load again upon next usage.
	labelNames, err = r.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"a"}, labelNames)
	testutil.Equals(t, float64(2), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadFailedCount))

	// Closing an already closed lazy reader should be a no-op.
	for i := 0; i < 2; i++ {
		testutil.Ok(t, r.Close())
		testutil.Equals(t, float64(2), promtestutil.ToFloat64(m.unloadCount))
		testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadFailedCount))
	}
}

func TestLazyBinaryReader_unload_ShouldReturnErrorIfNotIdle(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	// Create block.
	blockID, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124, metadata.NoneFunc)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	m := NewLazyBinaryReaderMetrics(nil)
	r, err := NewLazyBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, m, nil)
	testutil.Ok(t, err)
	testutil.Assert(t, r.reader == nil)

	// Should lazy load the index upon first usage.
	labelNames, err := r.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"a"}, labelNames)
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadFailedCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadFailedCount))

	// Try to unload but not idle since enough time.
	testutil.Equals(t, errNotIdle, r.unloadIfIdleSince(time.Now().Add(-time.Minute).UnixNano()))
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadFailedCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadFailedCount))

	// Try to unload and idle since enough time.
	testutil.Ok(t, r.unloadIfIdleSince(time.Now().UnixNano()))
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadFailedCount))
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(m.unloadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadFailedCount))
}

func TestLazyBinaryReader_LoadUnloadRaceCondition(t *testing.T) {
	// Run the test for a fixed amount of time.
	const runDuration = 5 * time.Second

	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	// Create block.
	blockID, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124, metadata.NoneFunc)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	m := NewLazyBinaryReaderMetrics(nil)
	r, err := NewLazyBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, m, nil)
	testutil.Ok(t, err)
	testutil.Assert(t, r.reader == nil)
	t.Cleanup(func() {
		testutil.Ok(t, r.Close())
	})

	done := make(chan struct{})
	time.AfterFunc(runDuration, func() { close(done) })
	wg := sync.WaitGroup{}
	wg.Add(2)

	// Start a goroutine which continuously try to unload the reader.
	go func() {
		defer wg.Done()

		for {
			select {
			case <-done:
				return
			default:
				testutil.Ok(t, r.unloadIfIdleSince(0))
			}
		}
	}()

	// Try to read multiple times, while the other goroutine continuously try to unload it.
	go func() {
		defer wg.Done()

		for {
			select {
			case <-done:
				return
			default:
				_, err := r.PostingsOffset("a", "1")
				testutil.Assert(t, err == nil || err == errUnloadedWhileLoading)
			}
		}
	}()

	// Wait until both goroutines have done.
	wg.Wait()
}
