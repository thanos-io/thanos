// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestReaderPool_NewBinaryReader(t *testing.T) {
	tests := map[string]struct {
		lazyReaderEnabled     bool
		lazyReaderIdleTimeout time.Duration
	}{
		"lazy reader is disabled": {
			lazyReaderEnabled: false,
		},
		"lazy reader is enabled but close on idle timeout is disabled": {
			lazyReaderEnabled:     true,
			lazyReaderIdleTimeout: 0,
		},
		"lazy reader and close on idle timeout are both enabled": {
			lazyReaderEnabled:     true,
			lazyReaderIdleTimeout: time.Minute,
		},
	}

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

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			pool := NewReaderPool(log.NewNopLogger(), testData.lazyReaderEnabled, testData.lazyReaderIdleTimeout, NewReaderPoolMetrics(nil))
			defer pool.Close()

			r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3)
			testutil.Ok(t, err)
			defer func() { testutil.Ok(t, r.Close()) }()

			// Ensure it can read data.
			labelNames, err := r.LabelNames()
			testutil.Ok(t, err)
			testutil.Equals(t, []string{"a"}, labelNames)
		})
	}
}

func TestReaderPool_ShouldCloseIdleLazyReaders(t *testing.T) {
	const idleTimeout = time.Second

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

	metrics := NewReaderPoolMetrics(nil)
	pool := NewReaderPool(log.NewNopLogger(), true, idleTimeout, metrics)
	defer pool.Close()

	r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, r.Close()) }()

	// Ensure it can read data.
	labelNames, err := r.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"a"}, labelNames)
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(metrics.lazyReader.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(metrics.lazyReader.unloadCount))

	// Wait enough time before checking it.
	time.Sleep(idleTimeout * 2)

	// We expect the reader has been closed, but not released from the pool.
	testutil.Assert(t, pool.isTracking(r.(*LazyBinaryReader)))
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(metrics.lazyReader.loadCount))
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(metrics.lazyReader.unloadCount))

	// Ensure it can still read data (will be re-opened).
	labelNames, err = r.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"a"}, labelNames)
	testutil.Assert(t, pool.isTracking(r.(*LazyBinaryReader)))
	testutil.Equals(t, float64(2), promtestutil.ToFloat64(metrics.lazyReader.loadCount))
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(metrics.lazyReader.unloadCount))

	// We expect an explicit call to Close() to close the reader and release it from the pool too.
	testutil.Ok(t, r.Close())
	testutil.Assert(t, !pool.isTracking(r.(*LazyBinaryReader)))
	testutil.Equals(t, float64(2), promtestutil.ToFloat64(metrics.lazyReader.loadCount))
	testutil.Equals(t, float64(2), promtestutil.ToFloat64(metrics.lazyReader.unloadCount))
}
