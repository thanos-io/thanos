// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
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
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc, nil)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	meta, err := metadata.ReadFromDir(filepath.Join(tmpDir, blockID.String()))
	testutil.Ok(t, err)

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			pool := NewReaderPool(log.NewNopLogger(), testData.lazyReaderEnabled, testData.lazyReaderIdleTimeout, NewReaderPoolMetrics(nil), AlwaysEagerDownloadIndexHeader)
			defer pool.Close()

			r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, meta)
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
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc, nil)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))
	meta, err := metadata.ReadFromDir(filepath.Join(tmpDir, blockID.String()))
	testutil.Ok(t, err)

	metrics := NewReaderPoolMetrics(nil)
	pool := NewReaderPool(log.NewNopLogger(), true, idleTimeout, metrics, AlwaysEagerDownloadIndexHeader)
	defer pool.Close()

	r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, meta)
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

func TestReaderPool_MultipleReaders(t *testing.T) {
	ctx := context.Background()

	blkDir := t.TempDir()

	bkt := objstore.NewInMemBucket()
	b1, err := e2eutil.CreateBlock(ctx, blkDir, []labels.Labels{
		labels.New(labels.Label{Name: "a", Value: "1"}),
		labels.New(labels.Label{Name: "a", Value: "2"}),
		labels.New(labels.Label{Name: "a", Value: "3"}),
		labels.New(labels.Label{Name: "a", Value: "4"}),
		labels.New(labels.Label{Name: "b", Value: "1"}),
	}, 100, 0, 1000, labels.New(labels.Label{Name: "ext1", Value: "val1"}), 124, metadata.NoneFunc, nil)
	testutil.Ok(t, err)

	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(blkDir, b1.String()), metadata.NoneFunc))

	readerPool := NewReaderPool(
		log.NewNopLogger(),
		true,
		time.Minute,
		NewReaderPoolMetrics(prometheus.NewRegistry()),
		AlwaysEagerDownloadIndexHeader,
	)

	dlDir := t.TempDir()

	m, err := metadata.ReadFromDir(filepath.Join(blkDir, b1.String()))
	testutil.Ok(t, err)

	startWg := &sync.WaitGroup{}
	startWg.Add(1)

	waitWg := &sync.WaitGroup{}

	const readersCount = 10
	waitWg.Add(readersCount)
	for range readersCount {
		go func() {
			defer waitWg.Done()
			t.Logf("waiting")
			startWg.Wait()
			t.Logf("starting")

			br, err := readerPool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, dlDir, b1, 32, m)
			testutil.Ok(t, err)

			t.Cleanup(func() {
				testutil.Ok(t, br.Close())
			})
		}()
	}

	startWg.Done()
	waitWg.Wait()
}
